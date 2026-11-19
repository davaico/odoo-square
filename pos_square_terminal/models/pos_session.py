from odoo import _, models
from odoo.exceptions import UserError
from odoo.tools import format_amount

from .pos_order import PAID_STATES


class PosSession(models.Model):
    _inherit = 'pos.session'

    def _square_lock_admission(self, *, touch=False):
        if not self:
            return
        self.check_access('read')
        self.env.cr.execute(
            'SELECT id FROM pos_session WHERE id IN %s ORDER BY id FOR NO KEY UPDATE', [tuple(self.ids)],
        )
        if touch:
            # Force a closing transaction with an older snapshot to see the admitted attempt on retry.
            self.env.cr.execute('UPDATE pos_session SET write_date = write_date WHERE id IN %s', [tuple(self.ids)])

    def _square_close_message(self):
        self._square_lock_admission()
        attempts = self.env['pos.square.payment.attempt'].sudo().search([
            ('order_id.session_id', 'in', self.ids), ('order_id.state', 'not in', PAID_STATES),
            ('square_order_link_id.cancelled', '=', False),
        ], order='id desc')
        if not attempts:
            return False

        states = dict(attempts._fields['state']._description_selection(self.env))
        details = {}
        for attempt in attempts:
            if attempt.payment_uuid in details:
                continue
            order = attempt.order_id
            status = states[attempt.state]
            if attempt.provider_status:
                status = _("%(status)s (Square %(provider)s)", status=status, provider=attempt.provider_status)
            details[attempt.payment_uuid] = _(
                "%(order)s · %(amount)s · %(status)s",
                order=order.pos_reference or order.name,
                amount=format_amount(self.env, attempt.requested_amount, attempt.currency_id or order.currency_id),
                status=status,
            )
        return _(
            "Resolve the Square payment or finish the verified sale before closing the register.\n%(sales)s",
            sales='\n'.join(details.values()),
        )

    def _cannot_close_session(self, bank_payment_method_diffs=None):
        message = self._square_close_message()
        if message:
            return {
                'successful': False, 'square_payment_blocked': True, 'redirect': False,
                'title': _("Square payment needs attention"), 'message': message,
            }
        return super()._cannot_close_session(bank_payment_method_diffs)

    def _square_require_closable(self):
        message = self._square_close_message()
        if message:
            raise UserError(message)

    def action_pos_session_closing_control(self, *args, **kwargs):
        self._square_require_closable()
        return super().action_pos_session_closing_control(*args, **kwargs)

    def _validate_session(self, *args, **kwargs):
        self._square_require_closable()
        return super()._validate_session(*args, **kwargs)

    def write(self, vals):
        if vals.get('state') in ('closing_control', 'closed'):
            self.check_access('write')
            self._square_require_closable()
        return super().write(vals)

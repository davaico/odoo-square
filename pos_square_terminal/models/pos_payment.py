from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError

from .pos_order import PAID_STATES, same_value


class PosPayment(models.Model):
    _inherit = 'pos.payment'

    square_attempt_state = fields.Char(compute='_compute_square_attempt_state')

    _square_evidence_fields = {
        'payment_status', 'transaction_id', 'card_type', 'card_brand', 'card_no', 'cardholder_name',
        'payment_ref_no', 'payment_method_authcode', 'payment_method_issuer_bank',
        'payment_method_payment_mode', 'ticket',
    }

    def _compute_square_attempt_state(self):
        attempts = self.pos_order_id._square_attempts()
        states = {attempt.payment_id.id: attempt.state for attempt in attempts}
        for payment in self:
            payment.square_attempt_state = states.get(payment.id, False)

    @api.model_create_multi
    def create(self, vals_list):
        orders = self.env['pos.order'].browse(list({
            vals.get('pos_order_id', self.env.context.get('default_pos_order_id')) for vals in vals_list
        } - {None, False}))
        orders._square_lock_checkout(touch=True)
        if orders._square_attempts():
            raise UserError(_("The original tender is reserved by a Square payment."))
        payments = super().create(vals_list)
        for payment in payments.filtered(lambda line: line.payment_method_id.use_payment_terminal == 'square_terminal'):
            if (
                payment.pos_order_id.state in PAID_STATES
                or payment.payment_status not in (False, 'pending', 'waiting')
                or any(payment[field] for field in payment._square_evidence_fields - {'payment_status'})
            ):
                raise AccessError(_("Square verifies the payment status and provider references."))
        return payments

    def write(self, vals):
        identity_fields = {'uuid', 'pos_order_id', 'payment_method_id', 'amount', 'is_change'}
        changes = identity_fields.intersection(vals)
        evidence = self._square_evidence_fields.intersection(vals)
        if not (changes or evidence):
            return super().write(vals)

        self.check_access('write')
        target_order = self.env['pos.order'].browse(vals.get('pos_order_id') or [])
        orders = self.pos_order_id | target_order
        orders._square_lock_checkout(touch=True)
        reserved = set(orders._square_attempts().order_id.ids)
        for payment in self:
            payment_vals = dict(vals)
            new_method = self.env['pos.payment.method'].browse(vals.get('payment_method_id') or [])
            if reserved.intersection({payment.pos_order_id.id, vals.get('pos_order_id')}):
                if any(not same_value(payment, field, payment_vals[field]) for field in changes):
                    raise UserError(_("The original tender is reserved by a Square payment."))
                for field in evidence:
                    # Native draft synchronization can replay the pre-verification values.
                    if not same_value(payment, field, payment_vals[field]) and (
                        (field == 'payment_status' and payment_vals[field] not in (False, 'pending', 'waiting'))
                        or (field != 'payment_status' and payment_vals[field])
                    ):
                        raise AccessError(_("Square verifies the payment status and provider references."))
                    payment_vals.pop(field)
            elif 'square_terminal' in (payment.payment_method_id.use_payment_terminal, new_method.use_payment_terminal):
                fields_to_check = (
                    self._square_evidence_fields
                    if new_method.use_payment_terminal == 'square_terminal' else evidence
                )
                if (target_order or payment.pos_order_id).state in PAID_STATES or any(
                    payment_vals.get(field, payment[field]) not in (
                        (False, 'pending', 'waiting') if field == 'payment_status' else (False,)
                    )
                    for field in fields_to_check
                ):
                    raise AccessError(_("Square verifies the payment status and provider references."))
            super(PosPayment, payment).write(payment_vals)
        return True

    def _square_set_verified(self, payment_id):
        self.ensure_one()
        super(PosPayment, self.sudo()).write({'payment_status': 'done', 'transaction_id': payment_id})
        self.invalidate_recordset(['square_attempt_state'])

    def unlink(self):
        self.check_access('unlink')
        orders = self.pos_order_id
        orders._square_lock_checkout(touch=True)
        if orders._square_attempts():
            raise UserError(_("The original tender is reserved by a Square payment."))
        return super().unlink()

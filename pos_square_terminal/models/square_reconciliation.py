import logging
from datetime import timedelta
from time import monotonic

from psycopg2.errors import DeadlockDetected, SerializationFailure

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import AccessError


_logger = logging.getLogger(__name__)

RECONCILIATION_BATCH_SIZE = 20
RECONCILIATION_MAX_CANDIDATES = 200
RECONCILIATION_TIME_BUDGET_SECONDS = 60


class SquarePaymentAttempt(models.Model):
    _inherit = 'pos.square.payment.attempt'

    currency_id = fields.Many2one('res.currency', compute='_compute_reconciliation_amounts', compute_sudo=True)
    square_order_id = fields.Char(compute='_compute_square_order_id', store=True, index='btree_not_null')
    verified_base_minor = fields.Integer(copy=False)
    verified_tip_minor = fields.Integer(copy=False)
    has_verified_amounts = fields.Boolean(copy=False)
    requested_amount = fields.Monetary(compute='_compute_reconciliation_amounts', compute_sudo=True)
    verified_base = fields.Monetary(compute='_compute_reconciliation_amounts', compute_sudo=True)
    verified_tip = fields.Monetary(compute='_compute_reconciliation_amounts', compute_sudo=True)
    reconciliation_error = fields.Char(copy=False)
    reconciliation_discrepancy = fields.Char(copy=False)
    order_pricing_discrepancy = fields.Char(compute='_compute_order_pricing_discrepancy', compute_sudo=True)
    next_reconciliation_at = fields.Datetime(default=fields.Datetime.now, index=True, copy=False)

    @api.depends('share_order_id', 'square_order_link_id.square_order_id')
    def _compute_square_order_id(self):
        for attempt in self:
            attempt.square_order_id = attempt.share_order_id or attempt.square_order_link_id.square_order_id

    @api.depends(
        'amount_minor', 'verified_base_minor', 'verified_tip_minor', 'currency_code', 'square_order_link_id.currency_digits',
    )
    def _compute_reconciliation_amounts(self):
        currencies = self.env['res.currency'].with_context(active_test=False).search([
            ('name', 'in', self.mapped('currency_code')),
        ])
        currencies_by_code = {currency.name: currency for currency in currencies}
        for attempt in self:
            divisor = 10 ** attempt.square_order_link_id.currency_digits
            attempt.currency_id = currencies_by_code.get(attempt.currency_code, False)
            attempt.requested_amount = attempt.amount_minor / divisor
            attempt.verified_base = attempt.verified_base_minor / divisor
            attempt.verified_tip = attempt.verified_tip_minor / divisor

    @api.depends('square_order_link_id.pricing_issue')
    def _compute_order_pricing_discrepancy(self):
        descriptions = {
            'preview_discrepancy': _("Square's price preview differed from the Odoo sale."),
            'created_order_discrepancy': _("The linked Square order differed from the Odoo sale."),
        }
        for attempt in self:
            attempt.order_pricing_discrepancy = descriptions.get(attempt.square_order_link_id.pricing_issue, False)

    @api.model
    def _square_reconcilable_states(self):
        return ('dispatching', 'pending', 'declined', 'uncertain', 'timed_out', 'authorized', 'captured', 'verified')

    def _record_successful_check(self):
        self.write({'last_checked_at': fields.Datetime.now(), 'reconciliation_error': False})

    def _record_failed_check(self, issue, *, discrepancy=False):
        values = {'reconciliation_error': issue}
        if discrepancy:
            values['reconciliation_discrepancy'] = issue
        # A failed read cannot release an authorization or change previously verified evidence.
        if self.state not in ('dispatching', 'timed_out', 'failed', 'authorized', 'captured', 'verified'):
            values['state'] = 'uncertain'
        self.write(values)

    def _observe_verified_payment(self, payment):
        self.write({
            'verified_base_minor': payment['amount_money']['amount'],
            'verified_tip_minor': payment.get('tip_money', {}).get('amount', 0),
            'has_verified_amounts': True,
        })

    def _lock_reconciliation(self, *, skip_locked=False):
        self.ensure_one()
        if skip_locked:
            self.env.cr.execute(
                'SELECT id FROM pos_order WHERE id = %s FOR NO KEY UPDATE SKIP LOCKED', [self.order_id.id],
            )
            if not self.env.cr.fetchone():
                return False
        else:
            self.order_id._square_lock_checkout()
        self.env.cr.execute(
            'SELECT id FROM pos_square_payment_attempt WHERE id = %s FOR NO KEY UPDATE', [self.id],
        )
        return bool(self.env.cr.fetchone())

    @api.model
    def _reconciliation_domain(self, now):
        return [
            ('state', 'in', self._square_reconcilable_states()),
            '|', ('next_reconciliation_at', '=', False), ('next_reconciliation_at', '<=', now),
        ]

    @api.model
    def _cron_reconcile_payments(self):
        due = self._reconciliation_domain(fields.Datetime.now())
        deadline = monotonic() + RECONCILIATION_TIME_BUDGET_SECONDS
        seen_ids = []
        made_progress = False
        # Prioritize sales awaiting payment confirmation ahead of routine refund checks.
        for priority in [('state', '!=', 'verified'), ('state', '=', 'verified')]:
            priority_due = [*due, priority]
            while len(seen_ids) < RECONCILIATION_MAX_CANDIDATES and monotonic() < deadline:
                batch_limit = min(RECONCILIATION_BATCH_SIZE, RECONCILIATION_MAX_CANDIDATES - len(seen_ids))
                attempt_ids = self.search(
                    [*priority_due, ('id', 'not in', seen_ids)], order='next_reconciliation_at, id', limit=batch_limit,
                ).ids
                for attempt_id in attempt_ids:
                    if monotonic() >= deadline:
                        break
                    # The selector's transaction can still see old due dates and locked rows.
                    seen_ids.append(attempt_id)
                    if self._reconcile_scheduled_attempt(attempt_id, priority_due):
                        made_progress = True
                if len(attempt_ids) < batch_limit:
                    break
        if made_progress and self.search_count([*due, ('id', 'not in', seen_ids)], limit=1):
            self._schedule_reconciliation_continuation()

    @api.model
    def _schedule_reconciliation_continuation(self):
        cron = self.env.ref('pos_square_terminal.ir_cron_square_terminal_reconcile_payments')
        now = fields.Datetime.now()
        continuation_at = now + timedelta(minutes=1)
        if not self.env['ir.cron.trigger'].sudo().search_count([
            ('cron_id', '=', cron.id), ('call_at', '>', now), ('call_at', '<=', continuation_at),
        ], limit=1):
            cron._trigger(at=continuation_at)

    @api.model
    def _reconcile_scheduled_attempt(self, attempt_id, due):
        try:
            with self.env.registry.cursor() as cr:
                attempt = api.Environment(cr, SUPERUSER_ID, {})[self._name].browse(attempt_id)
                if not attempt._lock_reconciliation(skip_locked=True) or not attempt.filtered_domain(due):
                    return False
                attempt.next_reconciliation_at = fields.Datetime.now() + timedelta(minutes=5)
                try:
                    with cr.savepoint():
                        if attempt._refresh() and attempt.state == 'verified':
                            attempt.next_reconciliation_at = fields.Datetime.now() + timedelta(days=1)
                except (SerializationFailure, DeadlockDetected):
                    raise
                except Exception:
                    _logger.exception('Could not check Square Terminal payment attempt %s', attempt_id)
                return True
        except (SerializationFailure, DeadlockDetected):
            return False

    def action_check_square_payment(self):
        self.ensure_one()
        self.check_access('read')
        if not self.env.user.has_group('point_of_sale.group_pos_manager'):
            raise AccessError(_("Sign in as a Point of Sale manager to check Square payments."))
        attempt = self.sudo()
        attempt._lock_reconciliation()
        attempt._refresh()
        return {'type': 'ir.actions.client', 'tag': 'reload'}


class PosOrder(models.Model):
    _inherit = 'pos.order'

    square_reconciliation_count = fields.Integer(
        compute='_compute_square_reconciliation_count', groups='point_of_sale.group_pos_manager',
    )

    def _compute_square_reconciliation_count(self):
        for order in self:
            order.square_reconciliation_count = self.env['pos.square.payment.attempt'].search_count([
                ('order_id', '=', order.id),
            ])

    def action_open_square_reconciliation(self):
        self.ensure_one()
        self.check_access('read')
        if not self.env.user.has_group('point_of_sale.group_pos_manager'):
            raise AccessError(_("Sign in as a Point of Sale manager to view Square payments."))
        action = self.env['ir.actions.actions']._for_xml_id('pos_square_terminal.action_square_reconciliation')
        action['domain'] = [('order_id', '=', self.id)]
        return action

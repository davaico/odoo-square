# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from datetime import timedelta

from psycopg2.errors import DeadlockDetected, SerializationFailure

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import AccessError, UserError
from odoo.fields import Domain
from odoo.tools.float_utils import float_round


_logger = logging.getLogger(__name__)

RECONCILIATION_BATCH_SIZE = 20
RECONCILIATION_MAX_CANDIDATES = 200


class SquarePaymentAttempt(models.Model):
    _inherit = 'pos.square.payment.attempt'

    currency_id = fields.Many2one('res.currency', compute='_compute_reconciliation_amounts', compute_sudo=True)
    square_order_id = fields.Char(related='square_order_link_id.square_order_id')
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
        return ('dispatching', 'pending', 'declined', 'uncertain', 'timed_out', 'authorized', 'verified')

    def _record_successful_check(self):
        self.write({'last_checked_at': fields.Datetime.now(), 'reconciliation_error': False})

    def _record_failed_check(self, issue, *, discrepancy=False):
        values = {'reconciliation_error': issue}
        if discrepancy:
            values['reconciliation_discrepancy'] = issue
        # A failed read cannot release an authorization or change previously verified evidence.
        if self.state not in ('dispatching', 'timed_out', 'failed', 'authorized', 'verified'):
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
        return (
            Domain('square_order_link_id.cancelled', '=', False)
            & (Domain('state', 'in', self._square_reconcilable_states())
               | Domain('square_order_link_id.cancel_requested', '=', True))
            & (Domain('next_reconciliation_at', '=', False) | Domain('next_reconciliation_at', '<=', now))
        )

    @api.model
    def _cron_reconcile_payments(self):
        cron = self.env['ir.cron']
        due = self._reconciliation_domain(fields.Datetime.now())
        time_left = cron._commit_progress()
        seen_ids = []
        processed = 0
        verified = Domain('state', '=', 'verified')
        refund_issue = Domain('refund_reconciliation_issue', '!=', False)
        # Recover open checkouts and known refund issues before routine historical checks.
        for priority in (~verified, verified & refund_issue, verified & ~refund_issue):
            priority_due = due & priority
            while time_left and len(seen_ids) < RECONCILIATION_MAX_CANDIDATES:
                limit = min(RECONCILIATION_BATCH_SIZE, RECONCILIATION_MAX_CANDIDATES - len(seen_ids))
                candidates = self.search(
                    priority_due & Domain('id', 'not in', seen_ids), order='next_reconciliation_at, id', limit=limit,
                )
                for attempt_id in candidates.ids:
                    if not time_left:
                        break
                    seen_ids.append(attempt_id)
                    checked = self._reconcile_scheduled_attempt(attempt_id, priority_due)
                    processed += checked
                    time_left = cron._commit_progress(int(checked), remaining=1)
                if len(candidates) < limit:
                    break
        # Leave a continuation for the scheduler when there is more due work.
        # If every candidate was locked, wait for the next regular run.
        remaining = (processed or not time_left) and self.search_count(
            self._reconciliation_domain(fields.Datetime.now()), limit=1,
        )
        cron._commit_progress(remaining=int(remaining))

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
                        if (
                            attempt._square_reconcile_payment(continue_checkout=True)
                            and attempt.state == 'verified' and not attempt.reconciliation_error
                        ):
                            attempt.next_reconciliation_at = fields.Datetime.now() + timedelta(days=1)
                except (SerializationFailure, DeadlockDetected):
                    raise
                except Exception:
                    _logger.exception('Could not check Square Terminal payment attempt %s', attempt_id)
                return True
        except (SerializationFailure, DeadlockDetected):
            return False

    def _square_reconcile_payment(self, *, continue_checkout=False):
        self.ensure_one()
        order = self.order_id.with_company(self.company_id)
        link = self.square_order_link_id
        if continue_checkout and link.cancel_requested and not link.cancelled and self.state != 'verified':
            order._square_lock_checkout(touch=True)
            order._square_finish_cancellation(self)
            checked = link.cancelled or self.state == 'verified'
        else:
            checked = self._refresh()
            if (
                checked and continue_checkout and order.state == 'draft' and self.state == 'authorized'
                and link.split and not link.cancel_requested and not link.cancelled
            ):
                self._square_continue_split(order, link)
        if checked and self.state == 'verified':
            order._square_complete_recovered_order(self)
        return checked

    def _square_continue_split(self, order, link):
        had_cash_intent = link.cash_recording
        had_settlement_intent = bool(link.settlement_request)
        try:
            with self.env.cr.savepoint():
                order._square_lock_checkout(touch=True)
                if had_settlement_intent:
                    order._square_settle_split(self)
                elif had_cash_intent:
                    order._square_record_split_cash(self)
                else:
                    # Newly entered cash belongs to the cashier until Send or Validate reserves it.
                    order._square_prepare_split_settlement(self, reserve_cash=False)
        except UserError as error:
            self._record_failed_check(str(error))
            return
        if (
            self.state == 'authorized'
            and (had_cash_intent != link.cash_recording or had_settlement_intent != bool(link.settlement_request))
        ):
            # A later transaction may use the durable intent for the next provider call.
            self.next_reconciliation_at = fields.Datetime.now()

    def action_check_square_payment(self):
        self.ensure_one()
        self.check_access('read')
        if not self.env.user.has_group('point_of_sale.group_pos_manager'):
            raise AccessError(_("Sign in as a Point of Sale manager to check Square payments."))
        attempt = self.sudo()
        attempt._lock_reconciliation()
        attempt._square_reconcile_payment()
        return {'type': 'ir.actions.client', 'tag': 'reload'}


class PosOrder(models.Model):
    _inherit = 'pos.order'

    square_reconciliation_count = fields.Integer(
        compute='_compute_square_reconciliation_count', groups='point_of_sale.group_pos_manager',
    )

    def _square_complete_recovered_order(self, attempt):
        self.ensure_one()
        config = self.config_id
        if self.state != 'draft' or not config.auto_validate_terminal_payment or not config.invoice_journal_id:
            return
        attempts = self._square_current_attempts()
        if not attempts or any(share.state != 'verified' for share in attempts):
            return
        partner = self.partner_id
        preset = self.preset_id
        needs_customer = self.to_invoice or self.shipping_date or any(
            self.payment_ids.payment_method_id.mapped('split_transactions')
        )
        if (
            (needs_customer and not partner)
            or (self.shipping_date and not (partner.name and partner.street and partner.city and partner.country_id))
            or (preset.identification == 'name' and not (self.floating_order_name or partner))
            or (preset.identification == 'address' and not (partner.street or partner.street2))
            or (preset.use_timing and not self.preset_time)
        ):
            return
        if config.cash_rounding:
            rounding = config.rounding_method
            for payment in self.payment_ids.filtered('payment_method_id.is_cash_count'):
                rounded = float_round(
                    payment.amount, precision_rounding=rounding.rounding, rounding_method=rounding.rounding_method,
                )
                if not self.currency_id.is_zero(payment.amount - rounded):
                    return
        try:
            with self.env.cr.savepoint():
                self._square_check_session()
                self.write({'amount_paid': self._compute_amount_paid(), 'date_order': fields.Datetime.now()})
                self.with_context(tz=self.user_id.tz or 'UTC')._process_saved_order(False)
        except (SerializationFailure, DeadlockDetected):
            raise
        except Exception:
            _logger.exception('Could not finish recovered Square Terminal sale %s', self.id)
            attempt._record_failed_check(_("Square verified the payment. Odoo could not finish the original sale."))

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

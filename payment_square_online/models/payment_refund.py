from datetime import timedelta
from decimal import Decimal
from math import isfinite
from re import fullmatch
from urllib.parse import quote
from uuid import uuid4

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError

from odoo.addons.payment import utils as payment_utils
from odoo.addons.payment.const import CURRENCY_MINOR_UNITS

from ..square_api import SquareAPIError
from .payment_recovery import _DISPATCH_RESERVATION_TIMEOUT


_REFUND_REJECTIONS = {
    'CARD_MISMATCH', 'CARD_PRESENCE_REQUIRED', 'CARD_TOKEN_EXPIRED', 'GENERIC_DECLINE',
    'INSUFFICIENT_PERMISSIONS_FOR_REFUND', 'INVALID_CARD', 'INVALID_CARD_DATA', 'INVALID_FEES',
    'PAYMENT_NOT_REFUNDABLE', 'PAYMENT_NOT_REFUNDABLE_DUE_TO_DISPUTE', 'REFUND_AMOUNT_INVALID',
    'REFUND_DECLINED', 'REFUND_ERROR_PAYMENT_NEEDS_COMPLETION', 'UNSUPPORTED_CARD_BRAND',
    'UNSUPPORTED_COUNTRY', 'UNSUPPORTED_CURRENCY', 'UNSUPPORTED_INSTRUMENT_TYPE', 'UNSUPPORTED_SOURCE_TYPE',
}
_REFUND_STATUSES = ('PENDING', 'COMPLETED', 'REJECTED', 'FAILED')


class PaymentTransaction(models.Model):
    _inherit = 'payment.transaction'

    square_refund_key = fields.Char(readonly=True, copy=False)
    square_refund_requested_by_id = fields.Many2one(
        'res.users', string='Refund Requested By', readonly=True, copy=False, ondelete='restrict',
    )
    square_refund_origin = fields.Selection(
        [('odoo', 'Odoo'), ('square', 'Square')], string='Refund Origin', compute='_compute_square_refund_origin',
    )
    square_refund_wizard_id = fields.Integer(readonly=True, copy=False)
    square_refund_revision = fields.Integer(readonly=True, copy=False)
    square_refund_recheck_minor = fields.Integer(readonly=True, copy=False)

    _sql_constraints = [
        ('square_refund_key_unique', 'UNIQUE(square_refund_key)', 'Square refund requests must be unique.'),
    ]

    @api.depends('provider_code', 'operation', 'square_refund_key')
    def _compute_square_refund_origin(self):
        for tx in self:
            tx.square_refund_origin = (
                ('odoo' if tx.square_refund_key else 'square')
                if tx.provider_code == 'square_online' and tx.operation == 'refund' else False
            )

    def action_refund(self, amount_to_refund=None):
        self.check_access('read')
        square_payments = self.filtered(lambda tx: tx.provider_code == 'square_online')
        square_payments._square_check_refund_access()
        result = super(PaymentTransaction, self - square_payments).action_refund(amount_to_refund=amount_to_refund)
        for tx in square_payments:
            tx._send_refund_request(amount_to_refund=amount_to_refund)
        return result

    def _square_can_refund(self):
        return (
            self.env.user.has_group('account.group_account_manager')
            or self.env.user.has_group('base.group_system')
        )

    def _square_check_refund_access(self):
        if not self:
            return
        if not self._square_can_refund():
            raise AccessError(_("Only accounting managers and administrators can refund Square payments."))
        self.check_access('write')
        if any(tx.company_id not in self.env.companies for tx in self):
            raise AccessError(_("This Square payment belongs to a different company."))

    def _square_is_captured_payment(self):
        self.ensure_one()
        return (
            self.operation == 'online_direct' and self.state == 'done' and self.payment_id and self.square_dispatched_at
            and bool(self.provider_reference and self.square_connection_id and self.square_journal_id)
            and self.square_amount_minor > 0 and self.square_currency_code == self.currency_id.name
        )

    def _square_refundable_minor(self, *, excluding=None):
        self.ensure_one()
        if self.square_refund_recheck_minor or self.square_linked_refunds_pending or not self._square_is_captured_payment():
            return 0
        return self._square_recorded_refund_balance(excluding=excluding)

    def _square_recorded_refund_balance(self, *, excluding=None):
        reserved = self.child_transaction_ids.filtered(
            lambda tx: tx != excluding and tx.operation == 'refund' and tx.state in ('pending', 'done'),
        )
        return max(self.square_amount_minor - sum(reserved.mapped('square_amount_minor')), 0)

    def _square_refund_amount(self, amount_minor):
        return payment_utils.to_major_currency_units(amount_minor, self.currency_id)

    def _square_refund_minor(self, amount):
        self.ensure_one()
        if type(amount) not in (int, float) or (type(amount) is float and not isfinite(amount)) or amount <= 0:
            raise ValidationError(_("The Square refund amount must be a positive number."))
        currency = self.currency_id
        digits = CURRENCY_MINOR_UNITS.get(currency.name, currency.decimal_places)
        amount_minor = Decimal(str(amount)).scaleb(digits)
        if amount_minor != amount_minor.to_integral_value():
            raise ValidationError(_("The Square refund amount must use whole minor units of %s.", currency.name))
        return int(amount_minor)

    def _send_refund_request(self, amount_to_refund=None, *, wizard_id=None):
        self.ensure_one()
        if self.provider_code != 'square_online':
            return super()._send_refund_request(amount_to_refund=amount_to_refund)

        self._square_check_refund_access()
        # Use a narrow context for records created by the integration. The RPC context can
        # contain defaults for transaction, invoice and payment fields.
        context = {'allowed_company_ids': self.env.companies.ids}
        uid = self.env.uid
        with self.env.registry.cursor() as cr:
            source = api.Environment(cr, uid, context)['payment.transaction'].browse(self.id)
            source._square_check_refund_access()
            refund, created = source.sudo().with_company(source.company_id)._square_reserve_refund(
                amount_to_refund, wizard_id, uid,
            )
            refund_id = refund.id

        if created:
            # First commit the pending child, then commit the dispatch claim. If Odoo stops
            # after the POST, status checks can still find and verify the original request.
            with self.env.registry.cursor() as cr:
                refund = api.Environment(cr, uid, context)['payment.transaction'].sudo().browse(refund_id)
                source = refund.source_transaction_id
                source._square_lock_for_status()
                refund._square_lock_for_status()
                if refund.state != 'pending' or refund.square_dispatched_at:
                    return self.browse(refund_id)
                if refund.square_amount_minor > source._square_refundable_minor(excluding=refund):
                    refund._set_error(_("Another Square refund reduced the remaining balance before Odoo sent this refund."))
                    source.square_refund_revision += 1
                    return self.browse(refund_id)
                refund.square_dispatched_at = fields.Datetime.now()

            # Release the credential lock before applying the financial result. Refund recovery
            # takes the source lock before making read requests.
            with self.env.registry.cursor() as cr:
                refund = api.Environment(cr, uid, context)['payment.transaction'].sudo().browse(refund_id)
                response, error_message, check_balance = refund.with_company(refund.company_id)._square_dispatch_refund()
            if response or error_message:
                with self.env.registry.cursor() as cr:
                    refund = api.Environment(cr, uid, context)['payment.transaction'].sudo().browse(refund_id)
                    source = refund.source_transaction_id
                    source._square_lock_for_status()
                    refund._square_lock_for_status()
                    if response:
                        refund._square_apply_refund(response)
                    elif refund.state == 'pending' and not refund.provider_reference:
                        refund._set_error(error_message)
                        source.square_refund_revision += 1
                        if check_balance:
                            amount = refund.square_amount_minor
                            if source._square_recorded_refund_balance() >= amount:
                                source.square_refund_recheck_minor = min(source.square_refund_recheck_minor or amount, amount)
                            source._square_refresh_status(discover_refunds=True)
        return self.browse(refund_id)

    def _square_reserve_refund(self, amount_to_refund, wizard_id, actor_id):
        self.ensure_one()
        self._square_lock_for_status()
        if wizard_id:
            existing = self.child_transaction_ids.filtered(lambda tx: tx.square_refund_wizard_id == wizard_id)
            if existing:
                return existing, False
        if not self._square_is_captured_payment():
            raise ValidationError(_("Only captured Square online payments recorded in Odoo can be refunded."))
        if self.square_refund_recheck_minor:
            raise UserError(_("Square reported a lower refundable balance. Refresh the original payment before trying again."))

        available = self._square_refundable_minor()
        if not available:
            raise UserError(_("This Square payment has no amount available to refund."))
        amount_minor = available if amount_to_refund is None else self._square_refund_minor(amount_to_refund)
        if amount_minor > available:
            raise ValidationError(_("The Square refund amount exceeds the remaining refundable balance."))
        if not wizard_id and amount_minor < available:
            raise ValidationError(_("Use the payment's Refund wizard to request a partial Square refund."))

        # Updating the shared source makes a competing repeatable-read snapshot retry before
        # summing its children, even when that snapshot predates the new child.
        self.square_refund_revision += 1
        refund = self._square_create_refund_child(
            amount_minor,
            square_refund_key=f'odoo-refund-{uuid4().hex}',
            square_refund_wizard_id=wizard_id or False,
            square_refund_requested_by_id=actor_id,
            square_reserved_at=fields.Datetime.now(),
        )
        refund._log_sent_message()
        refund._set_pending(state_message=_("Square is processing this refund."))
        return refund, True

    def _square_create_refund_child(self, amount_minor, **values):
        self.ensure_one()
        return self._create_child_transaction(
            self._square_refund_amount(amount_minor), is_refund=True,
            square_amount_minor=amount_minor,
            square_currency_code=self.square_currency_code,
            square_connection_id=self.square_connection_id.id,
            square_merchant_id=self.square_merchant_id,
            square_environment=self.square_environment,
            square_application_id=self.square_application_id,
            square_location_ref=self.square_location_ref,
            square_journal_id=self.square_journal_id.id,
            **values,
        )

    def _square_dispatch_refund(self):
        self.ensure_one()
        try:
            response = self._square_request('POST', '/v2/refunds', payload={
                'idempotency_key': self.square_refund_key,
                'payment_id': self.source_transaction_id.provider_reference,
                'amount_money': self._square_money(),
                'reason': self.square_refund_key,
            })
        except UserError as error:
            return None, error.args[0], False
        except SquareAPIError as error:
            if error.is_authorization_error:
                return None, _("Reconnect the original Square account before requesting a new refund."), False
            if error.status in (400, 402) and error.codes and set(error.codes) <= _REFUND_REJECTIONS:
                return (
                    None, _("Square rejected the refund. Check the original payment at Square for details."),
                    'REFUND_AMOUNT_INVALID' in error.codes,
                )
            return None, None, False
        return response.get('refund'), None, False

    def _square_discover_linked_refunds(self, payment):
        """Check the refund IDs Square reports on a verified original payment."""
        refund_ids = payment.get('refund_ids', [])
        verified = isinstance(refund_ids, list) and all(isinstance(value, str) and value for value in refund_ids)
        if self.square_linked_refunds_pending and not refund_ids:
            verified = False
        if verified:
            for refund_id in dict.fromkeys(refund_ids):
                try:
                    if not self._square_fetch_linked_refund(refund_id):
                        verified = False
                except (UserError, SquareAPIError):
                    verified = False
        if self.square_refund_recheck_minor:
            self.square_recovery_message = _("Square reported a lower refundable balance. Odoo is waiting for the related refund details.")
            return False
        if not verified:
            self.square_recovery_message = _("Odoo could not verify all refunds for this Square payment. Try refreshing its status again later.")
        return verified

    def _square_fetch_linked_refund(self, refund_id):
        response = self._square_request('GET', f'/v2/refunds/{quote(refund_id, safe="")}')
        refund = response.get('refund')
        return isinstance(refund, dict) and refund.get('id') == refund_id and self._square_ingest_refund(refund)

    def _square_matches_refund(self, refund, payment_id):
        if not isinstance(refund, dict):
            return False
        refund_id = refund.get('id')
        money = refund.get('amount_money')
        return (
            isinstance(refund_id, str) and bool(refund_id)
            and refund.get('payment_id') == payment_id
            and refund.get('location_id') == self.square_location_ref
            and refund.get('status') in _REFUND_STATUSES
            and isinstance(money, dict) and money.keys() == {'amount', 'currency'}
            and money['currency'] == self.square_currency_code and type(money['amount']) is int
        )

    def _square_ingest_refund(self, refund):
        """Record an existing Square refund retrieved with the original account's authorization."""
        self.ensure_one()
        self._square_lock_for_status()
        if not self._square_is_captured_payment() or not self._square_matches_refund(refund, self.provider_reference):
            return False
        refund_id = refund['id']
        money = refund['amount_money']
        if not 0 < money['amount'] <= self.square_amount_minor:
            return False

        children = self.child_transaction_ids.filtered(lambda tx: tx.operation == 'refund')
        child = children.filtered(lambda tx: tx.provider_reference == refund_id)
        reason = refund.get('reason')
        marked = not child and isinstance(reason, str) and fullmatch(r'odoo-refund-[0-9a-f]{32}', reason)
        if marked:
            # An accepted request may still have no Square ID if its response was lost.
            child = children.filtered(lambda tx: tx.square_refund_key == reason and tx.square_dispatched_at)
            if not child:
                return False
        unlinked = not child or not child.provider_reference
        if not child:
            # Share the parent revision with local reservations to serialize changes to the balance.
            self.square_refund_revision += 1
            child = self.with_company(self.company_id)._square_create_refund_child(
                money['amount'], square_reserved_at=fields.Datetime.now(),
            )
            child._set_pending(state_message=_("Square is processing this refund."))
        child._square_lock_for_status()
        applied = child._square_apply_refund(refund, require_marker=bool(marked))
        if applied and unlinked and self.square_refund_recheck_minor:
            # A newly verified refund may have failed since Square rejected our request.
            available = self._square_recorded_refund_balance()
            if child.state == 'error':
                available -= money['amount']
            if available < self.square_refund_recheck_minor:
                self.square_refund_recheck_minor = 0
                self.square_recovery_message = False
        return applied

    def _square_apply_refund(self, refund, *, require_marker=False):
        """Apply a verified response while holding the payment and refund transaction locks."""
        self.ensure_one()
        if not self._square_matches_refund(refund, self.source_transaction_id.provider_reference):
            return False
        refund_id = refund['id']
        status = refund['status']
        if (
            (self.provider_reference and self.provider_reference != refund_id)
            or refund['amount_money']['amount'] != self.square_amount_minor
            or (require_marker and (not self.square_refund_key or refund.get('reason') != self.square_refund_key))
        ):
            return False
        if self.state != 'pending':
            if self.provider_reference != refund_id:
                return False
            self.square_recovery_message = False
            self._square_post_process_recovered_payment()
            return True
        values = {'provider_reference': refund_id, 'square_recovery_message': False}
        if isinstance(refund.get('order_id'), str) and refund['order_id']:
            values['square_order_id'] = refund['order_id']
        self.write(values)
        if status == 'COMPLETED':
            self._set_done()
        elif status in ('REJECTED', 'FAILED'):
            self._set_error(_("Square reported that this refund failed."))
            self.source_transaction_id.square_refund_revision += 1
        self._square_post_process_recovered_payment()
        return True

    def _square_refresh_refund(self):
        if self.state != 'pending' and not self.provider_reference:
            return False
        if not self.provider_reference and not self.square_dispatched_at:
            if (
                self.state == 'pending' and self.square_reserved_at
                and self.square_reserved_at <= fields.Datetime.now() - _DISPATCH_RESERVATION_TIMEOUT
            ):
                self._set_error(_("Odoo stopped before sending the refund to Square. You can try again."))
                self.source_transaction_id.square_refund_revision += 1
                return True
            return False

        try:
            verified = self._square_fetch_refund()
        except UserError as error:
            self.square_recovery_message = error.args[0]
            return False
        except SquareAPIError as error:
            self.square_recovery_message = (
                _("Reconnect the original Square account to recover this transaction.")
                if error.is_authorization_error
                else _("Odoo could not check the Square refund. Try refreshing its status again later.")
            )
            return False
        if not verified:
            self.square_recovery_message = _("Square has not returned a matching refund. Odoo will check again.")
            return False

        return True

    def _square_fetch_refund(self):
        if self.provider_reference:
            response = self._square_request('GET', f'/v2/refunds/{quote(self.provider_reference, safe="")}')
            return self._square_apply_refund(response.get('refund'))

        earliest = self.square_dispatched_at - timedelta(minutes=5)
        params = {'location_id': self.square_location_ref, 'begin_time': earliest.isoformat() + 'Z'}
        cursors = set()
        while True:
            response = self._square_request('GET', '/v2/refunds', params=params)
            refunds = response.get('refunds', [])
            if not isinstance(refunds, list):
                return False
            if any(self._square_apply_refund(refund, require_marker=True) for refund in refunds):
                return True
            cursor = response.get('cursor')
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                return False
            cursors.add(cursor)
            params = {**params, 'cursor': cursor}

    def _create_payment(self, **extra_create_values):
        self.ensure_one()
        if self.provider_code == 'square_online' and self.operation == 'refund':
            journal = self.square_journal_id
            lines = journal.outbound_payment_method_line_ids
            method = lines.filtered(lambda line: line.code == 'manual')[:1]
            if not method:
                raise UserError(_("Enable the Manual outbound payment method on the original Square journal."))
            extra_create_values = {
                'journal_id': journal.id,
                'payment_method_line_id': method.id,
                **extra_create_values,
            }
        return super()._create_payment(**extra_create_values)

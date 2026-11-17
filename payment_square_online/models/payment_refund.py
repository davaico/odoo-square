from datetime import timedelta
from math import isfinite
from urllib.parse import quote
from uuid import uuid4

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError

from odoo.addons.payment import utils as payment_utils

from ..square_api import SquareAPIError
from .payment_recovery import _DISPATCH_RESERVATION_TIMEOUT


_REFUND_REJECTIONS = {
    'INSUFFICIENT_PERMISSIONS_FOR_REFUND', 'PAYMENT_NOT_REFUNDABLE',
    'PAYMENT_NOT_REFUNDABLE_DUE_TO_DISPUTE', 'REFUND_AMOUNT_INVALID', 'REFUND_DECLINED',
    'REFUND_ERROR_PAYMENT_NEEDS_COMPLETION',
}


class PaymentTransaction(models.Model):
    _inherit = 'payment.transaction'

    square_refund_key = fields.Char(readonly=True, copy=False)
    square_refund_requested_by_id = fields.Many2one(
        'res.users', string='Refund Requested By', readonly=True, copy=False, ondelete='restrict',
    )
    square_refund_wizard_id = fields.Integer(readonly=True, copy=False)
    square_refund_revision = fields.Integer(readonly=True, copy=False)

    _sql_constraints = [
        ('square_refund_key_unique', 'UNIQUE(square_refund_key)', 'Square refund requests must be unique.'),
    ]

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

    def _square_refundable_minor(self):
        self.ensure_one()
        if not self._square_is_captured_payment():
            return 0
        reserved = self.child_transaction_ids.filtered(
            lambda tx: tx.operation == 'refund' and tx.state in ('pending', 'done'),
        )
        return max(self.square_amount_minor - sum(reserved.mapped('square_amount_minor')), 0)

    def _square_refund_amount(self, amount_minor):
        return payment_utils.to_major_currency_units(
            amount_minor, self.currency_id, arbitrary_decimal_number=0 if self.square_currency_code == 'JPY' else 2,
        )

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
                refund._square_lock_for_status()
                if refund.state != 'pending' or refund.square_dispatched_at:
                    return self.browse(refund_id)
                refund.square_dispatched_at = fields.Datetime.now()

            with self.env.registry.cursor() as cr:
                refund = api.Environment(cr, uid, context)['payment.transaction'].sudo().browse(refund_id)
                refund._square_lock_for_status()
                if refund.state == 'pending':
                    refund.with_company(refund.company_id)._square_dispatch_refund()
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

        available = self._square_refundable_minor()
        if not available:
            raise UserError(_("This Square payment has no amount available to refund."))
        if amount_to_refund is not None and (
            type(amount_to_refund) not in (int, float)
            or (type(amount_to_refund) is float and not isfinite(amount_to_refund))
            or amount_to_refund != self._square_refund_amount(available)
        ):
            raise ValidationError(_("Refund the full remaining Square payment amount."))

        # Updating the shared source makes a competing repeatable-read snapshot retry before
        # summing its children, even when that snapshot predates the new child.
        self.square_refund_revision += 1
        refund = self._square_create_refund_child(
            available,
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
            self._set_error(error.args[0])
        except SquareAPIError as error:
            if error.is_authorization_error:
                self._set_error(_("Reconnect the original Square account before requesting a new refund."))
            elif error.status in (400, 402) and error.codes and set(error.codes) <= _REFUND_REJECTIONS:
                self._set_error(_("Square rejected the refund. Check the original payment at Square for details."))
        else:
            self._square_apply_refund(response.get('refund'))

    def _square_apply_refund(self, refund, *, require_marker=False):
        """Apply a verified response while holding the refund transaction lock."""
        self.ensure_one()
        if not isinstance(refund, dict):
            return False
        refund_id = refund.get('id')
        status = refund.get('status')
        money = refund.get('amount_money')
        if (
            not isinstance(refund_id, str) or not refund_id
            or (self.provider_reference and self.provider_reference != refund_id)
            or refund.get('payment_id') != self.source_transaction_id.provider_reference
            or refund.get('location_id') != self.square_location_ref
            or money != self._square_money() or type(money['amount']) is not int
            or (require_marker and (not self.square_refund_key or refund.get('reason') != self.square_refund_key))
            or status not in ('PENDING', 'COMPLETED', 'REJECTED', 'FAILED')
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
        self._square_post_process_recovered_payment()
        return True

    def _square_refresh_refund(self):
        if self.state != 'pending' and not self.provider_reference:
            return False
        if not self.square_dispatched_at:
            if (
                self.state == 'pending' and self.square_reserved_at
                and self.square_reserved_at <= fields.Datetime.now() - _DISPATCH_RESERVATION_TIMEOUT
            ):
                self._set_error(_("Odoo stopped before sending the refund to Square. You can try again."))
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

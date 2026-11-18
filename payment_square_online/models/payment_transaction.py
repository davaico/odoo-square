# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from uuid import uuid4

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import AccessError, ValidationError
from odoo.http import request
from odoo.tools import SQL

from odoo.addons.payment import utils as payment_utils

from ..square_api import MINOR_UNIT_DIGITS, SquareAPIError, square_amount_in_minor_units


_logger = logging.getLogger(__name__)
_TERMINAL_STATES = {"done", "error", "cancel"}
_SQUARE_STATES = {
    "APPROVED": "pending",
    "PENDING": "pending",
    "COMPLETED": "done",
    "FAILED": "error",
    "CANCELED": "cancel",
}
# Definitive CreatePayment rejections documented by Square. A used token, an
# idempotency conflict, or an unknown code does not establish non-collection.
_CARD_DECLINE_CODES = {
    "ADDRESS_VERIFICATION_FAILURE", "ALLOWABLE_PIN_TRIES_EXCEEDED", "BAD_EXPIRATION", "CARD_DECLINED",
    "CARD_DECLINED_CALL_ISSUER", "CARD_EXPIRED", "CARDHOLDER_INSUFFICIENT_PERMISSIONS", "CARD_NOT_SUPPORTED",
    "CARD_TOKEN_EXPIRED", "CHIP_INSERTION_REQUIRED", "CVV_FAILURE", "EXPIRATION_FAILURE", "GENERIC_DECLINE",
    "INSUFFICIENT_FUNDS", "INVALID_ACCOUNT", "INVALID_CARD", "INVALID_CARD_DATA", "INVALID_EXPIRATION",
    "INVALID_PIN", "INVALID_POSTAL_CODE", "MANUALLY_ENTERED_PAYMENT_NOT_SUPPORTED", "PAN_FAILURE",
    "TRANSACTION_LIMIT", "VERIFY_AVS_FAILURE", "VERIFY_CVV_FAILURE", "VOICE_FAILURE",
}
_AUTHENTICATION_CODES = {"CARD_DECLINED_VERIFICATION_REQUIRED", "VERIFY_BUYER_FAILED"}
_AMOUNT_REJECTION_CODES = {"AMOUNT_TOO_HIGH", "AMOUNT_TOO_LOW", "PAYMENT_AMOUNT_MISMATCH", "PAYMENT_LIMIT_EXCEEDED"}
_UNAVAILABLE_PAYMENT_CODES = {
    "INSUFFICIENT_PERMISSIONS", "INVALID_EMAIL_ADDRESS", "INVALID_FEES", "INVALID_LOCATION",
    "INVALID_PHONE_NUMBER", "PAYMENT_SOURCE_NOT_ENABLED_FOR_TARGET", "UNSUPPORTED_CURRENCY",
}
_DEFINITIVE_CHARGE_CODES = (
    _CARD_DECLINE_CODES | _AUTHENTICATION_CODES | _AMOUNT_REJECTION_CODES | _UNAVAILABLE_PAYMENT_CODES
)


class PaymentTransaction(models.Model):
    _inherit = "payment.transaction"

    square_online_order_id = fields.Many2one(
        "sale.order", readonly=True, copy=False, ondelete="restrict", index="btree_not_null",
    )
    square_online_website_id = fields.Many2one("website", readonly=True, copy=False, ondelete="restrict")
    square_online_connection_id = fields.Many2one(
        "square.online.connection", readonly=True, copy=False, ondelete="restrict",
    )
    square_online_journal_id = fields.Many2one(
        "account.journal", readonly=True, copy=False, ondelete="restrict",
    )
    square_online_operation_key = fields.Char(readonly=True, copy=False)
    square_online_dispatch_at = fields.Datetime(readonly=True, copy=False)
    square_online_amount_minor = fields.Integer(readonly=True, copy=False)
    square_online_currency_code = fields.Char(readonly=True, copy=False)
    square_online_merchant_id = fields.Char(readonly=True, copy=False)
    square_online_environment = fields.Char(readonly=True, copy=False)
    square_online_location_id = fields.Char(readonly=True, copy=False)
    square_online_order_reference = fields.Char(readonly=True, copy=False, index="btree_not_null")
    square_online_cart_snapshot = fields.Char(readonly=True, copy=False)
    square_online_has_order_mismatch = fields.Boolean(
        string="Square Order Requires Review", readonly=True, copy=False,
    )

    _square_online_operation_key_unique = models.Constraint(
        "UNIQUE(square_online_operation_key)", "Each Square payment operation must be unique.",
    )
    _square_online_provider_reference_unique = models.UniqueIndex(
        "(square_online_environment, square_online_merchant_id, provider_reference) "
        "WHERE square_online_dispatch_at IS NOT NULL AND operation = 'online_direct'",
        "A Square payment can only belong to one Odoo payment transaction.",
    )

    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.su and (
            any(name.startswith("default_square_online_") for name in self.env.context)
            or any(
                any(name.startswith("square_online_") for name in vals)
                or self.env["payment.provider"].sudo().browse(vals.get("provider_id")).code == "square_online"
                for vals in vals_list
            )
        ):
            raise AccessError(_("Square payments are created through website checkout."))
        transactions = super().create(vals_list)
        for tx in transactions.filtered(lambda tx: tx.provider_code != "square_online" and tx.operation in (
            "online_direct", "online_redirect", "online_token", "validation",
        )):
            # Both native sales portals create the transaction before calling a provider.
            tx.sale_order_ids.filtered("website_id")._square_online_guard_other_checkout(tx)
        return transactions

    def write(self, vals):
        if not self.env.su and (
            self.filtered(lambda tx: tx.provider_code == "square_online")
            or any(name.startswith("square_online_") for name in vals)
            or (
                "provider_id" in vals
                and self.env["payment.provider"].sudo().browse(vals["provider_id"]).code == "square_online"
            )
        ):
            raise AccessError(_("Square payment transactions are updated by the payment provider."))
        return super().write(vals)

    @api.ondelete(at_uninstall=False)
    def _unlink_except_square_online_operation(self):
        if any(self.mapped("square_online_dispatch_at")):
            raise AccessError(_("Square payment operations must be kept for payment history."))

    def _square_online_validate_checkout(self, order, website_id):
        self.ensure_one()
        provider = self.provider_id
        connection = provider._square_online_get_connection()
        # Match the native sale portal's buyer even when the invoice has a different contact.
        buyer = order.partner_id
        if request and not request.env.user._is_public():
            buyer = request.env.user.partner_id
        providers = provider.with_context(square_online_website_checkout=True)._get_compatible_providers(
            order.company_id.id, buyer.id, order.amount_total,
            currency_id=order.currency_id.id, website_id=website_id, sale_order_id=order.id,
        )
        methods = self.env["payment.method"]._get_compatible_payment_methods(
            (provider & providers).ids, buyer.id, currency_id=order.currency_id.id,
            website_id=website_id, sale_order_id=order.id,
        )
        if (
            not order.website_id
            or order.website_id.id != website_id
            or order.company_id != order.website_id.company_id
            or order.company_id != provider.company_id
            or order.state not in ("draft", "sent")
            or self.sale_order_ids != order
            or self.partner_id != order.partner_invoice_id
            or self.currency_id != order.currency_id
            or not connection
            or connection.company_id != order.company_id
            or not provider.is_published
            or provider not in providers
            or self.operation != "online_direct"
            or self.payment_method_id != self.env.ref("payment.payment_method_card")
            or self.payment_method_id not in methods
            or self.tokenize
            or self.token_id
        ):
            raise ValidationError(_("Square is unavailable for this order. Refresh the checkout page."))
        try:
            minor = square_amount_in_minor_units(order.amount_total, order.currency_id.name)
            tx_minor = square_amount_in_minor_units(self.amount, self.currency_id.name)
        except ValueError:
            raise ValidationError(_("Square cannot process this checkout amount.")) from None
        if not 0 < minor <= 2147483647 or minor != tx_minor:
            raise ValidationError(_("The cart total changed. Refresh the checkout page."))
        order._check_cart_is_ready_to_be_paid()
        return connection, minor

    def _square_online_prepare_checkout(self, order, website_id):
        connection, minor = self._square_online_validate_checkout(order, website_id)
        self.write({
            "square_online_order_id": order.id,
            "square_online_website_id": website_id,
            "square_online_journal_id": self.provider_id.journal_id.id,
            "square_online_connection_id": connection.id,
            "square_online_operation_key": uuid4().hex,
            "square_online_amount_minor": minor,
            "square_online_currency_code": order.currency_id.name,
            "square_online_merchant_id": connection.merchant_id,
            "square_online_environment": connection.environment,
            "square_online_location_id": connection.location_id.square_id,
            "square_online_cart_snapshot": order._square_online_cart_snapshot(),
        })

    def _square_online_submit_token(self):
        self.ensure_one()
        return payment_utils.generate_access_token(
            "square-online-submit", self.id, self.square_online_order_id.id,
            self.square_online_website_id.id, self.square_online_operation_key, env=self.env,
        )

    def _get_specific_processing_values(self, processing_values):
        values = super()._get_specific_processing_values(processing_values)
        if self.provider_code != "square_online":
            return values
        if not self.square_online_operation_key:
            raise ValidationError(_("Square is only available in website checkout."))
        connection = self.square_online_connection_id
        digits = MINOR_UNIT_DIGITS[self.square_online_currency_code]
        return {
            **values,
            "square_online_transaction_id": self.id,
            "square_online_access_token": self._square_online_submit_token(),
            "square_online_application_id": connection.application_id,
            "square_online_location_id": self.square_online_location_id,
            "square_online_verification": {
                "amount": f"{self.square_online_amount_minor / 10 ** digits:.{digits}f}",
                "currencyCode": self.square_online_currency_code,
                "intent": "CHARGE",
                "customerInitiated": True,
                "sellerKeyedIn": False,
                "billingContact": self.square_online_order_id._square_online_billing_contact(),
            },
        }

    def _square_online_lock_payment(self):
        self.ensure_one()
        self.square_online_order_id._square_online_lock_checkout()
        self.env.cr.execute(SQL(
            "SELECT id FROM payment_transaction WHERE id = %s FOR NO KEY UPDATE", self.id,
        ))

    def _square_online_claim_payment(self, website_id, cart_id):
        self.ensure_one()
        self._square_online_lock_payment()
        if self.square_online_dispatch_at:
            return False
        order = self.square_online_order_id
        if (
            self.state != "draft"
            or cart_id != order.id
            or order._square_online_block_conflicting_checkout(self)
        ):
            raise ValidationError(_("Payment is already being processed or the checkout has changed."))
        connection, minor = self._square_online_validate_checkout(order, website_id)
        if (
            connection != self.square_online_connection_id
            or self.provider_id.journal_id != self.square_online_journal_id
            or connection.location_id.square_id != self.square_online_location_id
            or order._square_online_cart_snapshot() != self.square_online_cart_snapshot
            or minor != self.square_online_amount_minor
        ):
            raise ValidationError(_("The checkout changed. Refresh the checkout page."))
        # A renewal failure before dispatch must leave checkout free to try again.
        try:
            connection._square_online_sync_access()
        except SquareAPIError:
            raise ValidationError(_("Square is temporarily unavailable. Try another payment method.")) from None
        order._square_online_mark_checkout_change()
        self.square_online_dispatch_at = fields.Datetime.now()
        self._set_pending(state_message=_("Square is processing your payment."))
        return True

    def _square_online_submit(self, website_id, cart_id, source_id):
        self.ensure_one()
        # Commit the claim independently of the HTTP transaction. Odoo may replay the
        # request after a local failure, and a replay must not send another CreatePayment.
        registry, tx_id = self.env.registry, self.id
        context = {"lang": self.env.lang}
        with registry.cursor() as cr:
            claimed = api.Environment(cr, SUPERUSER_ID, context)[self._name].browse(tx_id)
            should_send = claimed._square_online_claim_payment(website_id, cart_id)
        if should_send:
            with registry.cursor() as cr:
                claimed = api.Environment(cr, SUPERUSER_ID, context)[self._name].browse(tx_id)
                claimed._square_online_send_payment(source_id)
        with registry.cursor() as cr:
            tx = api.Environment(cr, SUPERUSER_ID, context)[self._name].browse(tx_id)
            return {"state": tx.state, "state_message": tx.state_message}

    def _square_online_send_payment(self, source_id):
        self.ensure_one()
        connection = self.square_online_connection_id
        try:
            result = connection._square_online_request("POST", "/v2/payments", payload={
                "source_id": source_id,
                "idempotency_key": self.square_online_operation_key,
                "reference_id": self.square_online_operation_key,
                "location_id": self.square_online_location_id,
                "amount_money": {
                    "amount": self.square_online_amount_minor,
                    "currency": self.square_online_currency_code,
                },
                "autocomplete": True,
                "accept_partial_authorization": False,
                "customer_details": {"customer_initiated": True, "seller_keyed_in": False},
            })
        except SquareAPIError as exc:
            self._square_online_apply_charge_error(exc)
        else:
            if not self._square_online_apply_payment(result.get("payment"), connection):
                _logger.warning("Square checkout received an unverified payment for transaction %s.", self.id)

    def _square_online_apply_charge_error(self, error):
        codes = set(error.codes)
        if error.authorization_failed or (error.status_code == 403 and codes == {"CARD_PROCESSING_NOT_ENABLED"}):
            message = _("Square is temporarily unavailable. Try another payment method.")
        elif error.status_code not in (400, 402) or not codes or not codes <= _DEFINITIVE_CHARGE_CODES:
            return
        elif codes & _UNAVAILABLE_PAYMENT_CODES:
            message = _("Square is temporarily unavailable. Try another payment method.")
        elif codes & _AMOUNT_REJECTION_CODES:
            message = _("Square could not accept the payment amount. Try another payment method.")
        elif codes & _AUTHENTICATION_CODES:
            message = _("Card authentication is required. Try again and complete the bank's verification.")
        else:
            message = _("The card was declined. Check your details or try a different card.")
        self._square_online_lock_payment()
        if self.state == "pending" and not self.provider_reference:
            self._set_error(message)

    def _square_online_apply_payment(self, payment, connection):
        self.ensure_one()
        if not self.square_online_order_id:
            return False
        self._square_online_lock_payment()
        if not self._square_online_matches_payment(payment, connection):
            return False
        target = _SQUARE_STATES[payment["status"]]
        if self.state in _TERMINAL_STATES and self.state != target:
            # Late non-terminal signals are expected around the final payment response.
            return target == "pending"
        if self.state not in {"pending", target}:
            return False

        values = {"provider_reference": payment["id"]}
        if payment.get("order_id"):
            values["square_online_order_reference"] = payment["order_id"]
        self.write({name: value for name, value in values.items() if self[name] != value})
        if self.state == "pending" and target in _TERMINAL_STATES:
            self._process("square_online", {"square_online_payment": payment})
        if self.state == "done" and not self.is_post_processed:
            self._square_online_record_order_mismatch()
            self.env.ref("payment.cron_post_process_payment_tx")._trigger()
        return True

    def _square_online_matches_payment(self, payment, connection):
        if (
            not isinstance(payment, dict)
            or not self.square_online_dispatch_at
            or connection != self.square_online_connection_id
            or not connection
            or connection.merchant_id != self.square_online_merchant_id
            or connection.environment != self.square_online_environment
            or self.operation != "online_direct"
            or self.provider_code != "square_online"
        ):
            return False
        status = payment.get("status")
        payment_id = payment.get("id")
        order_id = payment.get("order_id")
        money = payment.get("amount_money")
        total = payment.get("total_money")
        application = payment.get("application_details") or {}
        return (
            isinstance(status, str) and status in _SQUARE_STATES
            and isinstance(payment_id, str) and 0 < len(payment_id) <= 192
            and (not self.provider_reference or self.provider_reference == payment_id)
            and (order_id is None or isinstance(order_id, str) and 0 < len(order_id) <= 192)
            and (not self.square_online_order_reference or not order_id
                 or self.square_online_order_reference == order_id)
            and payment.get("reference_id") == self.square_online_operation_key
            and payment.get("location_id") == self.square_online_location_id
            and payment.get("source_type") == "CARD"
            and isinstance(application, dict)
            and application.get("application_id") in (None, connection.application_id)
            and isinstance(money, dict) and type(money.get("amount")) is int
            and money["amount"] == self.square_online_amount_minor
            and money.get("currency") == self.square_online_currency_code
            and (total is None or total == money)
        )

    def _extract_amount_data(self, payment_data):
        if payment := payment_data.get("square_online_payment"):
            digits = MINOR_UNIT_DIGITS[self.square_online_currency_code]
            return {
                "amount": payment["amount_money"]["amount"] / 10 ** digits,
                "currency_code": payment["amount_money"]["currency"],
                "precision_digits": digits,
            }
        return super()._extract_amount_data(payment_data)

    def _apply_updates(self, payment_data):
        if not (payment := payment_data.get("square_online_payment")):
            return super()._apply_updates(payment_data)
        status = payment["status"]
        if status == "COMPLETED":
            self._set_done()
        elif status == "FAILED":
            self._set_error(_("The card payment failed. Check your details or try another card."))
        elif status == "CANCELED":
            self._set_canceled(_("The card payment was cancelled. You can try another payment."))

    def _post_process(self):
        square_payments = self.filtered(
            lambda tx: tx.provider_code == "square_online" and tx.operation == "online_direct"
        )
        super(PaymentTransaction, self - square_payments)._post_process()
        for tx in square_payments.filtered(lambda tx: tx.state in _TERMINAL_STATES):
            tx._square_online_lock_payment()
            if tx.is_post_processed:
                continue
            if tx.state == "done":
                # The order may have changed between verified ingestion and accounting.
                tx._square_online_record_order_mismatch()
            super(PaymentTransaction, tx)._post_process()
            # Native accounting skips the parent when a child has already completed.
            # Square's refund children never replace the original inbound payment.
            if tx.state == "done" and not tx.payment_id:
                tx.with_company(tx.company_id)._create_payment()

    def _square_online_record_order_mismatch(self):
        self.ensure_one()
        order = self.square_online_order_id
        if not self.square_online_has_order_mismatch and (
            self.sale_order_ids != order
            or order.state == "cancel"
            or order._square_online_cart_snapshot() != self.square_online_cart_snapshot
        ):
            self.square_online_has_order_mismatch = True
            order.message_post(body=_(
                "Square collected this payment after the order changed. Review the payment "
                "and the order before fulfilling it."
            ))

    def _check_amount_and_confirm_order(self):
        eligible = self.filtered(lambda tx: not tx.square_online_has_order_mismatch)
        return super(PaymentTransaction, eligible)._check_amount_and_confirm_order()

    def _invoice_sale_orders(self):
        eligible = self.filtered(lambda tx: not tx.square_online_has_order_mismatch)
        return super(PaymentTransaction, eligible)._invoice_sale_orders()

    def _create_payment(self, **extra_create_values):
        if self.provider_code == "square_online" and self.operation == "online_direct":
            journal = self.square_online_journal_id
            lines = journal.inbound_payment_method_line_ids
            method = lines.filtered(lambda line: line.payment_provider_id == self.provider_id)[:1]
            if not method:
                method = lines.filtered(lambda line: line.code == "manual")[:1]
            extra_create_values = {
                "journal_id": journal.id,
                "payment_method_line_id": method.id,
                **extra_create_values,
            }
        return super()._create_payment(**extra_create_values)

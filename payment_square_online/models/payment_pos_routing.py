# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from datetime import UTC, datetime, timedelta

from odoo import api, models
from odoo.fields import Domain


class PaymentTransaction(models.Model):
    _inherit = "payment.transaction"

    @api.model
    def _square_online_pos_scope(self, environment, merchant_id=None, location_id=None):
        """Historical website operations that could own a POS event."""
        charges = (
            Domain("operation", "=", "online_direct")
            & Domain("square_online_dispatch_at", "!=", False)
            & (Domain("state", "=", "pending") | Domain("provider_reference", "!=", False))
        )
        refunds = Domain("operation", "=", "refund") & Domain("provider_reference", "!=", False)
        scope = (
            Domain("provider_code", "=", "square_online")
            & Domain("square_online_environment", "=", environment)
            & (charges | refunds)
        )
        if merchant_id:
            scope &= Domain("square_online_merchant_id", "=", merchant_id)
        if location_id:
            scope &= Domain("square_online_location_id", "=", location_id)
        return scope

    @api.model
    def _square_online_pos_knows(self, scope, *, order_id=None, payment_id=None, refund_id=None):
        identifiers = Domain.FALSE
        if order_id:
            identifiers |= Domain("square_online_order_reference", "=", order_id)
        if payment_id:
            identifiers |= Domain("operation", "=", "online_direct") & Domain(
                "provider_reference", "=", payment_id,
            )
        if refund_id:
            identifiers |= Domain("operation", "=", "refund") & Domain(
                "provider_reference", "=", refund_id,
            )
        return bool(self.search_count(scope & identifiers, limit=1))

    @api.model
    def _square_online_pos_has_unknown_charge(self, scope, *, order=None):
        scope &= (
            Domain("operation", "=", "online_direct")
            & Domain("state", "=", "pending")
            & Domain("provider_reference", "=", False)
        )
        if order is not None:
            # Website checkout lets CreatePayment create its own order. It cannot
            # pay a draft or an order that already existed before dispatch.
            if order.get("state") == "DRAFT":
                return False
            total = order.get("total_money")
            if (
                isinstance(total, dict) and type(total.get("amount")) is int
                and total["amount"] > 0 and isinstance(total.get("currency"), str)
            ):
                scope &= Domain("square_online_amount_minor", "=", total["amount"])
                scope &= Domain("square_online_currency_code", "=", total["currency"])
            created_at = order.get("created_at")
            if isinstance(created_at, str):
                try:
                    created = datetime.fromisoformat(created_at)
                except ValueError:
                    pass
                else:
                    if created.tzinfo:
                        # Allow for a difference between the Odoo and Square clocks.
                        latest_dispatch = created.astimezone(UTC).replace(tzinfo=None) + timedelta(minutes=5)
                        scope &= Domain("square_online_dispatch_at", "<=", latest_dispatch)
        return bool(self.search_count(scope, limit=1))

    @api.model
    def _square_online_pos_owns_payment(self, scope, payment):
        """Attribute a Payment retrieved with POS credentials, without applying its result."""
        reference = payment.get("reference_id")
        location = payment.get("location_id")
        if not isinstance(reference, str) or not reference or not isinstance(location, str) or not location:
            return False
        transaction = self.search(
            scope & Domain("operation", "=", "online_direct")
            & Domain("square_online_operation_key", "=", reference)
            & Domain("square_online_location_id", "=", location),
            limit=1,
        )
        if not transaction:
            return False
        connection = transaction.square_online_connection_id
        application = payment.get("application_details") or {}
        return (
            connection.company_id == transaction.company_id
            and connection.environment == transaction.square_online_environment
            and connection.merchant_id == transaction.square_online_merchant_id
            and isinstance(application, dict)
            and application.get("application_id") in (None, connection.application_id)
            and transaction.provider_reference in (False, payment.get("id"))
        )

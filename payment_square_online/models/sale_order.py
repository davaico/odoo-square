# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import hashlib
import json

from odoo import _, models
from odoo.exceptions import ValidationError
from odoo.tools import SQL, consteq

from odoo.addons.payment import utils as payment_utils

from ..square_api import square_amount_in_minor_units


class SaleOrder(models.Model):
    _inherit = "sale.order"

    def _square_online_billing_contact(self):
        self.ensure_one()
        partner = self.partner_invoice_id
        first_name, _, last_name = (partner.name or "").partition(" ")
        return {
            key: value for key, value in {
                "givenName": first_name,
                "familyName": last_name,
                "email": partner.email,
                "phone": partner.phone,
                "addressLines": [line for line in (partner.street, partner.street2) if line],
                "city": partner.city,
                "state": partner.state_id.code,
                "postalCode": partner.zip,
                "countryCode": partner.country_id.code,
            }.items() if value
        }

    def _square_online_cart_snapshot(self):
        self.ensure_one()
        lines = [
            [line.id, line.product_id.id, line.product_uom_qty, line.product_uom_id.id,
             line.price_unit, line.discount, line.tax_ids.ids, line.price_subtotal, line.price_total,
             line.name, line.is_delivery]
            for line in self.order_line.sorted("id")
        ]
        shipping = self.partner_shipping_id
        data = [
            self.id, self.company_id.id, self.website_id.id, self.partner_id.id,
            self.partner_invoice_id.id, self.currency_id.id, self.amount_total, self.carrier_id.id,
            self._square_online_billing_contact(),
            [shipping.id, shipping.street, shipping.street2, shipping.city, shipping.zip,
             shipping.state_id.id, shipping.country_id.id],
            lines,
        ]
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def _square_online_cart_token(self):
        self.ensure_one()
        return payment_utils.generate_access_token(
            "square-online-cart", self._square_online_cart_snapshot(), env=self.env,
        )

    def _square_online_prepare_website_checkout(self, transaction, website_id, cart_id, user):
        self.ensure_one()
        if self._square_online_block_conflicting_checkout(transaction):
            raise ValidationError(_("A payment for this order is already being processed."))
        if (
            cart_id != self.id
            or (not user._is_public() and user.partner_id.commercial_partner_id != self.partner_id.commercial_partner_id)
            or not consteq(self._square_online_cart_token(), self.env.context.get("square_online_cart", ""))
        ):
            raise ValidationError(_("Your checkout changed. Refresh the checkout page."))
        try:
            requested_minor = square_amount_in_minor_units(
                self.env.context.get("square_online_cart_amount"), self.currency_id.name,
            )
            order_minor = square_amount_in_minor_units(self.amount_total, self.currency_id.name)
        except ValueError:
            raise ValidationError(_("The cart total is invalid. Refresh the checkout page.")) from None
        if requested_minor != order_minor:
            raise ValidationError(_("The cart total changed. Refresh the checkout page."))
        transaction._square_online_prepare_checkout(self, website_id)
        # The native website transaction route already holds the order lock.
        self._square_online_mark_checkout_change()

    def _square_online_guard_other_checkout(self, transaction):
        for order in self.sorted("id"):
            order._square_online_lock_checkout()
            if order._square_online_block_conflicting_checkout(transaction):
                raise ValidationError(_("A payment for this order is already being processed."))
            order._square_online_mark_checkout_change()

    def _square_online_lock_checkout(self):
        self.ensure_one()
        self.env.cr.execute(SQL("SELECT id FROM sale_order WHERE id = %s FOR NO KEY UPDATE", self.id))

    def _square_online_mark_checkout_change(self):
        self.ensure_one()
        # Repeatable-read requests must retry if their snapshot predates another checkout.
        self.env.cr.execute(SQL("UPDATE sale_order SET write_date = write_date WHERE id = %s", self.id))

    def _square_online_block_conflicting_checkout(self, transaction):
        self.ensure_one()
        others = self.transaction_ids - transaction
        if transaction.provider_code == "square_online":
            competing = others.filtered(
                lambda other: other.state in ("pending", "authorized", "done")
                or (other.state == "draft" and other.provider_code != "square_online")
            )
        else:
            competing = others.filtered(
                lambda other: other.provider_code == "square_online"
                and other.state in ("pending", "done")
            )
        return bool(competing)

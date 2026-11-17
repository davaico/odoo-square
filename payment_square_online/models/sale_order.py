# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import hashlib
import json

from odoo import models
from odoo.tools import SQL

from odoo.addons.payment import utils as payment_utils


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

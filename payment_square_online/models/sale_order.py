import json
from hashlib import sha256

from odoo import _, fields, models
from odoo.exceptions import UserError


class SaleOrder(models.Model):
    _inherit = 'sale.order'

    square_online_transaction_id = fields.Many2one(
        'payment.transaction', readonly=True, copy=False, ondelete='restrict',
    )

    def _square_lock_checkout(self):
        self.ensure_one()
        self.env.cr.execute('SELECT id FROM sale_order WHERE id = %s FOR NO KEY UPDATE', [self.id])

    def _square_lock_checkout_for_change(self):
        self.ensure_one()
        # Changing the row makes an overlapping checkout re-read its snapshot under REPEATABLE READ.
        self.env.cr.execute("""
            UPDATE sale_order SET square_online_transaction_id = square_online_transaction_id
            WHERE id = %s
        """, [self.id])
        self.invalidate_recordset(['square_online_transaction_id'])
        if self._square_checkout_is_reserved():
            raise UserError(_("Payment is already being processed for this order."))

    def _square_checkout_is_reserved(self):
        self.ensure_one()
        return self.square_online_transaction_id.state in ('pending', 'done')

    def _square_cart_signature(self):
        self.ensure_one()
        addresses = self.partner_invoice_id | self.partner_shipping_id
        values = (
            self.id, self.company_id.id, self.website_id.id, self.partner_id.id,
            self.partner_invoice_id.id, self.partner_shipping_id.id, self.carrier_id.id,
            self.fiscal_position_id.id, self.pricelist_id.id, self.currency_id.id, self.amount_total,
            [(p.id, p.name, p.street, p.street2, p.city, p.zip, p.state_id.id, p.country_id.id,
              p.email, p.phone) for p in addresses],
            [(line.id, line.product_id.id, line.product_uom.id, line.product_uom_qty, line.price_unit,
              line.discount, line.tax_id.ids, line.price_total, line.name, line.write_date)
             for line in self.order_line],
        )
        return sha256(json.dumps(values, default=str).encode()).hexdigest()

    def get_portal_last_transaction(self):
        if self._square_checkout_is_reserved():
            return self.square_online_transaction_id
        return super().get_portal_last_transaction()

    def _cart_update(self, *args, **kwargs):
        self._square_lock_checkout_for_change()
        return super()._cart_update(*args, **kwargs)

    def _update_address(self, *args, **kwargs):
        self._square_lock_checkout_for_change()
        return super()._update_address(*args, **kwargs)

    def _cart_update_pricelist(self, *args, **kwargs):
        self._square_lock_checkout_for_change()
        return super()._cart_update_pricelist(*args, **kwargs)

    def _set_delivery_method(self, *args, **kwargs):
        self._square_lock_checkout_for_change()
        return super()._set_delivery_method(*args, **kwargs)

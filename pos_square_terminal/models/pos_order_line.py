from odoo import _, api, models
from odoo.exceptions import UserError

from .pos_order import same_value


class PosOrderLine(models.Model):
    _inherit = 'pos.order.line'

    _square_frozen_fields = {
        'order_id', 'uuid', 'product_id', 'qty', 'price_unit', 'price_subtotal', 'price_subtotal_incl',
        'discount', 'tax_ids', 'price_extra', 'price_type', 'refunded_orderline_id', 'combo_parent_id',
        'combo_item_id', 'combo_line_ids', 'attribute_value_ids', 'custom_attribute_value_ids', 'pack_lot_ids',
    }

    @api.model_create_multi
    def create(self, vals_list):
        orders = self.env['pos.order'].browse(list({
            vals.get('order_id', self.env.context.get('default_order_id')) for vals in vals_list
        } - {None, False}))
        orders._square_lock_checkout(touch=True)
        if orders._square_attempts():
            raise UserError(_("The original goods are reserved by a Square payment."))
        return super().create(vals_list)

    def write(self, vals):
        changes = self._square_frozen_fields.intersection(vals)
        if changes:
            self.check_access('write')
            orders = self.order_id | self.env['pos.order'].browse(vals.get('order_id') or [])
            orders._square_lock_checkout(touch=True)
            reserved = set(orders._square_attempts().order_id.ids)
            for line in self:
                if (
                    reserved.intersection({line.order_id.id, vals.get('order_id')})
                    and any(not same_value(line, field, vals[field]) for field in changes)
                ):
                    raise UserError(_("The original goods are reserved by a Square payment."))
        return super().write(vals)

    def unlink(self):
        self.check_access('unlink')
        orders = self.order_id
        orders._square_lock_checkout(touch=True)
        if orders._square_attempts():
            raise UserError(_("The original goods are reserved by a Square payment."))
        return super().unlink()

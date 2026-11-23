from uuid import uuid4

from odoo import _, api, models
from odoo.exceptions import UserError

from .pos_order import checkout_create_parents, same_value


class PosOrderLine(models.Model):
    _inherit = 'pos.order.line'

    _square_frozen_fields = {
        'order_id', 'uuid', 'product_id', 'qty', 'price_unit', 'price_subtotal', 'price_subtotal_incl',
        'discount', 'tax_ids', 'price_extra', 'price_type', 'refunded_orderline_id', 'combo_parent_id',
        'combo_item_id', 'combo_line_ids', 'attribute_value_ids', 'custom_attribute_value_ids', 'pack_lot_ids',
    }

    def _square_configured_attribute_values(self):
        self.ensure_one()
        selected = self.attribute_value_ids
        custom = self.custom_attribute_value_ids
        product = self.product_id
        if selected or custom or product.valid_product_template_attribute_line_ids:
            combination = selected | product.product_template_attribute_value_ids
            custom_options = selected.filtered('is_custom')
            if (
                not product.product_tmpl_id._is_combination_possible_by_config(combination)
                or custom.custom_product_template_attribute_value_id != custom_options
                or len(custom) != len(custom_options)
            ):
                raise UserError(_("The Odoo basket options no longer match the product configuration."))
        return selected

    @api.model_create_multi
    def create(self, vals_list):
        vals_list, orders = checkout_create_parents(self, vals_list, 'order_id')
        orders._square_lock_checkout(touch=True)
        if orders._square_reserved_attempts():
            raise UserError(_("The original goods are reserved by a Square payment."))
        return super().create(vals_list)

    def _square_create_device_tip(self, order, product, amount):
        return super(PosOrderLine, self.sudo()).create({
            'order_id': order.id,
            'product_id': product.id,
            'qty': 1,
            'price_unit': amount,
            'price_subtotal': amount,
            'price_subtotal_incl': amount,
            'price_type': 'manual',
            'full_product_name': product.display_name,
            'skip_change': True,
            'uuid': str(uuid4()),
        })

    def write(self, vals):
        changes = self._square_frozen_fields.intersection(vals)
        if changes:
            self.check_access('write')
            orders = self.order_id | self.env['pos.order'].browse(vals.get('order_id') or [])
            orders._square_lock_checkout(touch=True)
            reserved = set(orders._square_reserved_attempts().order_id.ids)
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
        if orders._square_reserved_attempts():
            raise UserError(_("The original goods are reserved by a Square payment."))
        return super().unlink()


class PosOrderLineLot(models.Model):
    _inherit = 'pos.pack.operation.lot'

    @api.model_create_multi
    def create(self, vals_list):
        vals_list, lines = checkout_create_parents(self, vals_list, 'pos_order_line_id')
        orders = lines.order_id
        orders._square_lock_checkout(touch=True)
        if orders._square_reserved_attempts():
            raise UserError(_("The lot and serial numbers are reserved by the original Square payment."))
        return super().create(vals_list)

    def write(self, vals):
        fixed_fields = {'lot_name', 'pos_order_line_id'}.intersection(vals)
        if fixed_fields:
            self.check_access('write')
            destination = self.env['pos.order.line'].browse(vals.get('pos_order_line_id') or []).order_id
            orders = self.order_id | destination
            orders._square_lock_checkout(touch=True)
            reserved_orders = orders._square_reserved_attempts().order_id
            for lot in self:
                if (lot.order_id | destination) & reserved_orders and any(
                    not same_value(lot, field, vals[field]) for field in fixed_fields
                ):
                    raise UserError(_("The lot and serial numbers are reserved by the original Square payment."))
        return super().write(vals)

    def unlink(self):
        self.check_access('unlink')
        orders = self.order_id
        orders._square_lock_checkout(touch=True)
        if orders._square_reserved_attempts():
            raise UserError(_("The lot and serial numbers are reserved by the original Square payment."))
        return super().unlink()

# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from uuid import uuid4

from odoo import _, api, models
from odoo.exceptions import UserError

from .pos_order import same_value


class PosOrderLine(models.Model):
    _inherit = 'pos.order.line'

    _square_frozen_fields = {
        'order_id', 'uuid', 'product_id', 'qty', 'price_unit', 'price_subtotal', 'price_subtotal_incl',
        'discount', 'tax_ids', 'price_extra', 'price_type', 'refunded_orderline_id', 'combo_parent_id',
        'combo_item_id', 'combo_line_ids', 'attribute_value_ids', 'custom_attribute_value_ids', 'pack_lot_ids',
        'extra_tax_data',
    }

    @api.model_create_multi
    def create(self, vals_list):
        orders = self.env['pos.order'].browse(list({
            vals.get('order_id', self.env.context.get('default_order_id')) for vals in vals_list
        } - {None, False}))
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
            'uuid': str(uuid4()),
        })

    def write(self, vals):
        changes = self._square_frozen_fields.intersection(vals)
        if changes:
            self.check_access('write')
            orders = self.order_id | self.env['pos.order'].browse(vals.get('order_id') or [])
            orders._square_lock_checkout(touch=True)
            reserved = set(orders._square_reserved_attempts().order_id.ids)
            protected = self.filtered(lambda line: reserved.intersection({line.order_id.id, vals.get('order_id')}))
            if any(not same_value(line, field, vals[field]) for line in protected for field in changes):
                raise UserError(_("The original goods are reserved by a Square payment."))
            if protected:
                # Keep persisted values when native POS sync replays an equivalent number or relation.
                other_values = {field: value for field, value in vals.items() if field not in changes}
                if other_values:
                    super(PosOrderLine, protected).write(other_values)
                unprotected = self - protected
                return super(PosOrderLine, unprotected).write(vals) if unprotected else True
        return super().write(vals)

    def unlink(self):
        self.check_access('unlink')
        orders = self.order_id
        orders._square_lock_checkout(touch=True)
        if orders._square_reserved_attempts():
            raise UserError(_("The original goods are reserved by a Square payment."))
        return super().unlink()


class PosSquareOrderLineDetail(models.AbstractModel):
    _name = 'pos.square.order.line.detail'
    _description = 'Protection for Square POS order line details'

    @api.model_create_multi
    def create(self, vals_list):
        default_line = self.env.context.get('default_pos_order_line_id')
        line_ids = {vals.get('pos_order_line_id', default_line) for vals in vals_list} - {None, False}
        orders = self.env['pos.order.line'].browse(list(line_ids)).order_id
        orders._square_lock_checkout(touch=True)
        if orders._square_reserved_attempts():
            raise UserError(_("The original goods are reserved by a Square payment."))
        return super().create(vals_list)

    def write(self, vals):
        changes = self._square_detail_fields.intersection(vals)
        if changes:
            self.check_access('write')
            destination = self.env['pos.order.line'].browse(vals.get('pos_order_line_id') or []).order_id
            orders = self.pos_order_line_id.order_id | destination
            orders._square_lock_checkout(touch=True)
            reserved = orders._square_reserved_attempts().order_id
            if any(
                (detail.pos_order_line_id.order_id | destination) & reserved
                and any(not same_value(detail, field, vals[field]) for field in changes)
                for detail in self
            ):
                raise UserError(_("The original goods are reserved by a Square payment."))
        return super().write(vals)

    def unlink(self):
        self.check_access('unlink')
        orders = self.pos_order_line_id.order_id
        orders._square_lock_checkout(touch=True)
        if orders._square_reserved_attempts():
            raise UserError(_("The original goods are reserved by a Square payment."))
        return super().unlink()


class PosPackOperationLot(models.Model):
    _name = 'pos.pack.operation.lot'
    _inherit = ['pos.pack.operation.lot', 'pos.square.order.line.detail']

    _square_detail_fields = {'pos_order_line_id', 'lot_name', 'product_id', 'order_id'}

    def copy(self, default=None):
        self.ensure_one()
        if self.pos_order_line_id.order_id._square_reserved_attempts():
            # Native POS first copies the lot, then attaches the copy to the return line.
            default = {'pos_order_line_id': False, **(default or {})}
        return super().copy(default)


class ProductAttributeCustomValue(models.Model):
    _name = 'product.attribute.custom.value'
    _inherit = ['product.attribute.custom.value', 'pos.square.order.line.detail']

    _square_detail_fields = {
        'pos_order_line_id', 'custom_product_template_attribute_value_id', 'custom_value',
    }

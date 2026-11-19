# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import api, models


_POS_STOCK_LINES = object()


class StockPicking(models.Model):
    _inherit = 'stock.picking'

    @api.model
    def _create_picking_from_pos_order_lines(self, location_dest_id, lines, picking_type, partner=False):
        # The stock move completes before native POS links the picking to its order.
        # A picking created at session close can also represent several POS orders.
        picking = self.with_context(square_terminal_stock_lines=(_POS_STOCK_LINES, tuple(lines.ids)))
        result = super(StockPicking, picking)._create_picking_from_pos_order_lines(
            location_dest_id, lines, picking_type, partner,
        )
        return result.with_env(self.env)


class StockMove(models.Model):
    _inherit = 'stock.move'

    def _square_terminal_inventory_orders(self, environment):
        self.ensure_one()
        if self.location_dest_id.usage != 'customer':
            return {}
        orders = self.picking_id.pos_order_id.sudo()
        stock_lines = self.env.context.get('square_terminal_stock_lines')
        if isinstance(stock_lines, tuple) and len(stock_lines) == 2 and stock_lines[0] is _POS_STOCK_LINES:
            lines = self.env['pos.order.line'].sudo().browse(stock_lines[1])
            orders |= lines.filtered(lambda line: line.product_id == self.product_id and line.qty > 0).order_id
        if not orders:
            return {}
        attempts = orders._square_attempts().filtered_domain([
            ('state', '=', 'verified'), ('company_id', '=', self.company_id.id), ('environment', '=', environment),
        ])
        by_location = {}
        for link in attempts.square_order_link_id:
            variations = sorted({
                line['catalog_object_id'] for line in link.create_request['order']['line_items']
                if line.get('catalog_object_id')
            })
            if link.square_order_id and variations:
                by_location.setdefault(link.square_location_id, {})[link.square_order_id] = {
                    'merchant_id': link.merchant_id,
                    'environment': link.environment,
                    'reference_id': link.reference,
                    'variations': variations,
                }
        return by_location

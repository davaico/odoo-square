from odoo import models
from odoo.exceptions import AccessError

from .stock_move import _SQUARE_MANAGED_PICKING


class StockPicking(models.Model):
    _inherit = "stock.picking"

    def _force_quantity_for_square(self):
        moves = self.move_ids.filtered(lambda move: move.state not in ("done", "cancel"))
        for move in moves:
            # Let Odoo distribute the demand across the move's reserved lines.
            move.quantity = move.product_uom_qty
            move.picked = True

    def _validate_square_picking(self, sale_order):
        """Finish fulfillment for a verified Square sale or its return."""
        sale_order.ensure_one()
        if not sale_order.square_order_id or any(
            picking.company_id != sale_order.company_id
            or sale_order not in (picking.sale_id, picking.return_id.sale_id)
            for picking in self
        ):
            raise AccessError("The picking is not linked to this Square sale.")

        pickings = self.with_context(square_managed_picking=_SQUARE_MANAGED_PICKING)
        pickings.filtered(
            lambda picking: picking.picking_type_code in ("outgoing", "incoming")
        )._force_quantity_for_square()
        return pickings.button_validate()

from odoo import fields, models

from .stock_move import _SQUARE_MANAGED_PICKING


class StockPicking(models.Model):
    _inherit = "stock.picking"

    square_exchange_order_id = fields.Char(
        string="Square Exchange Order ID", copy=False, readonly=True,
        groups="base.group_system,odoo_square.group_square_pos_service",
    )

    def _validate_square_picking(self):
        """Complete a picking prepared by the Square order or refund workflow."""
        pickings = self.with_context(square_managed_picking=_SQUARE_MANAGED_PICKING)
        moves = pickings.filtered(lambda p: p.picking_type_code in ("outgoing", "incoming")).move_ids
        for move in moves.filtered(lambda m: m.state not in ("done", "cancel")):
            move.quantity = move.product_uom_qty
        return pickings.button_validate()

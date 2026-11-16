from odoo import models


class StockPicking(models.Model):
    _inherit = "stock.picking"

    def _force_quantity_for_square(self):
        moves = self.move_ids.filtered(lambda move: move.state not in ("done", "cancel"))
        for move in moves:
            # Let Odoo distribute the demand across the move's reserved lines.
            move.quantity = move.product_uom_qty
            move.picked = True

    def button_validate(self):
        """
        Override button_validate to allow validation even with insufficient stock.
        For Square orders, we set the completed quantity to the demand, allowing
        negative stock if necessary.
        """
        if self.env.context.get("force_validate"):
            self.filtered(
                lambda picking: picking.picking_type_code in ("outgoing", "incoming")
            )._force_quantity_for_square()

        return super().button_validate()

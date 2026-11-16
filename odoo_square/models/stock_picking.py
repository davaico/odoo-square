from odoo import fields, models


class StockPicking(models.Model):
    _inherit = "stock.picking"

    square_exchange_order_id = fields.Char(
        string="Square Exchange Order ID", copy=False, readonly=True
    )

    def _force_quantity_for_square(self):
        for move in self.move_ids.filtered(lambda move: move.state not in ("done", "cancel")):
            move.quantity = move.product_uom_qty

    def button_validate(self):
        """
        Override button_validate to allow validation even with insufficient stock.
        For Square orders, we force validation by setting done quantities equal to demanded quantities,
        allowing negative stock if necessary.
        """
        force_validate = self.env.context.get("force_validate", False)

        if force_validate:
            self.filtered(
                lambda picking: picking.picking_type_code in ("outgoing", "incoming")
            )._force_quantity_for_square()

        return super().button_validate()

from odoo import models, api


class StockPicking(models.Model):
    _inherit = "stock.picking"

    @api.model
    def _force_quantity_for_square(self):
        for move in self.move_ids:
            if move.state not in ["done", "cancel"]:
                required_qty = move.product_uom_qty

                # set all move lines to required qty
                if move.move_line_ids:
                    move.move_line_ids.write({"quantity": required_qty})
                else:
                    self.env["stock.move.line"].create(
                        {
                            "move_id": move.id,
                            "product_id": move.product_id.id,
                            "product_uom_id": move.product_uom.id,
                            "location_id": move.location_id.id,
                            "location_dest_id": move.location_dest_id.id,
                            "quantity": required_qty,
                            "picking_id": self.id,
                        }
                    )

    def button_validate(self):
        """
        Override button_validate to allow validation even with insufficient stock.
        For Square orders, we force validation by setting done quantities equal to demanded quantities,
        allowing negative stock if necessary.
        """
        force_validate = self.env.context.get("force_validate", False)

        if force_validate and self.picking_type_code == "outgoing":
            self._force_quantity_for_square()

        return super().button_validate()

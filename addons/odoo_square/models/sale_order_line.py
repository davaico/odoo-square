# -*- coding: utf-8 -*-
from odoo import models, fields, api
import logging

_logger = logging.getLogger(__name__)


class SaleOrderLine(models.Model):
    _inherit = "sale.order.line"

    square_line_id = fields.Char(
        string="Square Line ID",
        help="The unique identifier from Square for this order line",
        index=True,
        copy=False,
    )

    square_catalog_id = fields.Char(
        string="Square Catalog ID",
        help="The Square catalog object ID for this product (used for exchange tracking)",
        index=True,
        copy=False,
    )

    # Return tracking fields
    returned_qty = fields.Float(
        string="Returned Quantity",
        digits="Product Unit of Measure",
        default=0.0,
        help="Quantity of this product that has been returned via Square refunds",
        copy=False,
    )

    effective_qty = fields.Float(
        string="Effective Quantity",
        compute="_compute_effective_qty",
        digits="Product Unit of Measure",
        help="Net quantity after returns: Original Qty - Returned Qty",
        store=True,
    )

    @api.depends("product_uom_qty", "discount", "price_unit", "tax_id")
    def _compute_amount(self):
        """
        Override to prevent tax recalculation for Square orders
        when exact amounts are already set
        """
        # Check if this is a Square order and we're in tax override mode
        if self.env.context.get("skip_tax_calculation"):
            # Skip the normal computation for Square orders with exact amounts
            return

        # For non-Square orders or when not overriding, use normal computation
        return super()._compute_amount()

    def write(self, vals):
        """Override write to handle Square tax amount setting"""
        # If we're setting exact amounts from Square, skip tax recalculation
        if self.env.context.get("skip_tax_calculation"):
            # Temporarily disable tax computation
            return super(
                SaleOrderLine, self.with_context(disable_tax_computation=True)
            ).write(vals)

        return super().write(vals)

    @api.depends("product_uom_qty", "returned_qty")
    def _compute_effective_qty(self):
        """Compute the effective quantity after returns"""
        for line in self:
            line.effective_qty = line.product_uom_qty - line.returned_qty

    def update_returned_quantity(self, returned_qty):
        """
        Update the returned quantity for this order line
        Called during refund processing to track returned items
        """
        self.ensure_one()

        if returned_qty < 0:
            raise ValueError("Returned quantity cannot be negative")

        if returned_qty > self.product_uom_qty - self.returned_qty:
            _logger.warning(
                f"Attempting to return {returned_qty} units of {self.product_id.name} "
                f"but only {self.product_uom_qty - self.returned_qty} units are available to return"
            )
            # Cap at available quantity
            returned_qty = self.product_uom_qty - self.returned_qty

        old_returned_qty = self.returned_qty
        self.returned_qty += returned_qty

        _logger.info(
            f"Updated returned quantity for {self.product_id.name}: "
            f"{old_returned_qty} -> {self.returned_qty} "
            f"(added {returned_qty})"
        )

        return returned_qty

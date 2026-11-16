# -*- coding: utf-8 -*-
import logging
from odoo import models

_logger = logging.getLogger(__name__)


class StockQuant(models.Model):
    _inherit = "stock.quant"

    def write(self, vals):
        """Override write to trigger Square sync when quantity changes"""
        if "quantity" not in vals or self.env.context.get("square_move_in_progress"):
            return super().write(vals)

        # Store old quantities before update
        old_quantities = {quant.id: quant.quantity for quant in self}
        result = super().write(vals)

        # Check if quantity changed and trigger sync
        for quant in self:
            if old_quantities[quant.id] != quant.quantity:
                self._trigger_square_sync_for_quant(quant)

        return result

    def _trigger_square_sync_for_quant(self, quant):
        """Trigger Square stock synchronization for a quantity change"""
        try:
            # Get warehouse for this quant's location
            warehouse = quant.location_id.warehouse_id
            if not warehouse:
                _logger.debug(
                    f"No warehouse found for location {quant.location_id.name}"
                )
                return

            # Get Square configuration
            square_config = self.env["square.config"].search([], limit=1)
            if not square_config:
                _logger.debug("No Square configuration found")
                return

            # Find location mapping for this warehouse
            location_mapping = square_config.location_mapping_ids.filtered(
                lambda m: m.warehouse_id == warehouse
            )

            if not location_mapping:
                _logger.debug(
                    f"No Square location mapping for warehouse {warehouse.name}"
                )
                return

            # Check if product should be synced (has SKU)
            if not quant.product_id.default_code:
                _logger.debug(f"Skipping product without SKU: {quant.product_id.name}")
                return

            # Get the Square stock sync service
            stock_sync = self.env["square.stock.sync"].search([], limit=1)
            if not stock_sync:
                # Create default sync service if it doesn't exist
                stock_sync = self.env["square.stock.sync"].create(
                    {"name": "Square Stock Sync"}
                )

            # Get current stock quantity for this product and warehouse
            current_stock = stock_sync._get_warehouse_stock(quant.product_id, warehouse)

            # Sync to Square for the specific location
            stock_sync.sync_product_stock_for_location(
                quant.product_id, location_mapping.square_location_id, current_stock
            )

        except Exception as e:
            _logger.error(
                f"Error triggering Square sync for quant {quant.id}: {str(e)}"
            )

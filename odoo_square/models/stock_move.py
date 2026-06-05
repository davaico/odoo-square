# -*- coding: utf-8 -*-
import logging
from odoo import models, api

_logger = logging.getLogger(__name__)


class StockMove(models.Model):
    _inherit = "stock.move"

    @api.model_create_multi
    def create(self, vals_list):
        """Override create to potentially trigger Square sync"""
        moves = super().create(vals_list)

        # Ensure moves is always a recordset
        if not isinstance(moves, models.BaseModel):
            moves = self.browse(moves) if isinstance(moves, (int, list)) else moves

        # Trigger sync for completed moves
        for move in moves:
            if move.exists() and move.state == "done":
                self._trigger_square_sync(move)

        return moves

    def write(self, vals):
        """Override write to trigger Square sync when move becomes 'done'"""
        result = super().write(vals)

        # Check if state changed to 'done'
        if "state" in vals and vals["state"] == "done":
            for move in self:
                self._trigger_square_sync(move)

        return result

    def _trigger_square_sync(self, move):
        """Trigger Square stock synchronization for a completed move"""
        try:
            # Get the Square stock sync service
            stock_sync = self.env["square.stock.sync"].search([], limit=1)
            if not stock_sync:
                # Create default sync service if it doesn't exist
                stock_sync = self.env["square.stock.sync"].create(
                    {"name": "Square Stock Sync"}
                )

            # Trigger the sync
            stock_sync.sync_stock_changes(move)

        except Exception as e:
            _logger.error(
                f"Error triggering Square sync for move {move.name}: {str(e)}"
            )

    def _action_done(self, cancel_backorder=False):
        """Override _action_done to ensure sync is triggered"""
        result = super()._action_done(cancel_backorder=cancel_backorder)

        # Trigger sync for all completed moves
        for move in self:
            if move.state == "done":
                self._trigger_square_sync(move)

        return result

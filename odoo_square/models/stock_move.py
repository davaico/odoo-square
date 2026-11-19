# -*- coding: utf-8 -*-
import logging
from odoo import models, api
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY

_logger = logging.getLogger(__name__)
_STOCK_MOVE_IN_PROGRESS = object()
_SQUARE_MANAGED_PICKING = object()


class StockMove(models.Model):
    _inherit = "stock.move"

    def _get_square_returnable_quantity(self):
        """Return the uncommitted quantity in this delivery move's unit."""
        self.ensure_one()
        if self.state != "done" or self.location_dest_usage == "inventory":
            return 0.0

        committed = sum(
            returned.product_uom._compute_quantity(
                returned.quantity if returned.state == "done" else returned.product_uom_qty,
                self.product_uom,
                round=False,
            )
            for returned in self.returned_move_ids
            if returned.state != "cancel"
        )
        return max(self.product_uom.round(self.quantity - committed), 0.0)

    def _allocate_square_return_quantity(self, requested, requested_uom, return_uom):
        """Cap a return to available stock and keep the remainder in the request unit."""
        self.ensure_one()
        quantity = requested_uom._compute_quantity(requested, return_uom, round=False)
        available = self.product_uom._compute_quantity(
            self._get_square_returnable_quantity(), return_uom, round=False
        )
        quantity = min(return_uom.round(quantity), available)
        allocated = return_uom._compute_quantity(quantity, requested_uom, round=False)
        return quantity, requested - allocated

    @api.model_create_multi
    def create(self, vals_list):
        """Override create to potentially trigger Square sync"""
        moves = super().create(vals_list)
        if self.env.context.get("square_move_in_progress") is _STOCK_MOVE_IN_PROGRESS:
            return moves

        # Trigger sync for completed moves
        for move in moves:
            if move.exists() and move.state == "done":
                self._trigger_square_sync(move)

        return moves

    def write(self, vals):
        """Override write to trigger Square sync when move becomes 'done'"""
        result = super().write(vals)

        # Check if state changed to 'done'
        if vals.get("state") == "done" and self.env.context.get(
            "square_move_in_progress"
        ) is not _STOCK_MOVE_IN_PROGRESS:
            for move in self:
                self._trigger_square_sync(move)

        return result

    def _trigger_square_sync(self, move):
        """Trigger Square stock synchronization for a completed move"""
        try:
            move.check_access("read")
            if self.env.context.get("square_managed_picking") is _SQUARE_MANAGED_PICKING or not move.company_id:
                return
            sync, config = self.env["square.stock.sync"]._for_company(move.company_id)
            if config:
                sync._sync_stock_changes(move, config)

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error triggering Square sync for move {move.display_name}: {str(e)}"
            )

    def _action_done(self, cancel_backorder=False):
        """Synchronize after the movement has updated its quants."""
        # Core updates quants and writes the move state during validation.
        moves = self.with_context(square_move_in_progress=_STOCK_MOVE_IN_PROGRESS)
        result = super(StockMove, moves)._action_done(cancel_backorder=cancel_backorder)
        result = result.with_env(self.env)
        for move in result.filtered(lambda move: move.state == "done"):
            self._trigger_square_sync(move)

        return result

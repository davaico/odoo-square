# -*- coding: utf-8 -*-
import logging
from odoo import models, api
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY
from odoo.tools.float_utils import float_round

_logger = logging.getLogger(__name__)
_STOCK_MOVE_IN_PROGRESS = object()
_SQUARE_MANAGED_PICKING = object()


class StockMove(models.Model):
    _inherit = "stock.move"

    def _get_square_returnable_quantity(self, uom, exclude_returns=None):
        """Get delivered quantity not already returned or reserved for returns."""
        self.ensure_one()
        delivered = self.product_uom._compute_quantity(self.quantity, uom, round=False)
        returns = self.returned_move_ids
        if exclude_returns:
            returns -= exclude_returns
        reserved = sum(
            returned.product_uom._compute_quantity(
                returned.quantity if returned.state == "done" else returned.product_uom_qty,
                uom,
                round=False,
            )
            for returned in returns
            if returned.state != "cancel"
        )
        return float_round(
            max(0, delivered - reserved),
            precision_rounding=uom.rounding,
            rounding_method="DOWN",
        )

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
            if not move.company_id:
                return
            square_managed = self.env.context.get("square_managed_picking") is _SQUARE_MANAGED_PICKING
            if square_managed:
                return
            sync, config = self.env["square.stock.sync"]._for_company(move.company_id)
            if config:
                sync._sync_stock_changes(move.with_env(sync.env), config)

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error triggering Square sync for move {move.name}: {str(e)}"
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

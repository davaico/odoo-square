# -*- coding: utf-8 -*-
import logging
from odoo import models
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY

from .stock_move import _STOCK_MOVE_IN_PROGRESS

_logger = logging.getLogger(__name__)


class StockQuant(models.Model):
    _inherit = "stock.quant"

    def write(self, vals):
        """Override write to trigger Square sync when quantity changes"""
        if "quantity" not in vals or self.env.context.get("square_move_in_progress") is _STOCK_MOVE_IN_PROGRESS:
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
            quant.check_access("read")
            if not quant.company_id:
                return
            sync, config = self.env["square.stock.sync"]._for_company(quant.company_id)
            if config:
                sync._sync_quant_change(quant.with_env(sync.env), config)

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error triggering Square sync for quant {quant.id}: {str(e)}"
            )

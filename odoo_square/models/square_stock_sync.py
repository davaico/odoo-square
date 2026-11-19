# -*- coding: utf-8 -*-
import logging
from odoo import models, fields, api
from markupsafe import escape
from odoo.exceptions import AccessError, UserError
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY

_logger = logging.getLogger(__name__)


class SquareStockSync(models.Model):
    _name = "square.stock.sync"
    _description = "Square Stock Synchronization Service"
    _rec_name = "name"

    name = fields.Char(string="Sync Name", default="Square Stock Sync", readonly=True)

    # Sync Statistics
    last_sync_date = fields.Datetime(string="Last Sync Date", readonly=True)
    total_syncs = fields.Integer(string="Total Syncs", default=0, readonly=True)
    successful_syncs = fields.Integer(
        string="Successful Syncs", default=0, readonly=True
    )
    failed_syncs = fields.Integer(string="Failed Syncs", default=0, readonly=True)

    # Configuration
    auto_sync_enabled = fields.Boolean(
        string="Auto Sync Enabled",
        default=True,
        help="Automatically sync stock changes to Square",
    )

    _statistics_fields = frozenset({
        "last_sync_date", "total_syncs", "successful_syncs", "failed_syncs",
    })

    def _check_management_access(self):
        if not self.env.is_system():
            raise AccessError("Only Square administrators can manage stock synchronization.")
        config = self.env["square.config"].sudo().with_context(active_test=False).search([], limit=1)
        if config:
            config.with_env(self.env).check_access("read")
            if config.company_id != self.env.company:
                raise AccessError("Switch to the Square configuration's company to manage stock synchronization.")

    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.su:
            self._check_management_access()
            if any(self._statistics_fields.intersection(vals) for vals in vals_list):
                raise AccessError("Stock synchronization statistics are maintained by the integration.")
            # Discard RPC context defaults as well as explicit statistics values.
            self = self.with_context({
                key: value for key, value in self.env.context.items()
                if key not in {f"default_{name}" for name in self._statistics_fields}
            })
        return super().create(vals_list)

    def write(self, vals):
        if not self.env.su:
            self._check_management_access()
            if self._statistics_fields.intersection(vals):
                raise AccessError("Stock synchronization statistics are maintained by the integration.")
        return super().write(vals)

    def unlink(self):
        if not self.env.su:
            self._check_management_access()
        return super().unlink()

    @api.model
    def _for_company(self, company):
        """Select integration records using the stock record's company."""
        service = self.with_context({}, allowed_company_ids=company.ids).sudo()
        config = service.env["square.config"].search([("company_id", "=", company.id)], limit=1)
        if not config:
            return service, config

        sync = service.search([], limit=1) or service.create({})
        return sync, config

    def _should_sync_product(self, product, company):
        return (
            product.default_code
            and product.is_storable
            and (not product.company_id or product.company_id == company)
        )

    def _get_warehouse_stock(self, product, warehouse):
        """Count stock in the warehouse's stock location and its bins."""
        quants = self.env["stock.quant"].search([
            ("company_id", "=", warehouse.company_id.id),
            ("product_id", "=", product.id),
            ("location_id", "child_of", warehouse.lot_stock_id.id),
        ])
        return max(0, sum(quants.mapped("quantity")))

    def _get_location_mappings(self, config):
        return config._valid_location_mappings()

    def _active_merchant_locations(self):
        locations = self.env["square.api.client"].get_locations()
        return {
            location["id"]: location for location in locations
            if isinstance(location, dict)
            and location.get("status") == "ACTIVE"
            and isinstance(location.get("id"), str)
            and location["id"]
        }

    def _sync_stock_changes(self, move, config):
        """Synchronize affected warehouses after core completes a stock move."""
        terminal_orders = getattr(move, "_square_terminal_inventory_orders", None)
        move = move.with_env(self.env)
        if not self.auto_sync_enabled or move.state != "done":
            return
        related_sales = (move | move.origin_returned_move_id).sale_line_id.order_id
        if move.picking_id.square_exchange_order_id or any(related_sales.mapped("square_order_id")):
            return
        if not self._should_sync_product(move.product_id, config.company_id):
            return

        warehouses = (move.location_id | move.location_dest_id).warehouse_id
        mappings = self._get_location_mappings(config).filtered(
            lambda mapping: mapping.warehouse_id in warehouses
        )
        orders_by_location = terminal_orders(config.square_environment) if mappings and terminal_orders else None
        self._queue_mapped_product(move.product_id, mappings, orders_by_location=orders_by_location)

    def _sync_quant_change(self, quant, config):
        if not self._should_sync_product(quant.product_id, config.company_id):
            return
        mappings = self._get_location_mappings(config).filtered(
            lambda mapping: mapping.warehouse_id == quant.location_id.warehouse_id
        )
        self._queue_mapped_product(quant.product_id, mappings)

    def _queue_mapped_product(self, product, mappings, *, orders_by_location=None):
        counts = self.env["square.stock.count"]
        orders_by_location = orders_by_location or {}
        for mapping in mappings:
            quantity = self._get_warehouse_stock(product, mapping.warehouse_id)
            counts._queue_count(
                product, mapping, quantity, terminal_orders=orders_by_location.get(mapping.square_location_id),
            )

    def manual_sync_all_products(self):
        """Synchronize stock from the configured company and its mapped warehouses."""
        self.ensure_one()
        self._check_management_access()
        self.check_access("write")
        sync = self.with_context({}, allowed_company_ids=self.env.company.ids).sudo()
        return sync._manual_sync_all_products()

    def _manual_sync_all_products(self):
        config = self.env["square.api.client"]._get_square_config()
        mappings = self._get_location_mappings(config)
        merchant_location_ids = list(self._active_merchant_locations())
        if not merchant_location_ids:
            raise UserError("No active Square locations could be verified for this configuration.")

        if not mappings:
            if config.with_context(active_test=False).location_mapping_ids:
                raise UserError("No active Square warehouse mappings belong to this company.")
            # Preserve the legacy single-warehouse path when no mapping has been set up.
            warehouse = self.env["stock.warehouse"].search(
                [("company_id", "=", config.company_id.id)], limit=1,
            )
            if not warehouse:
                raise UserError("No warehouse belongs to the Square configuration's company.")
            return self._sync_products_for_warehouse(warehouse, merchant_location_ids[0])

        if set(mappings.mapped("square_location_id")) - set(merchant_location_ids):
            raise UserError("A Square warehouse mapping points to a location outside the merchant's active locations.")

        result = {"successful": 0, "failed": 0, "total": 0, "locations": []}
        for mapping in mappings:
            totals = self._sync_products_for_warehouse(mapping.warehouse_id, mapping.square_location_id)
            for key, total in totals.items():
                result[key] += total
            result["locations"].append({
                "warehouse": mapping.warehouse_id.name,
                "square_location": mapping.square_location_name,
                **totals,
            })

        self.env["square.integration.log"]._log_square_event(
            event_type="stock_sync",
            title="Manual Square stock synchronization",
            description=(
                f"{result['successful']} products synchronized successfully. "
                f"{result['failed']} failed."
            ),
            status="success" if not result["failed"] else "warning",
        )
        return result

    def _sync_products_for_warehouse(self, warehouse, square_location_id):
        quants = self.env["stock.quant"].search([
            ("company_id", "=", warehouse.company_id.id),
            ("location_id", "child_of", warehouse.lot_stock_id.id),
        ])
        products = quants.product_id.filtered(
            lambda product: self._should_sync_product(product, warehouse.company_id)
        )
        successful = sum(
            self._sync_product_to_location(product, warehouse, square_location_id)
            for product in products
        )
        return {"successful": successful, "failed": len(products) - successful, "total": len(products)}

    def _sync_product_to_location(self, product, warehouse, square_location_id):
        """Send a physical count derived from Odoo stock for a validated mapping."""
        api_client = self.env["square.api.client"]
        try:
            sku = product.default_code
            search = api_client.search_catalog_by_sku(sku)
            if search["not_found"]:
                return True

            success = search["success"] and api_client._set_physical_count(
                search["catalog_object_id"], square_location_id,
                self._get_warehouse_stock(product, warehouse), sku,
            )
        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception:
            _logger.exception("Square could not synchronize stock")
            success = False

        self._record_count_result(product, square_location_id, success)
        return success

    def _record_count_result(self, product, square_location_id, success):
        self.write({
            "total_syncs": self.total_syncs + 1,
            "successful_syncs": self.successful_syncs + int(success),
            "failed_syncs": self.failed_syncs + int(not success),
            "last_sync_date": fields.Datetime.now(),
        })
        self.env["square.integration.log"]._log_square_event(
            event_type="stock_sync",
            title=f"Stock synchronized for {product.name}" if success else "Square stock synchronization failed",
            description=escape(f"SKU: {product.default_code}, Square location: {square_location_id}"),
            status="success" if success else "error",
        )

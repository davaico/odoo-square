# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from datetime import datetime, timedelta, timezone
from time import monotonic
from uuid import uuid4

from psycopg2.extras import Json

from odoo import api, fields, models
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY


_logger = logging.getLogger(__name__)


class SquareStockCount(models.Model):
    _name = "square.stock.count"
    _description = "Pending Square Inventory Count"
    _log_access = False

    product_id = fields.Many2one("product.product", required=True, ondelete="cascade")
    mapping_id = fields.Many2one("square.location.mapping", required=True, ondelete="cascade")
    warehouse_id = fields.Many2one("stock.warehouse", required=True, ondelete="cascade")
    square_location_id = fields.Char(required=True)
    sku = fields.Char(required=True)
    quantity = fields.Float(required=True)
    counted_at = fields.Datetime(required=True)
    request_key = fields.Char(required=True)
    catalog_object_id = fields.Char()
    terminal_orders = fields.Json()
    next_attempt = fields.Datetime(index=True)

    _sql_constraints = [
        ("product_mapping_unique", "UNIQUE(product_id, mapping_id)", "A mapped product has one pending count."),
    ]

    @api.model
    def _queue_count(self, product, mapping, quantity, *, terminal_orders=None):
        # Updating one row per product/location also serializes concurrent bin changes.
        # PostgreSQL retries cannot publish counts from a rolled-back stock transaction.
        self.flush_model()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.env.cr.execute("""
            INSERT INTO square_stock_count
                (product_id, mapping_id, warehouse_id, square_location_id, sku, quantity,
                 counted_at, request_key, next_attempt, terminal_orders)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id, mapping_id) DO UPDATE SET
                warehouse_id = EXCLUDED.warehouse_id, square_location_id = EXCLUDED.square_location_id,
                sku = EXCLUDED.sku, quantity = EXCLUDED.quantity, counted_at = EXCLUDED.counted_at,
                request_key = EXCLUDED.request_key, catalog_object_id = NULL,
                next_attempt = EXCLUDED.next_attempt,
                terminal_orders = CASE
                    WHEN square_stock_count.warehouse_id = EXCLUDED.warehouse_id
                        AND square_stock_count.square_location_id = EXCLUDED.square_location_id
                        AND square_stock_count.sku = EXCLUDED.sku
                    THEN COALESCE(square_stock_count.terminal_orders, '{}'::jsonb) || EXCLUDED.terminal_orders
                    ELSE EXCLUDED.terminal_orders
                END
            RETURNING id
        """, (product.id, mapping.id, mapping.warehouse_id.id, mapping.square_location_id,
              product.default_code, quantity, now, str(uuid4()), fields.Datetime.now(), Json(terminal_orders or {})))
        self.browse(self.env.cr.fetchone()[0]).invalidate_recordset(flush=False)

    @api.model
    def _cron_send_counts(self):
        now = fields.Datetime.now()
        domain = [("next_attempt", "<=", now)]
        pending = self.search(domain, order="next_attempt, id", limit=20)
        deadline = monotonic() + 5
        locations_by_config = {}
        for job in pending:
            try:
                with self.env.cr.savepoint():
                    job._process_count(locations_by_config)
            except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                raise
            except Exception:
                _logger.exception("Could not send queued Square inventory count %s", job.id)
                job.next_attempt = now + timedelta(minutes=5)
            if monotonic() >= deadline:
                break
        if pending and self.search_count(domain, limit=1):
            self.env.ref("odoo_square.ir_cron_square_stock_counts")._trigger()

    def _process_count(self, locations_by_config):
        mapping = self.mapping_id
        config = mapping.config_id
        if (
            not config.active or not mapping.active
            or mapping.warehouse_id != self.warehouse_id
            or mapping.square_location_id != self.square_location_id
            or self.warehouse_id.company_id != config.company_id
            or self.product_id.default_code != self.sku
        ):
            self.unlink()
            return
        service, active_config = self.env["square.stock.sync"]._for_company(config.company_id)
        product = self.product_id.with_env(service.env)
        if active_config != config or not service._should_sync_product(product, config.company_id):
            self.unlink()
            return
        if self.counted_at < fields.Datetime.now() - timedelta(hours=24):
            # A stale request may already have reached Square. Take a new warehouse
            # measurement with its own timestamp and key before resolving the SKU.
            self._queue_count(product, mapping, service._get_warehouse_stock(product, self.warehouse_id))
        if config.id not in locations_by_config:
            locations_by_config[config.id] = service._active_merchant_locations(active_config)
        location = locations_by_config[config.id].get(self.square_location_id)
        if not location:
            self.next_attempt = fields.Datetime.now() + timedelta(minutes=5)
            return
        client = service.env["square.api.client"]
        if not self.catalog_object_id:
            match = client._search_catalog_by_sku(self.sku)
            if match["not_found"]:
                self.unlink()
            elif match["success"]:
                # Commit the variation before dispatch so even a lost response
                # or a worker restart replays exactly the same Square request.
                self.catalog_object_id = match["catalog_object_id"]
            else:
                service._record_count_result(product, self.square_location_id, False)
                self.next_attempt = False if match.get("ambiguous") else fields.Datetime.now() + timedelta(minutes=5)
            return
        if self.terminal_orders:
            # Commit the effective count timestamp before dispatch, just like the variation.
            self._order_count_after_terminal_sales(client, config, location)
            return
        success = client._set_physical_count(
            self.catalog_object_id, self.square_location_id, self.quantity, self.sku,
            request_id=self.request_key, occurred_at=self.counted_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        service._record_count_result(product, self.square_location_id, success)
        if success:
            self.unlink()
        else:
            self.next_attempt = fields.Datetime.now() + timedelta(minutes=5)

    def _order_count_after_terminal_sales(self, client, config, location):
        counted_at = self.counted_at
        for order_id, scope in self.terminal_orders.items():
            if self.catalog_object_id not in scope['variations'] or scope['environment'] != config.square_environment:
                continue
            if not location.get('merchant_id'):
                self.next_attempt = fields.Datetime.now() + timedelta(minutes=5)
                return
            if scope['merchant_id'] != location['merchant_id']:
                continue
            order = client._get_order(order_id, square_config=config)
            if (
                not order or order.get('state') != 'COMPLETED'
                or order.get('location_id') != self.square_location_id
                or order.get('reference_id') != scope['reference_id']
            ):
                self.next_attempt = fields.Datetime.now() + timedelta(minutes=5)
                return
            try:
                closed_at = datetime.fromisoformat(order.get('closed_at', '').replace('Z', '+00:00'))
                if closed_at.tzinfo is None:
                    raise ValueError('Square order completion must include a time zone')
            except (AttributeError, ValueError):
                self.next_attempt = fields.Datetime.now() + timedelta(minutes=5)
                return
            # A tracked Square variation is deducted when the order completes. Keep
            # the warehouse observation after that event even if the clocks differ.
            closed_at = closed_at.astimezone(timezone.utc).replace(tzinfo=None)
            counted_at = max(counted_at, closed_at + timedelta(milliseconds=1))

        if counted_at > datetime.now(timezone.utc).replace(tzinfo=None):
            # Square rejects future physical counts. Leave the observation queued.
            self.next_attempt = counted_at + timedelta(seconds=1)
            return
        self.write({'counted_at': counted_at, 'terminal_orders': {}})

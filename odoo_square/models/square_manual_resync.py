# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from html import escape
from datetime import datetime, timedelta, timezone
from dateutil import parser as dt_parser
from odoo import _, api, fields, models
from odoo.exceptions import UserError, ValidationError
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY

_logger = logging.getLogger(__name__)


def _square_ts_to_odoo_naive(value):
    """Square timestamps are RFC3339 (timezone-aware). Odoo Datetime = naive UTC."""
    if not value:
        return None
    dt = value if isinstance(value, datetime) else dt_parser.parse(value)
    if dt.tzinfo:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class SquareManualResyncLine(models.TransientModel):
    _name = "square.manual.resync.line"
    _description = "Square Manual Resync Line"

    wizard_id = fields.Many2one(
        "square.manual.resync.wizard",
        string="Wizard",
        ondelete="cascade",
        required=True,
    )
    selected = fields.Boolean(string="Selected", default=True)
    square_order_id = fields.Char(string="Square Order ID", required=True)
    created_at = fields.Datetime(string="Created At")
    state = fields.Char(string="State")
    location_id = fields.Char(string="Location ID")
    total_amount = fields.Float(string="Total Amount")


class SquareManualResyncWizard(models.TransientModel):
    _name = "square.manual.resync.wizard"
    _description = "Square Manual Resync Wizard"

    config_id = fields.Many2one(
        "square.config",
        string="Square Configuration",
        required=True,
        ondelete="cascade",
    )

    # Date range filters (default window must cover typical sandbox / historical POS data)
    days_back = fields.Integer(
        string="Days Back",
        default=7,
        help="Used to set Start/End when those dates are empty. "
        "Increase if Square shows orders older than this window.",
    )
    start_at = fields.Datetime(
        string="Start Date",
        help="Inclusive start in UTC (Square SearchOrders created_at).",
    )
    end_at = fields.Datetime(
        string="End Date",
        help="Inclusive end in UTC. Usually leave with Days Back or set explicitly.",
    )

    # Optional filters
    location_ids = fields.Many2many(
        "square.location.mapping",
        string="Locations",
        help="Filter by specific locations (empty = all locations)",
    )

    state_filter = fields.Char(
        string="State Filter",
        default="OPEN,COMPLETED",
        help="Comma-separated list of states (e.g., OPEN,COMPLETED,CANCELED)",
    )

    # Line items (results)
    line_ids = fields.One2many(
        "square.manual.resync.line",
        "wizard_id",
        string="Missing Orders",
        readonly=True,
    )

    deferred_order_references = fields.Text(string="Orders Awaiting Ownership Verification", readonly=True)

    # Counters
    square_total = fields.Integer(
        string="Total Orders in Square",
        readonly=True,
        default=0,
    )
    odoo_total = fields.Integer(
        string="Total Orders in Odoo",
        readonly=True,
        default=0,
    )
    missing_total = fields.Integer(
        string="Missing Order Count",
        compute="_compute_counters",
        readonly=True,
    )
    selected_total = fields.Integer(
        string="Selected for Resync",
        compute="_compute_counters",
        readonly=True,
    )

    # State machine
    state = fields.Selection(
        [
            ("preview", "Preview"),
            ("scanning", "Scanning"),
            ("results", "Results"),
            ("syncing", "Syncing"),
            ("done", "Done"),
        ],
        string="State",
        default="preview",
        readonly=True,
    )

    # Final result summary
    processed_count = fields.Integer(string="Processed Count", readonly=True)
    error_count = fields.Integer(string="Error Count", readonly=True)
    error_details = fields.Text(string="Error Details", readonly=True)

    @api.depends("line_ids", "line_ids.selected")
    def _compute_counters(self):
        for record in self:
            record.missing_total = len(record.line_ids)
            record.selected_total = len(record.line_ids.filtered(lambda l: l.selected))

    def _reopen_wizard_action(self):
        """Re-show this wizard in a dialog (avoids full client reload closing the modal)."""
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "name": _("Manual Order Resync"),
            "res_model": self._name,
            "res_id": self.id,
            "view_mode": "form",
            "target": "new",
            "context": dict(self.env.context),
        }

    @api.onchange("days_back")
    def _onchange_days_back(self):
        """Auto-compute date range from days_back"""
        if self.days_back:
            now = datetime.utcnow()
            self.end_at = now
            self.start_at = now - timedelta(days=self.days_back)

    def action_scan_missing_orders(self):
        """
        Scan Square for orders and identify missing ones in Odoo
        """
        self.ensure_one()
        self.check_access("write")
        self.config_id.check_access("write")
        _logger.info("=" * 70)
        _logger.info("SCAN MISSING ORDERS STARTED")
        _logger.info(f"Wizard ID: {self.id}, Config: {self.config_id.name if self.config_id else 'None'}")
        _logger.info(f"Days back: {self.days_back}, Start: {self.start_at}, End: {self.end_at}")
        _logger.info("=" * 70)

        try:
            # Validate configuration
            if not self.config_id:
                raise ValidationError("No Square configuration selected")

            # Auto-compute dates if not set
            if not self.start_at or not self.end_at:
                now = datetime.utcnow()
                self.end_at = now
                self.start_at = now - timedelta(days=self.days_back)

            range_start = _square_ts_to_odoo_naive(self.start_at)
            range_end = _square_ts_to_odoo_naive(self.end_at)

            _logger.info(
                f"Scanning Square orders from {self.start_at} to {self.end_at}"
            )

            # Get API client
            api_client = self.env["square.api.client"]

            # Parse state filters
            states = [s.strip() for s in self.state_filter.split(",") if s.strip()]
            if not states:
                states = ["OPEN", "COMPLETED"]

            config = self.config_id
            mappings = self.location_ids or config.location_mapping_ids
            if not mappings:
                raise UserError("No locations configured. Please configure location mappings in Square Settings.")

            orders_by_id = {}
            for location_id in mappings.mapped("square_location_id"):
                try:
                    orders = api_client.get_location_orders(
                        location_id=location_id, limit=500, start_at=range_start,
                        end_at=range_end, states=states, square_config=config,
                    )
                except UserError as error:
                    raise UserError(_("Could not complete the Square order scan for location %(location)s. %(reason)s",
                                      location=location_id, reason=str(error))) from error
                orders_by_id.update((order["id"], order) for order in orders if order.get("id"))

            existing_orders = self.env["sale.order"].search([
                ("square_order_id", "in", list(orders_by_id)),
                ("company_id", "=", config.company_id.id),
            ])
            existing_order_ids = set(existing_orders.mapped("square_order_id"))
            line_vals = []
            deferred = []
            for order_id in sorted(orders_by_id.keys() - existing_order_ids):
                order = orders_by_id[order_id]
                routing = self.env["square.order.processor"]._square_pos_order_result(
                    order_id=order_id, location_id=order.get("location_id"),
                    square_config=config, full_order=order,
                )
                if routing:
                    if routing["status"] == "retry":
                        deferred.append(order_id)
                    continue
                try:
                    created_at = _square_ts_to_odoo_naive(order.get("created_at"))
                except (ValueError, TypeError, OverflowError):
                    created_at = None
                line_vals.append((0, 0, {
                    "square_order_id": order_id,
                    "created_at": created_at,
                    "state": order.get("state", "UNKNOWN"),
                    "location_id": order.get("location_id", ""),
                    "total_amount": float(order.get("total_money", {}).get("amount", 0)) / 100.0,
                    "selected": True,
                }))

            self.line_ids.unlink()
            self.line_ids = line_vals
            self.deferred_order_references = "\n".join(deferred)
            self.odoo_total = len(existing_order_ids)
            self.square_total = self.odoo_total + len(line_vals)
            self.state = "results"

            return self._reopen_wizard_action()

        except UserError:
            raise
        except Exception as e:
            _logger.error(f"Error scanning orders: {str(e)}", exc_info=True)
            raise UserError(f"Error scanning orders: {str(e)}")

    def action_validate_resync(self):
        """
        Process selected missing orders through the webhook pipeline
        """
        self.ensure_one()
        self.check_access("write")
        self.config_id.check_access("write")

        try:
            # Get selected lines
            selected_lines = self.line_ids.filtered(lambda l: l.selected)
            if not selected_lines:
                raise ValidationError("No orders selected for resync")

            _logger.info(f"Starting resync of {len(selected_lines)} orders")

            order_processor = self.env["square.order.processor"].with_company(self.config_id.company_id)
            api_client = self.env["square.api.client"]

            processed_count = 0
            error_details = []

            # Update state to syncing
            self.state = "syncing"

            # Process each selected order
            for line in selected_lines:
                order_id = line.square_order_id
                failure = f"Could not fetch order {order_id} from Square"
                try:
                    with self.env.cr.savepoint():
                        order_data = api_client.get_order(order_id, square_config=self.config_id)
                        if not order_data or order_data.get("id") != order_id:
                            raise UserError(failure)

                        events = ("created", "updated") if order_data.get("state") == "COMPLETED" else ("created",)
                        for event_type in events:
                            stage = "import" if event_type == "created" else "complete"
                            failure = f"Could not {stage} Square order {order_id} in Odoo"
                            result = order_processor._process_order_event(
                                order_data, event_type, square_config=self.config_id,
                            )
                            status = result.get("status")
                            if status == "ignored" or result.get("is_exchange"):
                                break
                            if status not in ("success", "already_processed", "updated", "skipped"):
                                if status in ("retry", "queued", "waiting_for_order"):
                                    failure = f"Square order {order_id} needs another {stage} attempt"
                                raise UserError(failure)
                except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                    raise
                except Exception as error:
                    error_details.append(failure)
                    _logger.warning("%s (%s)", failure, type(error).__name__)
                else:
                    if status != "ignored":
                        processed_count += 1

            # Update final counts and move to done state
            self.processed_count = processed_count
            error_count = len(error_details)
            self.error_count = error_count
            self.error_details = "\n".join(error_details)
            self.state = "done"

            errors_html = f"<pre>{escape(self.error_details)}</pre>" if error_details else ""

            # Log batch operation
            self.env["square.integration.log"]._log_square_event(
                event_type="manual_resync",
                title=f"Manual resync completed: {processed_count} orders processed",
                description=f"""
                    <p><strong>Manual Resync Batch Complete</strong></p>
                    <ul>
                        <li>Total Selected: {len(selected_lines)}</li>
                        <li>Successfully Processed: {processed_count}</li>
                        <li>Errors: {error_count}</li>
                        <li>Date Range: {self.start_at} to {self.end_at}</li>
                    </ul>
                    {errors_html}
                """,
                status="success" if error_count == 0 else "warning",
            )

            _logger.info(
                f"Resync complete: {processed_count} processed, {error_count} errors"
            )

            return self._reopen_wizard_action()

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except ValidationError:
            raise
        except Exception as e:
            _logger.error(f"Error during resync: {str(e)}", exc_info=True)
            raise UserError(f"Error during resync: {str(e)}")

    def action_close_wizard(self):
        """Close the wizard"""
        return {"type": "ir.actions.act_window_close"}

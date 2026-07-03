# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from datetime import datetime, timedelta, timezone
from dateutil import parser as dt_parser
from odoo import _, api, fields, models
from odoo.exceptions import UserError, ValidationError

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
        string="Missing Orders",
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

            # Fetch orders from Square
            square_order_ids = set()
            try:
                # Get config to determine which locations to search
                config = self.config_id

                # Get location mappings to search
                if self.location_ids:
                    # User specified specific locations
                    location_ids_to_search = self.location_ids.mapped("square_location_id")
                else:
                    # No specific locations - get from config mappings
                    location_ids_to_search = config.location_mapping_ids.mapped("square_location_id")

                _logger.info(f"Searching {len(location_ids_to_search)} locations for orders")
                _logger.info(f"Date range: {self.start_at} to {self.end_at}")
                _logger.info(f"States: {states}")

                # Fetch orders from each location (simpler than /orders/search)
                if not location_ids_to_search:
                    raise UserError("No locations configured. Please configure location mappings in Square Settings.")

                for location_id in location_ids_to_search:
                    try:
                        _logger.info(f"Fetching orders for location: {location_id}")
                        # Square SearchOrders: server-side date + state filter + pagination
                        # (avoids empty results when >limit orders exist outside the window)
                        loc_orders = api_client.get_location_orders(
                            location_id=location_id,
                            limit=500,
                            start_at=range_start,
                            end_at=range_end,
                            states=states,
                            square_config=config,
                        )

                        _logger.info(
                            f"Location {location_id} returned {len(loc_orders)} orders"
                        )

                        for order in loc_orders:
                            order_id = order.get("id")
                            if order_id:
                                square_order_ids.add(order_id)
                    except Exception as e:
                        _logger.warning(
                            f"Error fetching orders for location {location_id}: {str(e)}"
                        )
                        continue

                _logger.info(f"Found {len(square_order_ids)} orders in Square")

            except Exception as e:
                _logger.error(f"Error fetching orders from Square: {str(e)}", exc_info=True)
                raise UserError(f"Failed to fetch orders from Square: {str(e)}")

            # Get existing orders in Odoo
            existing_orders = self.env["sale.order"].search(
                [("square_order_id", "in", list(square_order_ids))]
            )
            existing_order_ids = set(existing_orders.mapped("square_order_id"))

            _logger.info(
                f"Found {len(existing_order_ids)} orders already in Odoo"
            )

            # Compute missing orders
            missing_order_ids = square_order_ids - existing_order_ids
            _logger.info(f"Found {len(missing_order_ids)} missing orders")

            # Fetch full details for missing orders and create wizard lines
            line_vals = []
            for order_id in sorted(missing_order_ids):
                try:
                    order_data = api_client.get_order(
                        order_id, square_config=self.config_id
                    )
                    if order_data:
                        created_at_str = order_data.get("created_at")
                        created_at = None
                        if created_at_str:
                            try:
                                created_at = _square_ts_to_odoo_naive(created_at_str)
                            except Exception:
                                pass

                        line_vals.append(
                            (
                                0,
                                0,
                                {
                                    "square_order_id": order_id,
                                    "created_at": created_at,
                                    "state": order_data.get("state", "UNKNOWN"),
                                    "location_id": order_data.get("location_id", ""),
                                    "total_amount": (
                                        float(order_data.get("total_money", {}).get("amount", 0))
                                        / 100.0
                                    ),
                                    "selected": True,
                                },
                            )
                        )
                except Exception as e:
                    _logger.warning(f"Error fetching order {order_id}: {str(e)}")
                    # Still add it to lines but without full details
                    line_vals.append(
                        (
                            0,
                            0,
                            {
                                "square_order_id": order_id,
                                "selected": True,
                            },
                        )
                    )

            # Clear existing lines and add new ones
            self.line_ids.unlink()
            self.line_ids = line_vals

            # Update counters and move to results state
            self.square_total = len(square_order_ids)
            self.odoo_total = len(existing_order_ids)
            self.state = "results"

            _logger.info(
                f"Scan complete: {len(square_order_ids)} in Square, "
                f"{len(existing_order_ids)} in Odoo, {len(missing_order_ids)} missing"
            )

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

        try:
            # Get selected lines
            selected_lines = self.line_ids.filtered(lambda l: l.selected)
            if not selected_lines:
                raise ValidationError("No orders selected for resync")

            _logger.info(f"Starting resync of {len(selected_lines)} orders")

            # Get webhook controller for processing
            from odoo.addons.odoo_square.controllers.square_webhook import (
                SquareWebhookController,
            )

            webhook_controller = SquareWebhookController()
            api_client = self.env["square.api.client"]

            processed_count = 0
            error_count = 0
            error_details = []

            # Update state to syncing
            self.state = "syncing"

            # Process each selected order
            for line in selected_lines:
                try:
                    order_id = line.square_order_id
                    _logger.info(f"Fetching full order data for {order_id}")

                    # Fetch full order from Square
                    order_data = api_client.get_order(
                        order_id, square_config=self.config_id
                    )
                    if not order_data:
                        raise UserError(f"Could not fetch order {order_id} from Square")

                    # Normalize order_id
                    if order_data.get("id") and not order_data.get("order_id"):
                        order_data["order_id"] = order_data["id"]

                    # Build webhook-like event payload for order creation
                    event_type = "order.created"
                    event_data = {"object": {"order_created": order_data}}

                    # Process through webhook controller with transaction isolation
                    with self.env.cr.savepoint():
                        result = webhook_controller._process_event(
                            event_type, event_data, webhook_event_id=None
                        )

                    if result.get("status") in ["success", "already_processed"]:
                        processed_count += 1
                        _logger.info(
                            f"Successfully processed order {order_id}: {result}"
                        )
                    else:
                        error_count += 1
                        error_msg = f"Order {order_id}: {result.get('message', 'Unknown error')}"
                        error_details.append(error_msg)
                        _logger.warning(error_msg)

                    # If order is already completed, also process completion
                    order_state = order_data.get("state")
                    if order_state == "COMPLETED":
                        _logger.info(
                            f"Order {order_id} is COMPLETED, processing completion event"
                        )
                        event_type_update = "order.updated"
                        event_data_update = {"object": {"order_updated": order_data}}

                        with self.env.cr.savepoint():
                            result_update = webhook_controller._process_event(
                                event_type_update, event_data_update, webhook_event_id=None
                            )

                        if result_update.get("status") not in ["error"]:
                            _logger.info(
                                f"Successfully processed completion for {order_id}"
                            )
                        else:
                            _logger.warning(
                                f"Completion processing had issues for {order_id}: {result_update}"
                            )

                except Exception as e:
                    error_count += 1
                    error_msg = f"Order {line.square_order_id}: {str(e)}"
                    error_details.append(error_msg)
                    _logger.error(error_msg, exc_info=True)

            # Update final counts and move to done state
            self.processed_count = processed_count
            self.error_count = error_count
            self.error_details = "\n".join(error_details)
            self.state = "done"

            # Log batch operation
            self.env["square.integration.log"].log_square_event(
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
                    {f"<p><strong>Errors:</strong></p><pre>{self.error_details}</pre>" if error_details else ""}
                """,
                status="success" if error_count == 0 else "warning",
            )

            _logger.info(
                f"Resync complete: {processed_count} processed, {error_count} errors"
            )

            return self._reopen_wizard_action()

        except ValidationError:
            raise
        except Exception as e:
            _logger.error(f"Error during resync: {str(e)}", exc_info=True)
            raise UserError(f"Error during resync: {str(e)}")

    def action_close_wizard(self):
        """Close the wizard"""
        return {"type": "ir.actions.act_window_close"}

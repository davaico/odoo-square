# -*- coding: utf-8 -*-
import json
import logging
import uuid
from odoo import models, fields, api
from odoo.exceptions import AccessError
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY
from datetime import timedelta

_logger = logging.getLogger(__name__)


class SquareWebhookQueue(models.Model):
    """
    Queue for webhook events that need to be retried later.
    This handles cases where events arrive out of order (e.g., order.updated before order.created)
    """

    _name = "square.webhook.queue"
    _description = "Square Webhook Queue for Retry"
    _order = "create_date asc"

    # Event Information
    webhook_event_id = fields.Char(string="Webhook Event ID", required=True, index=True)
    event_type = fields.Char(string="Event Type", required=True, index=True)
    order_data = fields.Text(string="Order Data (JSON)", required=True)
    config_id = fields.Many2one("square.config", string="Square Configuration", readonly=True)

    # Set only by the signed ingress path. Pre-upgrade rows are unverified.
    signature_verified = fields.Boolean(readonly=True, copy=False)

    # Retry Logic
    square_order_id = fields.Char(
        string="Square Order ID",
        index=True,
        help="The Square order ID that this event depends on",
    )
    order_version = fields.Integer(
        string="Order Version",
        index=True,
        help="The version number of the order from Square",
    )
    retry_count = fields.Integer(string="Retry Count", default=0)
    max_retries = fields.Integer(string="Max Retries", default=10)
    next_retry_time = fields.Datetime(string="Next Retry Time", index=True)

    # Status
    state = fields.Selection(
        [
            ("pending", "Pending"),
            ("processing", "Processing"),
            ("completed", "Completed"),
            ("failed", "Failed"),
            ("expired", "Expired"),
        ],
        string="State",
        default="pending",
        required=True,
        index=True,
    )

    # Dependencies
    depends_on_order = fields.Boolean(
        string="Depends on Order Creation",
        default=True,
        help="If True, this event requires the order to exist first",
    )

    # Metadata
    error_message = fields.Text(string="Last Error Message")
    processed_at = fields.Datetime(string="Processed At")

    @api.model
    def _queue_event(
        self, webhook_event_id, event_type, order_data, square_order_id=None, square_config=None,
    ):
        """
        Queue a webhook event for later processing

        Args:
            webhook_event_id: Unique webhook event ID
            event_type: Type of event (order.updated, etc.)
            order_data: The full order data as dict
            square_order_id: The Square order ID that this event relates to
        """
        if not webhook_event_id:
            oid = (
                square_order_id
                or (order_data or {}).get("order_id")
                or (order_data or {}).get("id")
                or "unknown"
            )
            webhook_event_id = f"synthetic-{event_type}-{oid}-{uuid.uuid4().hex[:16]}"

        # Check if already queued
        existing = self.search([
            ("config_id", "=", square_config.id if square_config else False),
            ("webhook_event_id", "=", webhook_event_id),
            ("signature_verified", "=", True),
        ], limit=1)
        if existing:
            _logger.info(
                f"Event {webhook_event_id} already queued, updating retry time"
            )
            existing.write(
                {
                    "next_retry_time": fields.Datetime.now() + timedelta(seconds=30),
                    "state": "pending",
                    "order_data": json.dumps(self._retry_snapshot(order_data)),
                }
            )
            return existing

        # Create new queue entry
        vals = {
            "webhook_event_id": webhook_event_id,
            "event_type": event_type,
            "order_data": json.dumps(self._retry_snapshot(order_data)),
            "signature_verified": True,
            "config_id": square_config.id if square_config else False,
            "square_order_id": square_order_id,
            "order_version": order_data.get("version"),
            "next_retry_time": fields.Datetime.now() + timedelta(seconds=30),
            "depends_on_order": event_type == "order.updated",
        }

        return self.create(vals)

    @api.model
    def _retry_snapshot(self, order_data):
        """Retain only the fields used by order-update replay."""
        data = {
            key: order_data[key]
            for key in ("id", "order_id", "state", "location_id", "version")
            if key in order_data
        }
        if "line_items" in order_data:
            data["line_items"] = [
                {key: line[key] for key in ("uid", "quantity") if key in line}
                for line in order_data["line_items"]
            ]
        if "returns" in order_data or "return_line_items" in order_data:
            data["return_line_items"] = [True] if (order_data.get("returns") or order_data.get("return_line_items")) else []
        return data

    @api.model
    def process_pending_events(self):
        """Run the retry schedule as the configured company's administrator."""
        config = self.env["square.config"].search([("company_id", "=", self.env.company.id)], limit=1)
        # Check the role even if this company has no active configuration.
        if not self.env.user.has_group("base.group_system"):
            raise AccessError("Only administrators can run Square webhook retries.")
        if config:
            config._check_square_operator()
            self.sudo()._process_pending_events(config)

    def _process_pending_events(self, config):
        now = fields.Datetime.now()

        # Find events ready for retry
        pending_events = self.search(
            [
                ("config_id", "=", config.id),
                ("state", "=", "pending"),
                ("next_retry_time", "<=", now),
            ],
            order="order_version asc",
            limit=50,  # Process in batches
        )

        _logger.error(
            f"Processing {len(pending_events)} pending webhook events from queue"
        )

        for event in pending_events:
            try:
                event._process_queued_event()
            except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                raise
            except Exception as e:
                _logger.error(
                    f"Error processing queued event {event.webhook_event_id}: {str(e)}"
                )

        # Clean up old expired events
        self._cleanup_expired_events(config)

    def _process_queued_event(self):
        """Process a single queued event"""
        self.ensure_one()

        if not self.signature_verified or not self.config_id or not self.config_id.active:
            self.write({
                "state": "failed", "order_data": "{}",
                "error_message": "Unverified legacy event; waiting for a new signed delivery.",
            })
            return False

        # Mark as processing
        self.write({"state": "processing", "retry_count": self.retry_count + 1})

        try:
            # Parse order data
            order_data = json.loads(self.order_data)

            if self.event_type != "order.updated":
                self.write({"state": "failed", "order_data": "{}", "error_message": "Unsupported event type"})
                return False

            config = self.config_id
            scoped = self.with_context(allowed_company_ids=config.company_id.ids)
            result = scoped.env["square.webhook.event"]._apply_provider_event(
                config, self.webhook_event_id,
                lambda: scoped.env["square.order.processor"]._process_webhook_order(
                    order_data, "updated", self.webhook_event_id, square_config=config,
                    requires_existing=True, signed=True,
                ),
            )

            # Check result
            if result.get("status") == "waiting_for_order":
                retry_delay = min(30 * (2**self.retry_count), 3600)
                expired = self.retry_count >= self.max_retries
                self.write({
                    "state": "expired" if expired else "pending",
                    "order_data": "{}" if expired else self.order_data,
                    "next_retry_time": fields.Datetime.now() + timedelta(seconds=retry_delay),
                    "error_message": (
                        f"Max retries ({self.max_retries}) reached, order still not found" if expired
                        else f"Order {self.square_order_id} not found (retry {self.retry_count})"
                    ),
                })
                return False

            if result.get("status") in ("success", "updated", "already_processed", "ignored"):
                self.write(
                    {
                        "state": "completed",
                        "processed_at": fields.Datetime.now(),
                        "order_data": "{}",
                        "error_message": None,
                    }
                )
                _logger.info(
                    f"Successfully processed queued event {self.webhook_event_id}"
                )
                return True
            else:
                # Processing failed, schedule retry
                retry_delay = min(60 * (2**self.retry_count), 3600)
                next_retry = fields.Datetime.now() + timedelta(seconds=retry_delay)

                self.write(
                    {
                        "state": "pending",
                        "next_retry_time": next_retry,
                        "error_message": "Square event processing did not complete.",
                    }
                )

                if self.retry_count >= self.max_retries:
                    self.write({"state": "failed", "order_data": "{}"})
                    _logger.error(
                        f"Event {self.webhook_event_id} failed after {self.max_retries} retries"
                    )

                return False

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            error_msg = str(e)
            _logger.error(
                f"Error processing queued event {self.webhook_event_id}: {error_msg}",
                exc_info=True,
            )

            # Schedule retry
            retry_delay = min(60 * (2**self.retry_count), 3600)
            next_retry = fields.Datetime.now() + timedelta(seconds=retry_delay)

            self.write(
                {
                    "state": "pending",
                    "next_retry_time": next_retry,
                    "error_message": "Square event processing failed.",
                }
            )

            if self.retry_count >= self.max_retries:
                self.write({"state": "failed", "order_data": "{}"})

            return False

    @api.model
    def _cleanup_expired_events(self, config):
        """Clean up old expired or completed events"""
        # Remove completed events older than 7 days
        cutoff_completed = fields.Datetime.now() - timedelta(days=7)
        old_completed = self.search(
            [("config_id", "=", config.id), ("state", "=", "completed"), ("processed_at", "<", cutoff_completed)]
        )

        if old_completed:
            _logger.info(
                f"Cleaning up {len(old_completed)} old completed queued events"
            )
            old_completed.unlink()

        # Remove expired/failed events older than 30 days
        cutoff_failed = fields.Datetime.now() - timedelta(days=30)
        old_failed = self.search(
            [
                ("config_id", "=", config.id),
                ("state", "in", ["expired", "failed"]),
                ("create_date", "<", cutoff_failed),
            ]
        )

        if old_failed:
            _logger.info(f"Cleaning up {len(old_failed)} old failed queued events")
            old_failed.unlink()

    @api.model
    def _process_pending_for_order(self, square_order_id, config):
        """
        Process all pending events for a specific order
        Called after successfully creating an order

        Args:
            square_order_id: The Square order ID that was just created
        """
        if not config:
            return
        pending_events = self.search(
            [("config_id", "=", config.id), ("square_order_id", "=", square_order_id), ("state", "=", "pending")],
            order="order_version asc",
        )

        if not pending_events:
            return

        _logger.info(
            f"Processing {len(pending_events)} pending events for order {square_order_id}"
        )

        for event in pending_events:
            try:
                event._process_queued_event()
            except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                raise
            except Exception as e:
                _logger.error(
                    f"Error processing pending event {event.webhook_event_id} for order {square_order_id}: {str(e)}"
                )

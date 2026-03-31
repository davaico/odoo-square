# -*- coding: utf-8 -*-
import json
import logging
import uuid
from odoo import models, fields, api
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
    def queue_event(
        self, webhook_event_id, event_type, order_data, square_order_id=None
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
        existing = self.search([("webhook_event_id", "=", webhook_event_id)], limit=1)
        if existing:
            _logger.info(
                f"Event {webhook_event_id} already queued, updating retry time"
            )
            existing.write(
                {
                    "next_retry_time": fields.Datetime.now() + timedelta(seconds=30),
                    "state": "pending",
                }
            )
            return existing

        # Create new queue entry
        vals = {
            "webhook_event_id": webhook_event_id,
            "event_type": event_type,
            "order_data": json.dumps(order_data),
            "square_order_id": square_order_id,
            "order_version": order_data.get("version"),
            "next_retry_time": fields.Datetime.now() + timedelta(seconds=30),
            "depends_on_order": event_type == "order.updated",
        }

        return self.create(vals)

    @api.model
    def process_pending_events(self):
        """
        Process pending events in the queue (called by cron)
        """
        now = fields.Datetime.now()

        # Find events ready for retry
        pending_events = self.search(
            [
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
            except Exception as e:
                _logger.error(
                    f"Error processing queued event {event.webhook_event_id}: {str(e)}"
                )

        # Clean up old expired events
        self._cleanup_expired_events()

    def _process_queued_event(self):
        """Process a single queued event"""
        self.ensure_one()

        # Mark as processing
        self.write({"state": "processing", "retry_count": self.retry_count + 1})

        try:
            # Parse order data
            order_data = json.loads(self.order_data)

            # Check if dependency is met (order exists)
            if self.depends_on_order and self.square_order_id:
                order_exists = (
                    self.env["sale.order"]
                    .sudo()
                    .search([("square_order_id", "=", self.square_order_id)], limit=1)
                )

                if not order_exists:
                    # Order still doesn't exist, schedule next retry with exponential backoff
                    retry_delay = min(30 * (2**self.retry_count), 3600)  # Max 1 hour
                    next_retry = fields.Datetime.now() + timedelta(seconds=retry_delay)

                    _logger.info(
                        f"Order {self.square_order_id} still not found for event {self.webhook_event_id}, "
                        f"retry {self.retry_count}/{self.max_retries} scheduled in {retry_delay}s"
                    )

                    self.write(
                        {
                            "state": "pending",
                            "next_retry_time": next_retry,
                            "error_message": f"Order {self.square_order_id} not found (retry {self.retry_count})",
                        }
                    )

                    # If max retries reached, mark as expired
                    if self.retry_count >= self.max_retries:
                        self.write(
                            {
                                "state": "expired",
                                "error_message": f"Max retries ({self.max_retries}) reached, order still not found",
                            }
                        )
                        _logger.warning(
                            f"Event {self.webhook_event_id} expired after {self.max_retries} retries"
                        )

                    return False

            # Process the event using the webhook controller
            from odoo.addons.odoo_square.controllers.square_webhook import (
                SquareWebhookController,
            )

            controller = SquareWebhookController()

            # Extract order data based on event type
            if self.event_type == "order.updated":
                _logger.info(
                    f"Processing queued event {self.webhook_event_id} - Order state: {order_data.get('state')}"
                )
                result = controller._process_order(
                    order_data, "updated", self.webhook_event_id
                )
            elif self.event_type == "order.created":
                _logger.info(
                    f"Processing queued event {self.webhook_event_id} - Order state: {order_data.get('state')}"
                )
                result = controller._process_order(
                    order_data, "created", self.webhook_event_id
                )
            else:
                _logger.error(
                    f"Unsupported event type {self.event_type} for queued event {self.webhook_event_id}"
                )
                self.write(
                    {"state": "failed", "error_message": "Unsupported event type"}
                )
                return False

            # Check result
            if result.get("status") in ["success", "updated", "already_processed"]:
                self.write(
                    {
                        "state": "completed",
                        "processed_at": fields.Datetime.now(),
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
                        "error_message": result.get("message", "Unknown error"),
                    }
                )

                if self.retry_count >= self.max_retries:
                    self.write({"state": "failed"})
                    _logger.error(
                        f"Event {self.webhook_event_id} failed after {self.max_retries} retries"
                    )

                return False

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
                    "error_message": error_msg,
                }
            )

            if self.retry_count >= self.max_retries:
                self.write({"state": "failed"})

            return False

    @api.model
    def _cleanup_expired_events(self):
        """Clean up old expired or completed events"""
        # Remove completed events older than 7 days
        cutoff_completed = fields.Datetime.now() - timedelta(days=7)
        old_completed = self.search(
            [("state", "=", "completed"), ("processed_at", "<", cutoff_completed)]
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
                ("state", "in", ["expired", "failed"]),
                ("create_date", "<", cutoff_failed),
            ]
        )

        if old_failed:
            _logger.info(f"Cleaning up {len(old_failed)} old failed queued events")
            old_failed.unlink()

    @api.model
    def process_pending_for_order(self, square_order_id):
        """
        Process all pending events for a specific order
        Called after successfully creating an order

        Args:
            square_order_id: The Square order ID that was just created
        """
        pending_events = self.search(
            [("square_order_id", "=", square_order_id), ("state", "=", "pending")],
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
            except Exception as e:
                _logger.error(
                    f"Error processing pending event {event.webhook_event_id} for order {square_order_id}: {str(e)}"
                )

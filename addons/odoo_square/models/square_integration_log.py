# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import models, fields, api
import logging

_logger = logging.getLogger(__name__)


class SquareIntegrationLog(models.Model):
    _name = "square.integration.log"
    _description = "Square Integration Activity Log"
    _order = "create_date desc"
    _rec_name = "display_name"

    display_name = fields.Char(
        string="Summary", compute="_compute_display_name", store=True
    )

    # Core Information
    event_type = fields.Selection(
        [
            ("order_created", "Order Created"),
            ("order_updated", "Order Updated"),
            ("refund_created", "Refund Created"),
            ("refund_updated", "Refund Updated"),
            ("refund_processed", "Refund Processed"),
            ("exchange_processed", "Exchange Processed"),
            ("return_processed", "Return Processed"),
            ("stock_sync", "Stock Synchronization"),
            ("webhook_received", "Webhook Received"),
            ("error", "Error"),
        ],
        string="Event Type",
        required=True,
        index=True,
    )

    status = fields.Selection(
        [
            ("success", "Success"),
            ("warning", "Warning"),
            ("error", "Error"),
            ("info", "Information"),
        ],
        string="Status",
        required=True,
        default="info",
        index=True,
    )

    # Square Data
    square_order_id = fields.Char(
        string="Square Order ID", help="Square order identifier", index=True
    )

    square_refund_id = fields.Char(
        string="Square Refund ID", help="Square refund identifier"
    )

    # Odoo Relations
    sale_order_id = fields.Many2one(
        "sale.order", string="Sale Order", help="Related Odoo sale order"
    )

    # Message Details
    title = fields.Char(string="Title", required=True)
    description = fields.Html(string="Description")
    technical_details = fields.Text(string="Technical Details")

    # Metadata
    webhook_event_id = fields.Char(string="Webhook Event ID")
    processing_time = fields.Float(string="Processing Time (seconds)")

    @api.depends("event_type", "title", "square_order_id")
    def _compute_display_name(self):
        for record in self:
            if record.square_order_id:
                record.display_name = f"[{record.event_type.replace('_', ' ').title()}] {record.title} ({record.square_order_id})"
            else:
                record.display_name = (
                    f"[{record.event_type.replace('_', ' ').title()}] {record.title}"
                )

    @api.model
    def log_square_event(
        self,
        event_type,
        title,
        description=None,
        status="info",
        square_order_id=None,
        square_refund_id=None,
        sale_order_id=None,
        technical_details=None,
        webhook_event_id=None,
        processing_time=None,
    ):
        """
        Utility method to log Square integration events
        """
        try:
            vals = {
                "event_type": event_type,
                "status": status,
                "title": title,
                "description": description or "",
                "square_order_id": square_order_id,
                "square_refund_id": square_refund_id,
                "sale_order_id": sale_order_id,
                "technical_details": technical_details,
                "webhook_event_id": webhook_event_id,
                "processing_time": processing_time,
            }

            return self.create(vals)

        except Exception as e:
            _logger.error(f"Error creating Square integration log: {str(e)}")
            return False

    @api.model
    def log_order_creation(self, sale_order, square_data, processing_time=None):
        """Log successful order creation"""
        return self.log_square_event(
            event_type="order_created",
            title=f"Order {sale_order.name} created from Square",
            description=f"""
                <p><strong>Square Order Created Successfully</strong></p>
                <ul>
                    <li>Odoo Order: <strong>{sale_order.name}</strong></li>
                    <li>Square Order ID: <code>{square_data.get('order_id')}</code></li>
                    <li>Customer: {sale_order.partner_id.name}</li>
                    <li>Total Amount: {sale_order.amount_total} {sale_order.currency_id.name}</li>
                    <li>Order Lines: {len(sale_order.order_line)} items</li>
                </ul>
            """,
            status="success",
            square_order_id=square_data.get("order_id"),
            sale_order_id=sale_order.id,
            processing_time=processing_time,
        )

    @api.model
    def log_refund_processed(self, sale_order, refund_data, processing_time=None):
        """Log successful refund processing"""
        return self.log_square_event(
            event_type="refund_processed",
            title=f"Refund processed for order {sale_order.name}",
            description=f"""
                <p><strong>Square Refund Processed Successfully</strong></p>
                <ul>
                    <li>Odoo Order: <strong>{sale_order.name}</strong></li>
                    <li>Square Refund ID: <code>{refund_data.get('id')}</code></li>
                    <li>Refunded Amount: {refund_data.get('amount_money', {}).get('amount', 0) / 100} {refund_data.get('amount_money', {}).get('currency', 'EUR')}</li>
                    <li>Order Lines: Quantity set to 0 (history preserved)</li>
                    <li>Stock: Returned to inventory</li>
                    <li>Credit Note: Created and validated</li>
                </ul>
            """,
            status="success",
            square_order_id=refund_data.get("order_id"),
            square_refund_id=refund_data.get("id"),
            sale_order_id=sale_order.id,
            processing_time=processing_time,
        )

    @api.model
    def log_error(
        self,
        title,
        error_message,
        square_order_id=None,
        sale_order_id=None,
        technical_details=None,
    ):
        """Log errors in Square integration"""
        return self.log_square_event(
            event_type="error",
            title=title,
            description=f"""
                <p><strong>Square Integration Error</strong></p>
                <p class="text-danger">{error_message}</p>
            """,
            status="error",
            square_order_id=square_order_id,
            sale_order_id=sale_order_id,
            technical_details=technical_details,
        )

    @api.model
    def log_webhook_received(self, event_type, event_data, webhook_event_id=None):
        """Log webhook reception"""
        return self.log_square_event(
            event_type="webhook_received",
            title=f"Webhook received: {event_type}",
            description=f"""
                <p><strong>Square Webhook Received</strong></p>
                <ul>
                    <li>Event Type: <code>{event_type}</code></li>
                    <li>Event ID: <code>{webhook_event_id or 'N/A'}</code></li>
                    <li>Data Keys: {', '.join(event_data.keys()) if event_data else 'None'}</li>
                </ul>
            """,
            status="info",
            webhook_event_id=webhook_event_id,
            technical_details=str(event_data) if event_data else None,
        )

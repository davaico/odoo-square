# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json
import logging
from datetime import datetime, timedelta
from lxml import html
from unittest.mock import patch, MagicMock
from odoo.tests import Form, TransactionCase
from odoo.tests.common import tagged
from odoo.exceptions import UserError, ValidationError
from odoo.tools import mute_logger

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient

from .common import SquareHttpCase

_logger = logging.getLogger(__name__)


@tagged("post_install", "-at_install", "TestSquareManualResync")
class TestSquareManualResync(TransactionCase):
    """Test cases for manual resync functionality"""

    def setUp(self):
        super().setUp()
        self.square_config = self.env["square.config"].search([], limit=1)
        if not self.square_config:
            self.square_config = self.env["square.config"].create(
                {
                    "name": "Test Configuration",
                    "square_application_id": "TEST_APP_ID",
                    "square_access_token": "TEST_TOKEN",
                    "square_environment": "sandbox",
                    "square_webhook_signature_key": "TEST_KEY",
                }
            )
        self.warehouse = self.env["stock.warehouse"].search([], limit=1)
        self.env["square.location.mapping"].create(
            {
                "config_id": self.square_config.id,
                "square_location_id": "LOC_123",
                "square_location_name": "Test Location",
                "warehouse_id": self.warehouse.id,
            }
        )

    def test_wizard_creation(self):
        """Test manual resync wizard can be created"""
        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
            }
        )
        self.assertEqual(wizard.state, "preview")
        self.assertEqual(wizard.config_id.id, self.square_config.id)
        self.assertEqual(wizard.days_back, 7)

    def test_wizard_date_range_computation(self):
        """Test that days_back correctly computes date range"""
        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "days_back": 14,
            }
        )
        wizard._onchange_days_back()

        # Check that dates are roughly correct (within 1 minute)
        now = datetime.utcnow()
        self.assertAlmostEqual(
            (wizard.end_at - now).total_seconds(), 0, delta=60
        )
        self.assertAlmostEqual(
            (wizard.start_at - (now - timedelta(days=14))).total_seconds(),
            0,
            delta=60,
        )

    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_location_orders")
    def test_scan_missing_orders_empty(self, mock_get_location_orders):
        """Test scanning when no orders are missing"""
        mock_get_location_orders.return_value = []

        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "days_back": 7,
            }
        )
        wizard._onchange_days_back()

        wizard.action_scan_missing_orders()

        self.assertEqual(wizard.state, "results")
        self.assertEqual(len(wizard.line_ids), 0)
        self.assertEqual(wizard.square_total, 0)
        self.assertEqual(wizard.missing_total, 0)

    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_location_orders")
    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_order")
    def test_scan_identifies_missing_orders(self, mock_get_order, mock_get_location_orders):
        """Test scanning identifies orders missing from Odoo"""
        # Create an existing order in Odoo
        existing_order = self.env["sale.order"].create(
            {
                "partner_id": self.env.ref("base.partner_root").id,
                "square_order_id": "existing_order_123",
            }
        )

        # Mock Square to return both existing and missing orders
        mock_get_location_orders.return_value = [
            {
                "id": "existing_order_123",
                "state": "COMPLETED",
                "created_at": "2024-01-01T10:00:00Z",
                "location_id": "LOC_123",
                "total_money": {"amount": 10000},
            },
            {
                "id": "missing_order_456",
                "state": "COMPLETED",
                "created_at": "2024-01-02T10:00:00Z",
                "location_id": "LOC_123",
                "total_money": {"amount": 20000},
            },
        ]

        # Mock get_order for missing order
        mock_get_order.side_effect = lambda order_id, **kwargs: {
            "id": order_id,
            "state": "COMPLETED",
            "created_at": "2024-01-02T10:00:00Z",
            "location_id": "LOC_123",
            "total_money": {"amount": 20000},
        }

        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "days_back": 7,
            }
        )
        wizard._onchange_days_back()
        wizard.action_scan_missing_orders()

        self.assertEqual(wizard.state, "results")
        self.assertEqual(wizard.square_total, 2)
        self.assertEqual(wizard.odoo_total, 1)
        self.assertEqual(wizard.missing_total, 1)

        # Check that missing order is in the wizard lines
        missing_lines = wizard.line_ids.filtered(
            lambda l: l.square_order_id == "missing_order_456"
        )
        self.assertEqual(len(missing_lines), 1)
        self.assertTrue(missing_lines[0].selected)

    def test_resync_requires_selection(self):
        """Test that resync requires at least one selected order"""
        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "state": "results",
            }
        )

        # Try to resync with no lines selected
        with self.assertRaises(ValidationError):
            wizard.action_validate_resync()

    @patch("odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_webhook_order")
    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_order")
    def test_resync_processes_selected_orders(self, mock_get_order, mock_process_event):
        """Test that resync processes selected orders through webhook pipeline"""
        # Mock order data
        mock_get_order.return_value = {
            "id": "order_789",
            "state": "COMPLETED",
            "created_at": "2024-01-03T10:00:00Z",
            "location_id": "LOC_123",
            "total_money": {"amount": 30000},
            "line_items": [],
        }

        # Mock webhook processing to return success
        mock_process_event.return_value = {"status": "success", "order_id": "order_789"}

        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "state": "results",
            }
        )

        # Add a line to resync
        self.env["square.manual.resync.line"].create(
            {
                "wizard_id": wizard.id,
                "square_order_id": "order_789",
                "selected": True,
                "state": "COMPLETED",
            }
        )

        # Process the resync
        wizard.action_validate_resync()

        self.assertEqual(wizard.state, "done")
        self.assertEqual(wizard.processed_count, 1)
        self.assertEqual(wizard.error_count, 0)

    @patch("odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_webhook_order")
    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_order")
    def test_resync_handles_errors(self, mock_get_order, mock_process_event):
        """Test that resync continues on error and collects error details"""
        # Mock get_order failure
        mock_get_order.side_effect = Exception("Private upstream error body")

        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "state": "results",
            }
        )

        # Add a line to resync
        self.env["square.manual.resync.line"].create(
            {
                "wizard_id": wizard.id,
                "square_order_id": "bad_order<strong>id</strong>",
                "selected": True,
            }
        )

        # Process the resync - should complete despite error
        wizard.action_validate_resync()

        self.assertEqual(wizard.state, "done")
        self.assertEqual(wizard.processed_count, 0)
        self.assertEqual(wizard.error_count, 1)
        self.assertIn("bad_order", wizard.error_details)
        warning = self.env["square.integration.log"].search([
            ("event_type", "=", "manual_resync"), ("status", "=", "warning"),
        ], order="id desc", limit=1)
        self.assertIn("Could not fetch order bad_order<strong>id</strong> from Square",
                      html.fromstring(warning.description).text_content())
        self.assertNotIn("Private upstream error body", warning.description)
        self.assertNotIn("Private upstream error body", wizard.error_details)

    def test_resync_rolls_back_order_when_native_invoicing_is_deferred(self):
        # A draft can appear after the missing-orders scan. A delivered-quantity service
        # with no recorded delivery cannot yet be invoiced by native Odoo accounting.
        order_id = "RESYNC-SERVICE-UNDELIVERED"
        service = self.env["product.product"].create({
            "name": "Undelivered Square service", "type": "service", "invoice_policy": "delivery",
        })
        order = self.env["sale.order"].create({
            "partner_id": self.env.ref("base.partner_root").id, "square_order_id": order_id,
            "order_line": [(0, 0, {"product_id": service.id, "product_uom_qty": 1, "price_unit": 25})],
        })
        wizard = self.env["square.manual.resync.wizard"].create({
            "config_id": self.square_config.id, "state": "results",
            "line_ids": [(0, 0, {"square_order_id": order_id, "selected": True})],
        })
        processor = type(self.env["square.order.processor"])
        with patch.object(
            type(self.env["square.api.client"]), "_get_order",
            return_value={"id": order_id, "location_id": "LOC_123", "state": "COMPLETED"},
        ), patch.object(
            processor, "_process_webhook_order", autospec=True, side_effect=processor._process_webhook_order,
        ) as apply, self.assertLogs("odoo.addons.odoo_square.models.square_order_processor", level="INFO") as messages:
            wizard.action_validate_resync()

        self.assertTrue(any("Square invoice processing failed for" in message for message in messages.output))
        self.assertEqual([call.args[2] for call in apply.call_args_list], ["created", "updated"])
        self.assertEqual((wizard.processed_count, wizard.error_count), (0, 1))
        self.assertIn(order_id, wizard.error_details)
        self.assertEqual(order.state, "draft")
        self.assertFalse(order.invoice_ids)
        self.assertFalse(order.picking_ids)
        self.assertFalse(self.env["square.webhook.event"].search([
            ("config_id", "=", self.square_config.id), ("event_key", "like", order_id),
        ]))

    def test_wizard_state_transitions(self):
        """Test wizard state machine transitions"""
        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
            }
        )

        # Initial state is preview
        self.assertEqual(wizard.state, "preview")

        # Mock the scan to transition to results
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_location_orders",
            return_value=[],
        ):
            wizard.action_scan_missing_orders()
            self.assertEqual(wizard.state, "results")

        # Create a line to resync
        self.env["square.manual.resync.line"].create(
            {
                "wizard_id": wizard.id,
                "square_order_id": "test_order",
                "selected": True,
            }
        )

        # Mock the validate to transition to done
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_order",
            return_value={"id": "test_order", "state": "COMPLETED"},
        ):
            with patch(
                "odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_webhook_order",
                return_value={"status": "success"},
            ):
                wizard.action_validate_resync()
                self.assertEqual(wizard.state, "done")

    def test_deselect_orders_excludes_from_resync(self):
        """Test that deselected orders are not resynced"""
        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "state": "results",
            }
        )

        # Add two lines, one selected, one not
        line1 = self.env["square.manual.resync.line"].create(
            {
                "wizard_id": wizard.id,
                "square_order_id": "order_1",
                "selected": True,
            }
        )
        line2 = self.env["square.manual.resync.line"].create(
            {
                "wizard_id": wizard.id,
                "square_order_id": "order_2",
                "selected": False,
            }
        )

        # Mock the processing
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_order",
            side_effect=lambda oid, **kwargs: {
                "id": oid,
                "state": "COMPLETED",
            },
        ):
            with patch(
                "odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_webhook_order",
                return_value={"status": "success"},
            ):
                wizard.action_validate_resync()

        # Only one order should be processed
        self.assertEqual(wizard.processed_count, 1)
        self.assertEqual(wizard.selected_total, 1)

    def test_configuration_required(self):
        """Test that wizard requires a Square configuration."""
        with self.assertRaises(Exception), self.env.cr.savepoint():
            self.env["square.manual.resync.wizard"].create({})

    def test_wizard_counters_computed(self):
        """Test that wizard counters are properly computed"""
        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "state": "results",
            }
        )

        # Add some lines
        for i in range(3):
            self.env["square.manual.resync.line"].create(
                {
                    "wizard_id": wizard.id,
                    "square_order_id": f"order_{i}",
                    "selected": i < 2,  # First two selected
                }
            )

        # Check computed fields
        self.assertEqual(wizard.selected_total, 2)
        self.assertEqual(wizard.missing_total, 3)


@tagged("post_install", "-at_install")
class TestManualResyncRecovery(SquareHttpCase):
    def _call_button(self, record, button):
        response = self.url_open(
            "/web/dataset/call_button",
            data=json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "call",
                    "params": {
                        "model": record._name,
                        "method": button,
                        "args": [record.ids],
                        "kwargs": {},
                    },
                }
            ),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn("error", response.json(), response.text)
        return response.json()["result"]

    def _scan_resync(self, config):
        action = self._call_button(config, "action_open_manual_resync_wizard")
        self.assertEqual(action["res_model"], "square.manual.resync.wizard")
        self.assertEqual(action["target"], "new")
        admin = self.env.ref("base.user_admin")
        wizard = self.env[action["res_model"]].with_user(admin).browse(action["res_id"])
        with Form(wizard, view="odoo_square.view_square_manual_resync_wizard_form") as form:
            form.start_at = "2025-10-05 00:00:00"
            form.end_at = "2025-10-08 23:59:59"
        action = self._call_button(wizard, "action_scan_missing_orders")
        self.assertEqual(action["res_id"], wizard.id)
        self.assertEqual(action["target"], "new")
        self.assertEqual(wizard.state, "results")
        return wizard

    def _unselect_order(self, wizard, order_id):
        line_index = wizard.line_ids.mapped("square_order_id").index(order_id)
        with Form(wizard, view="odoo_square.view_square_manual_resync_wizard_form") as form:
            with form.line_ids.edit(line_index) as line:
                line.selected = False

    @mute_logger(
        "odoo.addons.odoo_square.controllers.square_webhook",
        "odoo.addons.odoo_square.models.square_api_client",
        "odoo.addons.odoo_square.models.square_manual_resync",
    )
    def test_admin_recovers_only_selected_orders_and_can_repeat_scan(self):
        self.authenticate("admin", "admin")
        self.env.company.currency_id = self.env.ref("base.EUR")
        partner = self.env["res.partner"].create(
            {"name": "Resync Customer", "email": "resync@example.com"}
        )
        journal = self.env["account.journal"].create(
            {"name": "Resync Receipts", "type": "bank", "code": "SQRS"}
        )
        tax = self.env["account.tax"].create(
            {"name": "Square VAT 20%", "type_tax_use": "sale", "amount": 20}
        )
        other_warehouse = self.env["stock.warehouse"].search(
            [("company_id", "=", self.env.company.id)], limit=1
        )
        warehouse = self.env["stock.warehouse"].create(
            {"name": "Resync Warehouse", "code": "SQRS"}
        )
        product = self.env["product.product"].create(
            {
                "name": "Resync Item",
                "default_code": "RESYNC-ITEM",
                "type": "consu",
                "is_storable": True,
                "invoice_policy": "delivery",
                "taxes_id": [(6, 0, tax.ids)],
            }
        )
        for stock_warehouse, quantity in ((warehouse, 5), (other_warehouse, 9)):
            self.env["stock.quant"].create(
                {
                    "product_id": product.id,
                    "location_id": stock_warehouse.lot_stock_id.id,
                    "quantity": quantity,
                }
            )
        config = self.env["square.config"].create(
            {
                "name": "Resync Integration",
                "square_application_id": "test_app_id",
                "square_access_token": "test_access_token",
                "payment_journal_id": journal.id,
                "location_mapping_ids": [
                    (
                        0,
                        0,
                        {
                            "square_location_id": "resync-location",
                            "square_location_name": "Resync Location",
                            "warehouse_id": warehouse.id,
                        },
                    )
                ],
            }
        )
        unavailable_id = "resync-01-unavailable"
        paid_id = "resync-02-paid"
        unselected_id = "resync-03-unselected"
        existing_id = "resync-04-existing"
        order_ids = [unavailable_id, paid_id, unselected_id, existing_id]
        existing_order = self.env["sale.order"].create(
            {"partner_id": partner.id, "square_order_id": existing_id}
        )
        square_order = {
            "id": paid_id,
            "location_id": "resync-location",
            "state": "COMPLETED",
            "created_at": "2025-10-06T12:00:00+02:00",
            "line_items": [
                {
                    "uid": "resync-line",
                    "name": "Resync Item",
                    "catalog_object_id": "RESYNC-ITEM",
                    "quantity": "2",
                    "base_price_money": {"amount": 1250, "currency": "EUR"},
                    "total_money": {"amount": 3000, "currency": "EUR"},
                    "total_tax_money": {"amount": 500, "currency": "EUR"},
                }
            ],
            "total_money": {"amount": 3000, "currency": "EUR"},
            "total_tax_money": {"amount": 500, "currency": "EUR"},
            "fulfillments": [
                {"pickup_details": {"recipient": {"email_address": partner.email}}}
            ],
            "tenders": [
                {
                    "id": "resync-payment",
                    "amount_money": {"amount": 3000, "currency": "EUR"},
                }
            ],
        }
        orders = {order_id: dict(square_order, id=order_id) for order_id in order_ids}
        search_requests = []

        def square_request(endpoint, method="GET", data=None, square_config=None):
            if endpoint == "/v2/orders/search":
                search_requests.append(data)
                return {"orders": list(orders.values())}
            if endpoint == f"/v2/orders/{unavailable_id}" and order_unavailable:
                return {}
            return {"order": orders[endpoint.removeprefix("/v2/orders/")].copy()}

        mapped_stock = product.with_context(warehouse_id=warehouse.id)
        other_stock = product.with_context(warehouse_id=other_warehouse.id)
        self.assertEqual(mapped_stock.qty_available, 5)
        self.assertEqual(other_stock.qty_available, 9)
        invoice_domain = [("partner_id", "=", partner.id), ("move_type", "=", "out_invoice")]
        payment_domain = [("partner_id", "=", partner.id)]
        order_domain = [("square_order_id", "in", order_ids)]
        log_domain = [("event_type", "=", "manual_resync")]
        log_model = self.env["square.integration.log"]
        order_unavailable = False
        with patch.object(SquareApiClient, "_make_api_request", side_effect=square_request):
            wizard = self._scan_resync(config)
            self.assertEqual(
                (wizard.square_total, wizard.odoo_total, wizard.missing_total), (4, 1, 3)
            )
            self.assertCountEqual(wizard.line_ids.mapped("square_order_id"), order_ids[:3])
            self.assertEqual(wizard.selected_total, 3)
            paid_line = wizard.line_ids.filtered(lambda line: line.square_order_id == paid_id)
            self.assertEqual(paid_line.created_at, datetime(2025, 10, 6, 10))
            self.assertEqual(paid_line.total_amount, 30)
            self._unselect_order(wizard, unselected_id)
            self.assertEqual(wizard.selected_total, 2)

            # Another preview can still show the order after the first import.
            stale_wizard = self._scan_resync(config)
            self._unselect_order(stale_wizard, unselected_id)
            self._unselect_order(stale_wizard, unavailable_id)
            self.assertEqual(stale_wizard.selected_total, 1)

            order_unavailable = True
            action = self._call_button(wizard, "action_validate_resync")
            self.assertEqual(action["res_id"], wizard.id)
            self.assertEqual(action["target"], "new")
            self.assertEqual(wizard.state, "done")
            self.assertEqual((wizard.processed_count, wizard.error_count), (1, 1))
            self.assertIn(
                f"Could not fetch order {unavailable_id} from Square", wizard.error_details
            )
            warning_log = log_model.search(log_domain + [("status", "=", "warning")])
            self.assertEqual(len(warning_log), 1)
            self.assertIn(unavailable_id, warning_log.description)
            self.assertIn("Successfully Processed: 1", warning_log.description)

        orders_in_odoo = self.env["sale.order"].search(order_domain)
        self.assertEqual(len(orders_in_odoo), 2)
        self.assertEqual(existing_order.state, "draft")
        self.assertFalse(existing_order.invoice_ids)
        self.assertFalse(existing_order.picking_ids)
        order = orders_in_odoo - existing_order
        self.assertEqual(order.square_order_id, paid_id)
        self.assertEqual(order.square_payment_id, "resync-payment")
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.partner_id, partner)
        self.assertEqual(order.warehouse_id, warehouse)
        self.assertAlmostEqual(order.amount_total, 30, places=2)
        self.assertEqual(order.order_line.product_id, product)
        self.assertEqual(order.order_line.product_uom_qty, 2)
        self.assertEqual(order.order_line.qty_delivered, 2)
        delivery = order.picking_ids
        self.assertEqual(len(delivery), 1)
        self.assertEqual(delivery.picking_type_code, "outgoing")
        self.assertEqual(delivery.state, "done")
        self.assertEqual(delivery.location_id, warehouse.lot_stock_id)
        self.assertEqual(delivery.move_ids.product_id, product)
        self.assertEqual(sum(delivery.move_ids.mapped("quantity")), 2)
        self.assertEqual(mapped_stock.qty_available, 3)
        self.assertEqual(other_stock.qty_available, 9)

        invoice = self.env["account.move"].search(invoice_domain)
        self.assertEqual(len(invoice), 1)
        self.assertEqual(order.invoice_ids, invoice)
        self.assertEqual(invoice.state, "posted")
        self.assertEqual(invoice.currency_id.name, "EUR")
        self.assertAlmostEqual(invoice.amount_total, 30, places=2)
        self.assertAlmostEqual(invoice.amount_residual, 0, places=2)
        self.assertIn(invoice.payment_state, ("paid", "in_payment"))
        payment = self.env["account.payment"].search(payment_domain)
        self.assertEqual(len(payment), 1)
        self.assertAlmostEqual(payment.amount, 30, places=2)
        self.assertEqual(payment.currency_id, invoice.currency_id)
        self.assertEqual(payment.payment_type, "inbound")
        self.assertEqual(payment.journal_id, journal)
        self.assertEqual(payment.move_id.state, "posted")
        invoice_receivable = invoice.line_ids.filtered(
            lambda line: line.account_type == "asset_receivable"
        )
        payment_receivable = payment.move_id.line_ids.filtered(
            lambda line: line.account_type == "asset_receivable"
        )
        self.assertTrue(invoice_receivable.full_reconcile_id)
        self.assertEqual(payment_receivable.full_reconcile_id, invoice_receivable.full_reconcile_id)

        with patch.object(SquareApiClient, "_make_api_request", side_effect=square_request):
            self._call_button(stale_wizard, "action_validate_resync")
            self.assertEqual(stale_wizard.state, "done")
            self.assertEqual((stale_wizard.processed_count, stale_wizard.error_count), (1, 0))
            self.assertEqual(
                log_model.search_count(log_domain + [("status", "=", "success")]), 1
            )

            order_unavailable = False
            repeated_scan = self._scan_resync(config)
            self.assertEqual(
                (repeated_scan.square_total, repeated_scan.odoo_total, repeated_scan.missing_total),
                (4, 2, 2),
            )
            self.assertCountEqual(
                repeated_scan.line_ids.mapped("square_order_id"), [unavailable_id, unselected_id]
            )
            self.assertEqual(
                self._call_button(repeated_scan, "action_close_wizard")["type"],
                "ir.actions.act_window_close",
            )

        self.assertEqual(self.env["sale.order"].search(order_domain), orders_in_odoo)
        self.assertEqual(order.picking_ids, delivery)
        self.assertEqual(self.env["account.move"].search(invoice_domain), invoice)
        self.assertEqual(self.env["account.payment"].search(payment_domain), payment)
        self.assertAlmostEqual(invoice.amount_total, 30, places=2)
        self.assertAlmostEqual(invoice.amount_residual, 0, places=2)
        self.assertAlmostEqual(payment.amount, 30, places=2)
        self.assertEqual(sum(delivery.move_ids.mapped("quantity")), 2)
        self.assertEqual(mapped_stock.qty_available, 3)
        self.assertEqual(other_stock.qty_available, 9)

        self.assertEqual(len(search_requests), 3)
        for request in search_requests:
            self.assertEqual(request["location_ids"], ["resync-location"])
            self.assertEqual(
                request["query"]["filter"],
                {
                    "date_time_filter": {
                        "created_at": {
                            "start_at": "2025-10-05T00:00:00Z",
                            "end_at": "2025-10-08T23:59:59Z",
                        }
                    },
                    "state_filter": {"states": ["OPEN", "COMPLETED"]},
                },
            )

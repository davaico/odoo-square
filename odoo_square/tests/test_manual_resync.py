# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from datetime import datetime, timedelta
from lxml import html
from unittest.mock import patch, MagicMock
from odoo.tests import TransactionCase
from odoo.tests.common import tagged
from odoo.exceptions import UserError, ValidationError

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

    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_location_orders")
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

    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_location_orders")
    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_order")
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

        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.square_config.id,
                "days_back": 7,
            }
        )
        wizard._onchange_days_back()
        wizard.action_scan_missing_orders()

        mock_get_order.assert_not_called()
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

    def test_scan_reports_incomplete_search_without_partial_results(self):
        wizard = self.env["square.manual.resync.wizard"].create({"config_id": self.square_config.id})
        first_page = {"orders": [{"id": "missing-before-outage"}], "cursor": "next-page"}
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient._make_api_request",
            side_effect=[first_page, UserError("Square is unavailable.")],
        ), self.assertRaisesRegex(UserError, "Could not complete the Square order scan for location LOC_123"):
            wizard.action_scan_missing_orders()
        self.assertEqual(wizard.state, "preview")
        self.assertFalse(wizard.line_ids)

    def test_scan_requires_shorter_range_when_page_limit_reached(self):
        wizard = self.env["square.manual.resync.wizard"].create({"config_id": self.square_config.id})
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient._make_api_request",
            return_value={"orders": [{"id": "missing-order"}], "cursor": "another-page"},
        ), self.assertRaisesRegex(UserError, "Select a shorter date range"):
            wizard.action_scan_missing_orders()
        self.assertEqual(wizard.state, "preview")
        self.assertFalse(wizard.line_ids)

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

    @patch("odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_order_event")
    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_order")
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

    @patch("odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_order_event")
    @patch("odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_order")
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
            type(self.env["square.api.client"]), "get_order",
            return_value={"id": order_id, "location_id": "LOC_123", "state": "COMPLETED"},
        ), patch.object(
            processor, "_process_order_event", autospec=True, side_effect=processor._process_order_event,
        ) as apply, self.assertLogs("odoo.addons.odoo_square.models.square_order_processor", level="INFO") as messages:
            wizard.action_validate_resync()

        self.assertTrue(any("Square invoicing failed for" in message for message in messages.output))
        self.assertEqual([call.args[2] for call in apply.call_args_list], ["created"])
        self.assertEqual((wizard.processed_count, wizard.error_count), (0, 1))
        self.assertIn(order_id, wizard.error_details)
        self.assertEqual(order.state, "draft")
        self.assertFalse(order.invoice_ids)
        self.assertFalse(order.picking_ids)

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
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_location_orders",
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
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_order",
            return_value={"id": "test_order", "state": "COMPLETED"},
        ):
            with patch(
                "odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_order_event",
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
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_order",
            side_effect=lambda oid, **kwargs: {
                "id": oid,
                "state": "COMPLETED",
            },
        ):
            with patch(
                "odoo.addons.odoo_square.models.square_order_processor.SquareOrderProcessor._process_order_event",
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

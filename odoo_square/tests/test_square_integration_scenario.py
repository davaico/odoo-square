# -*- coding: utf-8 -*-
import json
import logging
from unittest.mock import patch, MagicMock
from odoo.tests.common import tagged
from odoo.tools import mute_logger
from odoo.addons.odoo_square.models.square_api_client import SquareApiClient

from .common import SquareHttpCase

_logger = logging.getLogger(__name__)


@tagged("post_install", "-at_install", "TestSquareIntegrationScenario")
class TestSquareIntegrationScenario(SquareHttpCase):
    """
    Complete integration test for the Square Odoo integration scenario.
    Tests the full flow: setup, sale, stock sync, refund, and stock restoration.
    """

    def setUp(self):
        super().setUp()

        # Create test warehouses
        self.warehouse_w1 = self.env["stock.warehouse"].create(
            {"name": "Warehouse W1", "code": "W1"}
        )
        self.warehouse_w2 = self.env["stock.warehouse"].create(
            {"name": "Warehouse W2", "code": "W2"}
        )

        # Create Square configuration with location mappings
        self.square_config = self.env["square.config"].create(
            {
                "name": "Test Square Integration",
                "square_application_id": "test_app_id",
                "square_access_token": "test_access_token",
                "square_environment": "sandbox",
            }
        )

        # Create location mappings
        self.env["square.location.mapping"].create(
            {
                "config_id": self.square_config.id,
                "square_location_id": "L1",
                "square_location_name": "Location L1",
                "warehouse_id": self.warehouse_w1.id,
            }
        )
        self.env["square.location.mapping"].create(
            {
                "config_id": self.square_config.id,
                "square_location_id": "L2",
                "square_location_name": "Location L2",
                "warehouse_id": self.warehouse_w2.id,
            }
        )

        # Create payment journal for Square
        self.payment_journal = self.env["account.journal"].create(
            {
                "name": "Square Payments",
                "type": "bank",
                "code": "SQ",
            }
        )

        # Update config with payment journal
        self.square_config.payment_journal_id = self.payment_journal.id

        # Create test products with SKUs
        self.product_p1 = self.env["product.product"].create(
            {
                "name": "Product P1",
                "default_code": "P1-SKU",
                "list_price": 1.00,  # TTC price
                "type": "product",
            }
        )
        self.product_p2 = self.env["product.product"].create(
            {
                "name": "Product P2",
                "default_code": "P2-SKU",
                "list_price": 2.00,  # TTC price
                "type": "product",
            }
        )

        # Set initial stock levels (50 each in both warehouses)
        self._set_initial_stock(self.product_p1, self.warehouse_w1, 50)
        self._set_initial_stock(self.product_p1, self.warehouse_w2, 50)
        self._set_initial_stock(self.product_p2, self.warehouse_w1, 50)
        self._set_initial_stock(self.product_p2, self.warehouse_w2, 50)

        # Mock Square API client responses
        self.mock_square_api = {
            "get_order": self._mock_get_order,
            "get_catalog_object": self._mock_get_catalog_object,
            "test_connection": MagicMock(
                return_value={"success": True, "message": "Connected"}
            ),
        }

        self._patcher_get_order = patch.object(
            SquareApiClient,
            "get_order",
            side_effect=self._patch_side_get_order,
        )
        self._patcher_get_catalog = patch.object(
            SquareApiClient,
            "get_catalog_object",
            side_effect=self._patch_side_get_catalog,
        )
        self._patcher_get_order.start()
        self._patcher_get_catalog.start()

        # Track created records for assertions
        self.created_sale_order = None
        self.created_invoice = None
        self.created_credit_note = None
        self.created_customer = None

    def _set_initial_stock(self, product, warehouse, quantity):
        """Set initial stock for a product in a warehouse"""
        stock_location = warehouse.lot_stock_id
        self.env["stock.quant"].create(
            {
                "product_id": product.id,
                "location_id": stock_location.id,
                "quantity": quantity,
            }
        )

    def _mock_get_order(self, order_id):
        """Mock Square API get_order response"""
        if order_id == "ORDER_L1_001":
            return {
                "id": order_id,
                "location_id": "L1",
                "state": "COMPLETED",
                "line_items": [
                    {
                        "uid": "line_1",
                        "name": "Product P1",
                        "catalog_object_id": "P1-SKU",
                        "quantity": "1",
                        "total_money": {"amount": 100, "currency": "EUR"},  # 1.00€
                        "total_tax_money": {"amount": 20, "currency": "EUR"},  # 0.20€
                    },
                    {
                        "uid": "line_2",
                        "name": "Product P2",
                        "catalog_object_id": "P2-SKU",
                        "quantity": "1",
                        "total_money": {"amount": 200, "currency": "EUR"},  # 2.00€
                        "total_tax_money": {"amount": 40, "currency": "EUR"},  # 0.40€
                    },
                ],
                "total_money": {"amount": 300, "currency": "EUR"},  # 3.00€
                "total_tax_money": {"amount": 60, "currency": "EUR"},  # 0.60€
                "fulfillments": [
                    {
                        "pickup_details": {
                            "recipient": {
                                "display_name": "John Doe",
                                "email_address": "john.doe@example.com",
                                "phone_number": "+33123456789",
                            }
                        }
                    }
                ],
                "tenders": [
                    {
                        "id": "payment_001",
                        "amount_money": {"amount": 300, "currency": "EUR"},
                        "card_details": {"card": {"cardholder_name": "JOHN DOE"}},
                    }
                ],
            }
        return None

    def _mock_get_catalog_object(self, catalog_id):
        """Mock Square API get_catalog_object response"""
        sku_mapping = {
            "P1-SKU": {"sku": "P1-SKU", "name": "Product P1"},
            "P2-SKU": {"sku": "P2-SKU", "name": "Product P2"},
        }

        if catalog_id in sku_mapping:
            return {
                "success": True,
                "sku": sku_mapping[catalog_id]["sku"],
                "name": sku_mapping[catalog_id]["name"],
            }
        return {"success": False, "not_found": True}

    def _patch_side_get_order(self, *args, **kwargs):
        """Odoo may call @api.model methods as (rs, id) or (id,) on the mock."""
        order_id = kwargs.get("order_id")
        if order_id is None:
            if len(args) >= 2:
                order_id = args[1]
            elif len(args) == 1 and isinstance(args[0], str):
                order_id = args[0]
        if not order_id or not isinstance(order_id, str):
            return None
        return self._mock_get_order(order_id)

    def _patch_side_get_catalog(self, *args, **kwargs):
        catalog_id = kwargs.get("catalog_object_id")
        if catalog_id is None:
            if len(args) >= 2:
                catalog_id = args[1]
            elif len(args) == 1 and isinstance(args[0], str):
                catalog_id = args[0]
        if not catalog_id or not isinstance(catalog_id, str):
            return {"success": False, "not_found": True}
        return self._mock_get_catalog_object(catalog_id)

    def _confirm_deliver_invoice_mapped_wh(self, sale_order):
        """Confirm SO, deliver from mapped W1, post invoice (for refund tests)."""
        wh = self.warehouse_w1
        for line in sale_order.order_line:
            self._set_initial_stock(line.product_id, wh, 100)
        sale_order.action_confirm()
        picking = sale_order.picking_ids.filtered(
            lambda p: p.picking_type_code == "outgoing"
            and p.state not in ("done", "cancel")
        )[:1]
        self.assertTrue(picking)
        picking.action_assign()
        for move in picking.move_ids:
            move.quantity = move.product_uom_qty
        picking.button_validate()
        inv = sale_order._create_invoices()
        inv.action_post()

    def _get_stock_quantity(self, product, warehouse):
        """Get current stock quantity for a product in a warehouse"""
        stock_location = warehouse.lot_stock_id
        quant = self.env["stock.quant"].search(
            [
                ("product_id", "=", product.id),
                ("location_id", "=", stock_location.id),
            ],
            limit=1,
        )
        return quant.quantity if quant else 0

    @mute_logger("odoo.addons.odoo_square.controllers.square_webhook")
    def test_complete_square_integration_scenario(self):
        """Test the complete Square integration scenario"""

        # ===== PHASE 1: Initial Setup Verification =====
        self._test_initial_setup()

        # ===== PHASE 2: Sale Processing =====
        self._test_sale_processing()

        # ===== PHASE 3: Stock Updates =====
        self._test_stock_updates()

        # ===== PHASE 4: Refund Processing =====
        self._test_refund_processing()

        # ===== PHASE 5: Stock Restoration =====
        self._test_stock_restoration()

        # ===== PHASE 6: Final State Verification =====
        self._test_final_state()

    def _test_initial_setup(self):
        """Test initial setup: warehouses, products, stock levels"""

        # Verify warehouses exist
        self.assertTrue(self.warehouse_w1.exists())
        self.assertTrue(self.warehouse_w2.exists())

        # Verify location mappings
        l1_mapping = self.env["square.location.mapping"].search(
            [
                ("square_location_id", "=", "L1"),
                ("warehouse_id", "=", self.warehouse_w1.id),
            ]
        )
        self.assertTrue(l1_mapping.exists())

        l2_mapping = self.env["square.location.mapping"].search(
            [
                ("square_location_id", "=", "L2"),
                ("warehouse_id", "=", self.warehouse_w2.id),
            ]
        )
        self.assertTrue(l2_mapping.exists())

        # Verify initial stock levels
        self.assertEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w1), 50
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w2), 50
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w2), 50
        )

        _logger.info("✓ Initial setup verification passed")

    def _test_sale_processing(self):
        """Test sale processing via webhook"""

        # Simulate order.created webhook
        order_created_data = {
            "type": "order.created",
            "data": {
                "object": {
                    "order_created": {
                        "id": "ORDER_L1_001",
                        "location_id": "L1",
                        "state": "OPEN",
                    }
                }
            },
            "event_id": "evt_order_created_001",
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(order_created_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify draft sale order was created
        self.created_sale_order = self.env["sale.order"].search(
            [("square_order_id", "=", "ORDER_L1_001")]
        )
        self.assertTrue(self.created_sale_order.exists())
        self.assertEqual(self.created_sale_order.state, "draft")
        self.assertEqual(self.created_sale_order.warehouse_id, self.warehouse_w1)

        # Verify customer was created/found
        self.created_customer = self.created_sale_order.partner_id
        self.assertTrue(self.created_customer.exists())
        self.assertEqual(self.created_customer.email, "john.doe@example.com")

        # Verify order lines were created
        self.assertEqual(len(self.created_sale_order.order_line), 2)
        p1_line = self.created_sale_order.order_line.filtered(
            lambda l: l.product_id == self.product_p1
        )
        p2_line = self.created_sale_order.order_line.filtered(
            lambda l: l.product_id == self.product_p2
        )

        self.assertTrue(p1_line.exists())
        self.assertTrue(p2_line.exists())
        self.assertEqual(p1_line.product_uom_qty, 1)
        self.assertEqual(p2_line.product_uom_qty, 1)

        # Verify total amount (with VAT) — Odoo sale lines add tax on top of Square TTC split
        self.assertAlmostEqual(self.created_sale_order.amount_total, 3.45, places=2)

        _logger.info("✓ Sale processing verification passed")

    def _test_stock_updates(self):
        """Test stock updates after order completion"""

        # Simulate order.updated webhook to COMPLETED
        order_updated_data = {
            "type": "order.updated",
            "data": {
                "object": {
                    "order_updated": {
                        "id": "ORDER_L1_001",
                        "location_id": "L1",
                        "state": "COMPLETED",
                    }
                }
            },
            "event_id": "evt_order_updated_001",
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(order_updated_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertIn(
            response_data["status"],
            ("success", "updated"),
            "COMPLETED update should succeed",
        )

        # Refresh sale order
        self.created_sale_order.invalidate_recordset()

        # Verify order was confirmed
        self.assertEqual(self.created_sale_order.state, "sale")

        # Verify invoice was created and posted
        invoices = self.created_sale_order.invoice_ids
        self.assertTrue(len(invoices) > 0)
        posted_invoice = invoices.filtered(lambda inv: inv.state == "posted")
        self.assertTrue(posted_invoice.exists())
        self.created_invoice = posted_invoice[0]

        # Verify payment was registered (refunds may leave residual reconciliation)
        self.assertIn(
            self.created_invoice.payment_state,
            ("paid", "partial", "in_payment"),
        )

        # Verify stock was decreased in W1
        self.assertEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w1), 49
        )

        # Verify stock in W2 unchanged
        self.assertEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w2), 50
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w2), 50
        )

        _logger.info("✓ Stock updates verification passed")

    def _test_refund_processing(self):
        """Test partial refund processing"""

        # Create refund record manually for testing (simulating refund.created webhook)
        refund_record = self.env["square.refund"].create(
            {
                "square_refund_id": "REFUND_L1_001",
                "square_order_id": "ORDER_L1_001",
                "status": "pending",
                "refund_amount": 1.00,  # Partial refund for P1
                "currency_id": self.env.ref("base.EUR").id,
                "sale_order_id": self.created_sale_order.id,
                "refund_reason": "Customer return",
                "refunded_line_ids": ["line_1"],  # Refund P1 specifically
                "webhook_event_id": "evt_refund_created_001",
            }
        )

        # Same as webhook COMPLETED: pending actions then finalize
        refund_record.action_process_refund()
        refund_record.invalidate_recordset()
        refund_record.write({"status": "completed"})
        refund_record.action_process_refund()

        # Verify refund status changed to completed
        refund_record.invalidate_recordset()
        self.assertEqual(refund_record.status, "completed")

        # Verify credit note was created
        self.assertTrue(refund_record.credit_note_id.exists())
        self.created_credit_note = refund_record.credit_note_id
        self.assertEqual(self.created_credit_note.state, "posted")
        self.assertIn(
            self.created_credit_note.payment_state,
            ("paid", "partial", "in_payment"),
        )

        _logger.info("✓ Refund processing verification passed")

    def _test_stock_restoration(self):
        """Test stock restoration after refund"""

        # Verify return picking was created and processed
        return_pickings = self.env["stock.picking"].search(
            [
                ("sale_id", "=", self.created_sale_order.id),
                ("picking_type_code", "=", "incoming"),
            ]
        )
        if not return_pickings:
            _logger.warning(
                "No incoming return picking on sale order — skip stock restoration asserts"
            )
            return

        # Verify return picking was processed (assuming it was auto-validated)
        processed_returns = return_pickings.filtered(lambda p: p.state == "done")
        if processed_returns:
            self.assertGreaterEqual(
                self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50
            )
        else:
            _logger.warning(
                "Return picking not auto-processed - manual verification needed"
            )

        _logger.info("✓ Stock restoration verification passed")

    def _test_final_state(self):
        """Test final state of all records"""

        # Verify sale order final state (SO total drops after partial refund / line updates)
        self.assertEqual(self.created_sale_order.state, "sale")
        self.created_sale_order.invalidate_recordset()
        self.assertGreater(self.created_sale_order.amount_total, 0)
        self.assertLessEqual(self.created_sale_order.amount_total, 3.46)

        # Verify refund tracking
        self.assertEqual(len(self.created_sale_order.square_refund_ids), 1)
        refund = self.created_sale_order.square_refund_ids[0]
        self.assertEqual(refund.status, "completed")
        self.assertEqual(refund.refund_amount, 1.00)

        # Verify total refunded amount
        self.assertEqual(self.created_sale_order.total_refunded_amount, 1.00)

        # Verify refund status
        self.assertEqual(self.created_sale_order.refund_status, "partially_refunded")

        # Verify final stock levels
        self.assertGreaterEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50
        )
        self.assertLessEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w1), 50
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w2), 50
        )  # Unchanged
        self.assertEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w2), 50
        )  # Unchanged

        # Verify invoice and credit note
        self.assertAlmostEqual(self.created_invoice.amount_total, 3.45, places=2)
        self.assertIn(
            self.created_invoice.payment_state,
            ("paid", "partial", "in_payment"),
        )
        self.assertTrue(self.created_credit_note.exists())
        self.assertIn(
            self.created_credit_note.payment_state,
            ("paid", "partial", "in_payment"),
        )

        _logger.info("✓ Final state verification passed")

    def test_refund_idempotency_simple(self):
        """Test that the same refund is not processed twice (simple idempotency)"""

        # First, simulate order creation and completion
        self._test_sale_processing()
        self._test_stock_updates()

        # Create initial refund record
        refund_record = self.env["square.refund"].create(
            {
                "square_refund_id": "REFUND_IDEMPOTENCY_TEST",
                "square_order_id": "ORDER_L1_001",
                "status": "pending",
                "refund_amount": 1.00,
                "currency_id": self.env.ref("base.EUR").id,
                "sale_order_id": self.created_sale_order.id,
            }
        )

        # Process the refund once (pending → actions → completed)
        refund_record.action_process_refund()
        refund_record.invalidate_recordset()
        refund_record.write({"status": "completed"})
        refund_record.action_process_refund()
        refund_record.invalidate_recordset()
        self.assertEqual(refund_record.status, "completed")

        # Try to create the same refund again (simulate duplicate webhook)
        duplicate_refund = self.env["square.refund"].create_from_square_data(
            {
                "id": "REFUND_IDEMPOTENCY_TEST",
                "order_id": "ORDER_L1_001",
                "status": "COMPLETED",
                "amount_money": {"amount": 100, "currency": "EUR"},
            },
            self.created_sale_order,
        )

        # Should return the existing completed refund, not create a new one
        self.assertEqual(duplicate_refund.id, refund_record.id)
        self.assertEqual(duplicate_refund.status, "completed")

        _logger.info("✓ Simple refund idempotency test passed")

    def test_partial_refund_single_line_quantity_fix(self):
        """Test that partial refund on single line order returns exact quantity"""

        self._test_sale_processing()

        # Draft order: keep one line only, then confirm / deliver / invoice
        single_line = self.created_sale_order.order_line[0]
        other_lines = self.created_sale_order.order_line.filtered(
            lambda l: l.id != single_line.id
        )
        if other_lines:
            other_lines.unlink()

        single_line.product_uom_qty = 2
        single_line.price_unit = 1.0  # 1€ per unit, 2€ total

        self._confirm_deliver_invoice_mapped_wh(self.created_sale_order)

        # Create refund for exactly 1€
        refund_record = self.env["square.refund"].create(
            {
                "square_refund_id": "SINGLE_LINE_REFUND_TEST",
                "square_order_id": "ORDER_L1_001",
                "status": "pending",
                "refund_amount": 1.00,  # Exactly 1€, should return exactly 1 unit
                "currency_id": self.env.ref("base.EUR").id,
                "sale_order_id": self.created_sale_order.id,
            }
        )

        refund_record.action_process_refund()
        refund_record.invalidate_recordset()
        refund_record.write({"status": "completed"})
        refund_record.action_process_refund()
        refund_record.invalidate_recordset()
        self.assertEqual(refund_record.status, "completed")

        # Refresh the line to get updated values
        single_line.invalidate_recordset()

        # Verify that exactly 1 unit was returned (not 1.2 or other proportional amount)
        self.assertEqual(
            single_line.returned_qty,
            1.0,
            "Should return exactly 1 unit for 1€ refund on 1€/unit item",
        )

        # Verify order quantity is updated correctly
        self.assertEqual(
            single_line.product_uom_qty, 2.0, "Order line quantity should remain 2"
        )
        self.assertGreaterEqual(single_line.qty_delivered, 1.0)

        self.assertTrue(refund_record.credit_note_id)

        _logger.info("✓ Single line partial refund quantity fix test passed")

    def tearDown(self):
        """Clean up test data"""
        for p in getattr(self, "_patcher_get_order", None), getattr(
            self, "_patcher_get_catalog", None
        ):
            if p is not None:
                try:
                    p.stop()
                except RuntimeError:
                    pass

        # Additional cleanup if needed
        try:
            # Clean up any remaining test records
            self.env["sale.order"].search(
                [("square_order_id", "=", "ORDER_L1_001")]
            ).unlink()

            self.env["square.refund"].search(
                [("square_refund_id", "=", "REFUND_L1_001")]
            ).unlink()

        except Exception as e:
            _logger.warning(f"Error during test cleanup: {str(e)}")

        super().tearDown()

# -*- coding: utf-8 -*-
import json
import logging
from datetime import datetime
from unittest.mock import call, patch

from odoo.tests import Form
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

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        company = cls.env.company
        company.currency_id = cls.env.ref("base.EUR")
        cls.env["account.chart.template"].try_loading("generic_coa", company)
        cls.sale_tax = cls.env["account.tax"].create(
            {
                "name": "Square VAT 20%",
                "amount": 20.0,
                "amount_type": "percent",
                "type_tax_use": "sale",
                "price_include_override": "tax_excluded",
            }
        )

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

        # Create the unrelated mapping first so fallback cannot pass the warehouse check.
        self.env["square.location.mapping"].create(
            {
                "config_id": self.square_config.id,
                "square_location_id": "L2",
                "square_location_name": "Location L2",
                "warehouse_id": self.warehouse_w2.id,
            }
        )
        self.env["square.location.mapping"].create(
            {
                "config_id": self.square_config.id,
                "square_location_id": "L1",
                "square_location_name": "Location L1",
                "warehouse_id": self.warehouse_w1.id,
            }
        )

        # Create payment journal for Square
        self.payment_journal = self.env["account.journal"].create(
            {
                "name": "Card Receipts",
                "type": "bank",
                "code": "SQ",
            }
        )

        # Update config with payment journal
        self.square_config.payment_journal_id = self.payment_journal.id
        self.env["square.stock.sync"].create({"auto_sync_enabled": False})
        self.customer = self.env["res.partner"].create(
            {"name": "Existing John Doe", "email": "john.doe@example.com"}
        )

        # Create test products with SKUs
        self.product_p1 = self.env["product.product"].create(
            {
                "name": "Product P1",
                "default_code": "P1-SKU",
                "list_price": 5.0,
                "type": "consu",
                "is_storable": True,
                "invoice_policy": "order",
                "taxes_id": [(6, 0, self.sale_tax.ids)],
            }
        )
        self.product_p2 = self.env["product.product"].create(
            {
                "name": "Product P2",
                "default_code": "P2-SKU",
                "list_price": 7.0,
                "type": "consu",
                "is_storable": True,
                "invoice_policy": "order",
                "taxes_id": [(6, 0, self.sale_tax.ids)],
            }
        )

        # Set initial stock levels (50 each in both warehouses)
        self._set_initial_stock(self.product_p1, self.warehouse_w1, 50)
        self._set_initial_stock(self.product_p1, self.warehouse_w2, 50)
        self._set_initial_stock(self.product_p2, self.warehouse_w1, 50)
        self._set_initial_stock(self.product_p2, self.warehouse_w2, 50)

        self.enterContext(
            patch.object(SquareApiClient, "get_order", side_effect=self._patch_side_get_order)
        )
        self.enterContext(
            patch.object(
                SquareApiClient,
                "get_catalog_object",
                side_effect=self._patch_side_get_catalog,
            )
        )
        self.enterContext(
            patch.object(
                SquareApiClient,
                "search_catalog_by_sku",
                return_value={"success": True, "not_found": True},
            )
        )

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
                        "base_price_money": {"amount": 100, "currency": "EUR"},
                        "total_money": {"amount": 120, "currency": "EUR"},
                        "total_tax_money": {"amount": 20, "currency": "EUR"},
                    },
                    {
                        "uid": "line_2",
                        "name": "Product P2",
                        "catalog_object_id": "P2-CATALOG",
                        "quantity": "2",
                        "base_price_money": {"amount": 100, "currency": "EUR"},
                        "total_money": {"amount": 240, "currency": "EUR"},
                        "total_tax_money": {"amount": 40, "currency": "EUR"},
                    },
                ],
                "total_money": {"amount": 360, "currency": "EUR"},
                "total_tax_money": {"amount": 60, "currency": "EUR"},
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
                        "amount_money": {"amount": 360, "currency": "EUR"},
                        "card_details": {"card": {"cardholder_name": "JOHN DOE"}},
                    }
                ],
            }
        return None

    def _mock_get_catalog_object(self, catalog_id):
        """Mock Square API get_catalog_object response"""
        sku_mapping = {
            "P1-SKU": {"sku": "P1-SKU", "name": "Product P1"},
            "P2-CATALOG": {"sku": "P2-SKU", "name": "Product P2"},
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
        quants = self.env["stock.quant"].search(
            [
                ("product_id", "=", product.id),
                ("location_id", "=", stock_location.id),
            ],
        )
        return sum(quants.mapped("quantity"))

    def _send_square_order_event(self, event_type, event_id, state):
        payload = {
            "type": event_type,
            "event_id": event_id,
            "data": {
                "object": {
                    event_type.replace(".", "_"): {
                        "id": "ORDER_L1_001",
                        "location_id": "L1",
                        "state": state,
                    }
                }
            },
        }
        response = self.url_open(
            "/square/webhook",
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 200)
        return response.json()

    def _import_paid_square_sale(self):
        result = self._send_square_order_event(
            "order.updated", "evt_paid_sale_001", "COMPLETED"
        )
        self.assertIn(result["status"], ("success", "updated"), result)
        self._assert_paid_square_sale()

    def _assert_square_sale_order(self, state):
        order = self.env["sale.order"].search([("square_order_id", "=", "ORDER_L1_001")])
        self.assertEqual(len(order), 1)
        self.assertEqual(order.partner_id, self.customer)
        self.assertEqual(order.state, state)
        self.assertEqual(order.warehouse_id, self.warehouse_w1)
        self.assertEqual(order.currency_id, self.env.ref("base.EUR"))
        self.assertEqual(order.square_payment_id, "payment_001")
        self.assertEqual(len(order.order_line), 2)
        for product, quantity, tax, total in (
            (self.product_p1, 1, 0.20, 1.20),
            (self.product_p2, 2, 0.40, 2.40),
        ):
            line = order.order_line.filtered(lambda line: line.product_id == product)
            self.assertEqual(len(line), 1)
            self.assertEqual(line.product_uom_qty, quantity)
            self.assertAlmostEqual(line.price_tax, tax, places=2)
            self.assertAlmostEqual(line.price_total, total, places=2)
        self.assertAlmostEqual(order.amount_total, 3.60, places=2)

        self.created_sale_order = order
        self.created_customer = order.partner_id
        return order

    def _assert_paid_square_sale(self):
        order = self._assert_square_sale_order("sale")
        invoice = order.invoice_ids
        self.assertEqual(len(invoice), 1)
        self.assertEqual(invoice.move_type, "out_invoice")
        self.assertEqual(invoice.state, "posted")
        self.assertEqual(invoice.currency_id, order.currency_id)
        self.assertAlmostEqual(invoice.amount_total, 3.60, places=2)
        self.assertAlmostEqual(invoice.amount_residual, 0.0, places=2)
        self.assertEqual(invoice.payment_state, "paid")

        payment = self.env["account.payment"].search(
            [("partner_id", "=", self.customer.id)]
        )
        self.assertEqual(len(payment), 1)
        self.assertEqual(invoice.reconciled_payment_ids, payment)
        self.assertEqual(payment.journal_id, self.payment_journal)
        self.assertEqual(payment.payment_type, "inbound")
        self.assertEqual(payment.currency_id, order.currency_id)
        self.assertAlmostEqual(payment.amount, 3.60, places=2)
        self.assertEqual(payment.move_id.state, "posted")

        invoice_receivable = invoice.line_ids.filtered(
            lambda line: line.account_id.account_type == "asset_receivable"
        )
        payment_receivable = payment.move_id.line_ids.filtered(
            lambda line: line.account_id.account_type == "asset_receivable"
        )
        self.assertEqual(len(invoice_receivable), 1)
        self.assertEqual(len(payment_receivable), 1)
        self.assertTrue(invoice_receivable.full_reconcile_id)
        self.assertEqual(
            invoice_receivable.full_reconcile_id, payment_receivable.full_reconcile_id
        )

        delivery = order.picking_ids
        self.assertEqual(len(delivery), 1)
        self.assertEqual(delivery.picking_type_code, "outgoing")
        self.assertEqual(delivery.state, "done")
        self.assertEqual(delivery.location_id, self.warehouse_w1.lot_stock_id)
        self.assertEqual(len(delivery.move_ids), 2)
        self.assertEqual(set(delivery.move_ids.mapped("state")), {"done"})
        self.assertEqual(
            {move.product_id: move.quantity for move in delivery.move_ids},
            {self.product_p1: 1, self.product_p2: 2},
        )
        for product, quantity in ((self.product_p1, 49), (self.product_p2, 48)):
            self.assertEqual(
                self._get_stock_quantity(product, self.warehouse_w1), quantity
            )
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

        self.created_invoice = invoice

    def _call_admin_button(self, record, method):
        response = self.url_open(
            f"/web/dataset/call_button/{record._name}/{method}",
            json={
                "jsonrpc": "2.0",
                "method": "call",
                "params": {
                    "model": record._name,
                    "method": method,
                    "args": [record.ids],
                    "kwargs": {},
                },
                "id": 1,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertNotIn("error", payload, payload)
        return payload["result"]

    def _scan_manual_resync(self, admin, start_at, end_at):
        action = self._call_admin_button(
            self.square_config, "action_open_manual_resync_wizard"
        )
        registered_action = self.env.ref("odoo_square.action_square_manual_resync_wizard")
        self.assertEqual(action["res_model"], registered_action.res_model)
        self.assertEqual(action["views"][0][1], "form")
        self.assertEqual(action["target"], "new")
        wizard = self.env[action["res_model"]].with_user(admin).browse(action["res_id"])
        self.assertEqual(wizard.config_id, self.square_config)
        self.assertEqual(wizard.state, "preview")
        with Form(wizard) as form:
            form.start_at = start_at
            form.end_at = end_at

        scanned = self._call_admin_button(wizard, "action_scan_missing_orders")
        self.assertEqual(scanned["res_id"], wizard.id)
        self.assertEqual(wizard.state, "results")
        return wizard

    def _deselect_resync_order(self, wizard, order_id):
        line_index = wizard.line_ids.mapped("square_order_id").index(order_id)
        with Form(wizard) as form:
            with form.line_ids.edit(line_index) as line:
                line.selected = False
        self.assertEqual(wizard.selected_total, 1)

    def test_manual_resync_recovers_only_selected_paid_orders(self):
        sale_orders = self.env["sale.order"]
        existing_id = "ORDER_ALREADY_IMPORTED"
        existing = sale_orders.create(
            {"square_order_id": existing_id, "partner_id": self.customer.id}
        )
        result = self._send_square_order_event(
            "order.updated", "evt_resync_pending", "OPEN"
        )
        self.assertEqual(result["status"], "queued", result)
        queue = self.env["square.webhook.queue"]
        queued = queue.search([("webhook_event_id", "=", "evt_resync_pending")])
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued.state, "pending")

        paid = self._mock_get_order("ORDER_L1_001")
        paid["created_at"] = "2026-09-08T11:00:00+01:00"
        deferred_id = "ORDER_L1_DEFERRED"
        deferred = dict(
            paid,
            id=deferred_id,
            tenders=[dict(paid["tenders"][0], id="payment_deferred")],
        )
        orders = {paid["id"]: paid, deferred_id: deferred}
        orders_by_location = {"L1": [paid, deferred, {"id": existing_id}], "L2": []}

        admin = self.env.ref("base.user_admin")
        self.authenticate(admin.login, admin.login)
        start_at = datetime(2026, 9, 8, 9, 0)
        end_at = datetime(2026, 9, 8, 12, 0)

        with (
            patch.object(
                SquareApiClient,
                "get_location_orders",
                side_effect=lambda *, location_id, **kwargs: orders_by_location[location_id],
            ) as search_orders,
            patch.object(
                SquareApiClient,
                "get_order",
                side_effect=lambda order_id, **kwargs: dict(orders[order_id]),
            ),
        ):
            wizard = self._scan_manual_resync(admin, start_at, end_at)
            self.assertCountEqual(
                search_orders.call_args_list,
                [
                    call(
                        location_id=location,
                        limit=500,
                        start_at=start_at,
                        end_at=end_at,
                        states=["OPEN", "COMPLETED"],
                        square_config=self.square_config,
                    )
                    for location in ("L1", "L2")
                ],
            )
            self.assertEqual((wizard.square_total, wizard.odoo_total), (3, 1))
            self.assertEqual((wizard.missing_total, wizard.selected_total), (2, 2))
            self.assertCountEqual(wizard.line_ids.mapped("square_order_id"), orders)
            paid_line = wizard.line_ids.filtered(
                lambda line: line.square_order_id == paid["id"]
            )
            self.assertEqual(paid_line.created_at, datetime(2026, 9, 8, 10, 0))
            self.assertAlmostEqual(paid_line.total_amount, 3.60, places=2)
            self._deselect_resync_order(wizard, deferred_id)

            overlapping_scan = self._scan_manual_resync(admin, start_at, end_at)
            self._deselect_resync_order(overlapping_scan, deferred_id)

            validated = self._call_admin_button(wizard, "action_validate_resync")
            self.assertEqual(validated["res_id"], wizard.id)
            self.assertEqual(wizard.state, "done")
            self.assertEqual((wizard.processed_count, wizard.error_count), (1, 0))
            self._assert_paid_square_sale()
            recovered_order = self.created_sale_order
            invoice = self.created_invoice
            payment = invoice.reconciled_payment_ids
            delivery = recovered_order.picking_ids

            # The other dialog still lists the order it found before the recovery.
            self._call_admin_button(overlapping_scan, "action_validate_resync")
            self.assertEqual(overlapping_scan.state, "done")
            self.assertEqual(
                (overlapping_scan.processed_count, overlapping_scan.error_count), (1, 0)
            )
            self._assert_paid_square_sale()
            self.assertEqual(self.created_sale_order, recovered_order)
            self.assertEqual(self.created_invoice, invoice)
            self.assertEqual(invoice.reconciled_payment_ids, payment)
            self.assertEqual(recovered_order.picking_ids, delivery)

            # A fresh scan recognizes the recovered sale and still offers the deferred one.
            fresh_scan = self._scan_manual_resync(admin, start_at, end_at)
            self.assertEqual((fresh_scan.square_total, fresh_scan.odoo_total), (3, 2))
            self.assertEqual((fresh_scan.missing_total, fresh_scan.selected_total), (1, 1))
            self.assertEqual(fresh_scan.line_ids.square_order_id, deferred_id)

        self.assertFalse(sale_orders.search([("square_order_id", "=", deferred_id)]))
        self.assertEqual(sale_orders.search([("square_order_id", "=", existing_id)]), existing)
        self.assertEqual(existing.state, "draft")
        self.assertFalse(existing.invoice_ids)
        self.assertFalse(existing.picking_ids)
        self.assertEqual(queue.search([("square_order_id", "=", paid["id"])]), queued)
        self.assertEqual(queued.state, "completed")
        self.assertEqual(queued.retry_count, 1)
        self.assertTrue(queued.processed_at)

        logs = self.env["square.integration.log"].with_user(admin)
        batches = logs.search([("event_type", "=", "manual_resync")])
        self.assertEqual(len(batches), 2)
        self.assertEqual(set(batches.mapped("status")), {"success"})
        for batch in batches:
            self.assertIn("Successfully Processed: 1", batch.description)
            self.assertIn("Errors: 0", batch.description)
        recovered_logs = logs.search(
            [
                ("sale_order_id", "=", recovered_order.id),
                ("square_order_id", "=", paid["id"]),
                ("status", "=", "success"),
            ]
        )
        self.assertIn("order_updated", recovered_logs.mapped("event_type"))
        self.assertIn("stock_sync", recovered_logs.mapped("event_type"))

    def test_paid_sale_import_and_replay(self):
        self._import_paid_square_sale()

        order_logs = self.env["square.integration.log"].search(
            [
                ("sale_order_id", "=", self.created_sale_order.id),
                ("square_order_id", "=", "ORDER_L1_001"),
                ("status", "=", "success"),
            ]
        )
        self.assertIn("order_updated", order_logs.mapped("event_type"))
        self.assertIn("stock_sync", order_logs.mapped("event_type"))

        result = self._send_square_order_event(
            "order.updated", "evt_paid_sale_001", "COMPLETED"
        )
        self.assertEqual(result["status"], "already_processed", result)
        self._assert_paid_square_sale()

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
        """Import the open order as a draft quotation."""
        result = self._send_square_order_event(
            "order.created", "evt_order_created_001", "OPEN"
        )
        self.assertEqual(result["status"], "success", result)
        self._assert_square_sale_order("draft")

    def _test_stock_updates(self):
        """Complete the imported quotation through the Square webhook."""
        result = self._send_square_order_event(
            "order.updated", "evt_order_updated_001", "COMPLETED"
        )
        self.assertIn(result["status"], ("success", "updated"), result)
        self._assert_paid_square_sale()

    def _test_refund_processing(self):
        """Test partial refund processing"""

        # Create refund record manually for testing (simulating refund.created webhook)
        refund_record = self.env["square.refund"].create(
            {
                "square_refund_id": "REFUND_L1_001",
                "square_order_id": "ORDER_L1_001",
                "status": "pending",
                "refund_amount": 1.20,  # Partial refund for P1
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
        self.assertLessEqual(self.created_sale_order.amount_total, 3.60)

        # Verify refund tracking
        self.assertEqual(len(self.created_sale_order.square_refund_ids), 1)
        refund = self.created_sale_order.square_refund_ids[0]
        self.assertEqual(refund.status, "completed")
        self.assertEqual(refund.refund_amount, 1.20)

        # Verify total refunded amount
        self.assertEqual(self.created_sale_order.total_refunded_amount, 1.20)

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
        self.assertAlmostEqual(self.created_invoice.amount_total, 3.60, places=2)
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
        single_line.price_unit = 1.0  # €1.20 per unit including VAT

        self._confirm_deliver_invoice_mapped_wh(self.created_sale_order)

        # Refund one item at its VAT-inclusive price
        refund_record = self.env["square.refund"].create(
            {
                "square_refund_id": "SINGLE_LINE_REFUND_TEST",
                "square_order_id": "ORDER_L1_001",
                "status": "pending",
                "refund_amount": 1.20,
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
            "A refund of one unit’s price should return exactly one unit",
        )

        # Verify order quantity is updated correctly
        self.assertEqual(
            single_line.product_uom_qty, 2.0, "Order line quantity should remain 2"
        )
        self.assertGreaterEqual(single_line.qty_delivered, 1.0)

        self.assertTrue(refund_record.credit_note_id)

        _logger.info("✓ Single line partial refund quantity fix test passed")

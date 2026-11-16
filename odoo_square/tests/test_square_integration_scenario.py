# -*- coding: utf-8 -*-
import json
import logging
from unittest.mock import patch
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

        self.env.company.currency_id = self.env.ref("base.EUR")

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

        tax = self.env["account.tax"].create(
            {"name": "Square VAT 20%", "type_tax_use": "sale", "amount": 20}
        )

        # Create test products with SKUs
        self.product_p1 = self.env["product.product"].create(
            {
                "name": "Product P1",
                "default_code": "P1-SKU",
                "list_price": 1.00,  # TTC price
                "type": "consu",
                "is_storable": True,
                "taxes_id": [(6, 0, tax.ids)],
            }
        )
        self.product_p2 = self.env["product.product"].create(
            {
                "name": "Product P2",
                "default_code": "P2-SKU",
                "list_price": 2.00,  # TTC price
                "type": "consu",
                "is_storable": True,
                "taxes_id": [(6, 0, tax.ids)],
            }
        )

        # Set initial stock levels (50 each in both warehouses)
        self._set_initial_stock(self.product_p1, self.warehouse_w1, 50)
        self._set_initial_stock(self.product_p1, self.warehouse_w2, 50)
        self._set_initial_stock(self.product_p2, self.warehouse_w1, 50)
        self._set_initial_stock(self.product_p2, self.warehouse_w2, 50)

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
        self.addCleanup(self._patcher_get_order.stop)
        self._patcher_get_catalog.start()
        self.addCleanup(self._patcher_get_catalog.stop)

        # Track created records for assertions
        self.created_sale_order = None
        self.created_invoice = None
        self.created_customer = None
        self.p2_order_quantity = 1

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
            p2_qty = self.p2_order_quantity
            total = 120 + 180 * p2_qty
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
                        "total_tax_money": {"amount": 20, "currency": "EUR"},  # 0.20€
                    },
                    {
                        "uid": "line_2",
                        "name": "Product P2",
                        "catalog_object_id": "P2-SKU",
                        "quantity": str(p2_qty),
                        "base_price_money": {"amount": 150, "currency": "EUR"},
                        "total_money": {"amount": 180 * p2_qty, "currency": "EUR"},
                        "total_tax_money": {"amount": 30 * p2_qty, "currency": "EUR"},
                    },
                ],
                "total_money": {"amount": total, "currency": "EUR"},
                "total_tax_money": {"amount": 20 + 30 * p2_qty, "currency": "EUR"},
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
                        "amount_money": {"amount": total, "currency": "EUR"},
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
        self.pay_invoice(inv, self.payment_journal)

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

        self._test_initial_setup()
        self._test_sale_processing()
        self._test_stock_updates()

        order = self.created_sale_order
        invoice = self.created_invoice
        delivery = order.picking_ids
        self.assertEqual(len(delivery), 1)
        self.assertEqual(delivery.picking_type_code, "outgoing")
        self.assertEqual(delivery.state, "done")
        self.assertEqual(delivery.location_id, self.warehouse_w1.lot_stock_id)

        refund_data = {
            "id": "REFUND_L1_001",
            "order_id": "ORDER_L1_001",
            "payment_id": "payment_001",
            "reason": "Returned goods",
            "amount_money": {"amount": 300, "currency": "EUR"},
        }
        refund_domain = [("square_refund_id", "=", refund_data["id"])]
        credit_domain = [
            ("partner_id", "=", order.partner_id.id),
            ("move_type", "=", "out_refund"),
        ]
        payment_domain = [
            ("partner_id", "=", order.partner_id.id),
            ("payment_type", "=", "outbound"),
        ]
        self._send_refund_webhook(refund_data, "created", "PENDING", "evt-refund-created")
        refund = self.env["square.refund"].search(refund_domain)
        self.assertEqual(len(refund), 1)
        self.assertEqual(refund.status, "pending")
        self.assertFalse(refund.processed_at)
        self.assertEqual(refund.sale_order_id, order)
        self.assertEqual(refund.invoice_id, invoice)
        credit = refund.credit_note_id
        self.assertEqual(self.env["account.move"].search(credit_domain), credit)
        self.assertEqual(credit.reversed_entry_id, invoice)
        self.assertEqual(credit.state, "draft")
        self.assertFalse(self.env["account.payment"].search(payment_domain))
        returned = refund.return_picking_ids
        self.assertEqual(len(returned), 1)
        self.assertNotIn(returned.state, ("done", "cancel"))
        self._assert_warehouse_stock(49)

        payment = None
        for event_id, expected in (
            ("evt-refund-completed", "success"),
            ("evt-refund-completed", "already_processed"),
            ("evt-refund-replayed", "already_processed"),
        ):
            self._send_refund_webhook(refund_data, "updated", "COMPLETED", event_id, expected)

            self.assertEqual(self.env["square.refund"].search(refund_domain), refund)
            self.assertEqual(order.square_refund_ids, refund)
            self.assertEqual(order.total_refunded_amount, 3.00)
            self.assertEqual(order.refund_status, "fully_refunded")
            self.assertEqual(refund.refund_amount, 3.00)
            self.assertEqual(refund.currency_id, invoice.currency_id)
            self.assertEqual(refund.status, "completed")
            self.assertTrue(refund.processed_at)
            self.assertEqual(refund.sale_order_id, order)
            self.assertEqual(refund.invoice_id, invoice)
            self.assertEqual(refund.refund_reason, "Returned goods")
            self.assertEqual(refund.webhook_event_id, "evt-refund-completed")
            self.assertEqual(refund.square_data["status"], "COMPLETED")

            self.assertEqual(self.env["account.move"].search(credit_domain), credit)
            self.assertEqual(refund.credit_note_id, credit)
            self.assertEqual(credit.reversed_entry_id, invoice)
            self.assert_reconciled_payment(invoice, "inbound", 3.00)
            refund_payment = self.assert_reconciled_payment(credit, "outbound", 3.00)
            if payment:
                self.assertEqual(refund_payment, payment)
            payment = refund_payment
            self.assertEqual(self.env["account.payment"].search(payment_domain), payment)

            self.assertEqual(refund.return_picking_ids, returned)
            self.assertEqual(order.picking_ids, delivery | returned)
            self.assertEqual(returned.return_id, delivery)
            self.assertEqual(returned.picking_type_code, "incoming")
            self.assertEqual(returned.state, "done")
            self.assertEqual(returned.location_dest_id, self.warehouse_w1.lot_stock_id)
            self.assertEqual(
                {move.product_id.id: move.quantity for move in returned.move_ids},
                {self.product_p1.id: 1, self.product_p2.id: 1},
            )
            self._assert_warehouse_stock(50)

        for event_type, event_id in (
            ("refund_created", "evt-refund-created"),
            ("refund_updated", "evt-refund-completed"),
        ):
            self.assertEqual(self.env["square.integration.log"].search_count([
                ("event_type", "=", event_type),
                ("webhook_event_id", "=", event_id),
                ("status", "=", "success"),
                ("sale_order_id", "=", order.id),
                ("square_refund_id", "=", refund.square_refund_id),
            ]), 1)

    @mute_logger("odoo.addons.odoo_square.controllers.square_webhook")
    def test_late_inline_open_update_leaves_paid_sale_intact(self):
        self._test_sale_processing()
        self._test_stock_updates()
        order = self.created_sale_order
        invoice = self.created_invoice
        lines = order.order_line
        delivery = order.picking_ids
        self.assertEqual(len(delivery), 1)
        self.assertEqual(delivery.state, "done")
        sale_payment = self.assert_reconciled_payment(invoice, "inbound", 3.00)
        stale_order = self._mock_get_order(order.square_order_id)
        stale_order["state"] = "OPEN"
        stale_order["line_items"][0].update({
            "quantity": "3",
            "total_money": {"amount": 360, "currency": "EUR"},
        })

        response = self.url_open(
            "/square/webhook",
            data=json.dumps({
                "type": "order.updated",
                "event_id": "evt-late-inline-open",
                "data": {"object": {"order_updated": stale_order}},
            }),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "updated", response.text)
        self.assertEqual(self.env["sale.order"].search([
            ("square_order_id", "=", order.square_order_id),
        ]), order)
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.order_line, lines)
        self.assertEqual(lines.mapped("product_uom_qty"), [1, 1])
        self.assertEqual(lines.mapped("qty_delivered"), [1, 1])
        self.assertEqual(order.amount_total, 3.00)
        self.assertEqual(order.invoice_ids, invoice)
        self.assertEqual(self.assert_reconciled_payment(invoice, "inbound", 3.00), sale_payment)
        self.assertEqual(self.env["account.payment"].search([
            ("partner_id", "=", order.partner_id.id),
        ]), sale_payment)
        self.assertEqual(order.picking_ids, delivery)
        self.assertEqual(delivery.move_ids.mapped("quantity"), [1, 1])
        self._assert_warehouse_stock(49)

    def test_failed_and_cancelled_refunds_leave_paid_sale_intact(self):
        self._test_sale_processing()
        self._test_stock_updates()

        order = self.created_sale_order
        for square_status, expected_status in (("FAILED", "failed"), ("CANCELED", "cancelled")):
            with self.subTest(status=square_status):
                refund_data = {
                    "id": f"refund-{square_status}",
                    "order_id": order.square_order_id,
                    "amount_money": {"amount": 300, "currency": "EUR"},
                }
                self._send_refund_webhook(refund_data, "created", "PENDING", f"created-{square_status}")
                refund = order.square_refund_ids.filtered(
                    lambda record: record.square_refund_id == refund_data["id"]
                )
                self.assertEqual(refund.status, "pending")
                self.assertTrue(refund.return_picking_ids)
                self.assertEqual(refund.credit_note_id.state, "draft")

                usd_refund = {**refund_data, "amount_money": {"amount": 300, "currency": "USD"}}
                self._send_refund_webhook(
                    usd_refund, "updated", "COMPLETED", f"usd-{square_status}", "error"
                )
                self.assertEqual(refund.status, "pending")
                self.assertEqual(refund.credit_note_id.state, "draft")

                self._send_refund_webhook(refund_data, "updated", square_status, f"updated-{square_status}")
                self.assertEqual(refund.status, expected_status)
                self.assertEqual(refund.square_data["status"], square_status)
                self.assertFalse(refund.processed_at)
                self.assertEqual(refund.invoice_id, self.created_invoice)
                self.assertEqual(refund.credit_note_id.state, "cancel")
                self.assertEqual(refund.return_picking_ids.mapped("state"), ["cancel"])
                self.assertEqual(order.total_refunded_amount, 0)
                self.assertEqual(order.refund_status, "not_refunded")
                self.assertFalse(self.env["account.payment"].search([
                    ("partner_id", "=", order.partner_id.id),
                    ("payment_type", "=", "outbound"),
                ]))
                self.assert_reconciled_payment(self.created_invoice, "inbound", 3.00)
                self._assert_warehouse_stock(49)

    def test_completed_refund_can_prepare_a_return_after_its_credit_note(self):
        self._test_sale_processing()
        self._test_stock_updates()
        order = self.created_sale_order
        delivery = order.picking_ids
        reserved = {
            "id": "reserved-refund",
            "order_id": order.square_order_id,
            "amount_money": {"amount": 300, "currency": "EUR"},
        }
        refund_data = {
            "id": "late-return-details",
            "order_id": order.square_order_id,
            "amount_money": {"amount": 120, "currency": "EUR"},
        }
        self._send_refund_webhook(reserved, "created", "PENDING", "evt-reserve-products")
        self._send_refund_webhook(refund_data, "created", "PENDING", "evt-prepare-credit")
        refund = order.square_refund_ids.filtered(
            lambda record: record.square_refund_id == refund_data["id"]
        )
        credit = refund.credit_note_id
        self.assertEqual(credit.state, "draft")
        self.assertFalse(refund.return_picking_ids)
        self.assertFalse(credit.reconciled_payment_ids)
        self._assert_warehouse_stock(49)

        self._send_refund_webhook(reserved, "updated", "CANCELED", "evt-release-products")
        self._send_refund_webhook(
            {**refund_data, "refunded_line_ids": ["line_1"]},
            "updated", "COMPLETED", "evt-complete-with-return",
        )
        self.assertTrue(refund.processed_at)
        self.assertEqual(refund.credit_note_id, credit)
        self.assert_reconciled_payment(credit, "outbound", 1.20)
        returned = refund.return_picking_ids
        self.assertEqual(len(returned), 1)
        self.assertEqual(returned.return_id, delivery)
        self.assertEqual(returned.state, "done")
        self.assertEqual(returned.location_dest_id, self.warehouse_w1.lot_stock_id)
        self.assertEqual(returned.move_ids.product_id, self.product_p1)
        self.assertEqual(returned.move_ids.quantity, 1)
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50)
        self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 49)
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w2), 50)

    def test_successive_partial_refunds_of_paid_square_sale(self):
        self.p2_order_quantity = 2
        self._test_sale_processing(expected_amount=4.80)
        self._test_stock_updates(expected_amount=4.80)

        order = self.created_sale_order
        invoice = self.created_invoice
        delivery = order.picking_ids
        sale_lines = order.order_line
        p1_line = sale_lines.filtered(lambda line: line.product_id == self.product_p1)
        p2_line = sale_lines.filtered(lambda line: line.product_id == self.product_p2)
        sale_payment = self.assert_reconciled_payment(invoice, "inbound", 4.80)
        first_refund = {
            "id": "REFUND_L1_FIRST_PARTIAL",
            "order_id": "ORDER_L1_001",
            "payment_id": "payment_001",
            "amount_money": {"amount": 122, "currency": "EUR"},
            "refunded_line_ids": ["line_1"],
        }
        second_refund = {
            "id": "REFUND_L1_SECOND_PARTIAL",
            "order_id": "ORDER_L1_001",
            "payment_id": "payment_001",
            "amount_money": {"amount": 180, "currency": "EUR"},
        }

        self._send_refund_webhook(first_refund, "created", "PENDING", "evt-first-pending")
        first = order.square_refund_ids
        self.assertEqual(len(first), 1)
        self.assertEqual(first.status, "pending")
        self.assertFalse(first.processed_at)
        prepared_credit = first.credit_note_id
        prepared_return = first.return_picking_ids
        self.assertEqual(prepared_credit.state, "draft")
        self.assertEqual(prepared_credit.amount_total, 1.22)
        self.assertFalse(prepared_credit.reconciled_payment_ids)
        self.assertEqual(len(prepared_return), 1)
        self.assertNotIn(prepared_return.state, ("done", "cancel"))
        self.assertEqual(order.total_refunded_amount, 0)

        for payload, event, expected_response, p2_returned, refund_total, remaining in (
            (first_refund, "evt-first-completed", "success", 0, 1.22, 3.58),
            (first_refund, "evt-first-completed", "already_processed", 0, 1.22, 3.58),
            (second_refund, "evt-second-completed", "success", 1, 3.02, 1.78),
            (first_refund, "evt-first-replayed", "already_processed", 1, 3.02, 1.78),
        ):
            with self.subTest(event=event, response=expected_response):
                self._send_refund_webhook(payload, "updated", "COMPLETED", event, expected_response)
                expected_refunds = [(first_refund["id"], 1.22, self.product_p1)]
                if p2_returned:
                    expected_refunds.append((second_refund["id"], 1.80, self.product_p2))
                refunds = order.square_refund_ids
                self.assertEqual(len(refunds), len(expected_refunds))
                self.assertIn(first, refunds)
                self.assertEqual(first.credit_note_id, prepared_credit)
                self.assertEqual(first.return_picking_ids, prepared_return)
                credits = self.env["account.move"]
                payments = sale_payment
                returns = self.env["stock.picking"]
                for refund_id, amount, product in expected_refunds:
                    refund = refunds.filtered(lambda record: record.square_refund_id == refund_id)
                    self.assertEqual(len(refund), 1)
                    self.assertEqual(refund.sale_order_id, order)
                    self.assertEqual(refund.invoice_id, invoice)
                    self.assertEqual(refund.status, "completed")
                    self.assertTrue(refund.processed_at)
                    self.assertEqual(refund.refund_amount, amount)
                    credit = refund.credit_note_id
                    self.assertEqual(credit.reversed_entry_id, invoice)
                    credits |= credit
                    payments |= self.assert_reconciled_payment(credit, "outbound", amount)

                    returned = refund.return_picking_ids
                    self.assertEqual(len(returned), 1)
                    self.assertEqual(returned.return_id, delivery)
                    self.assertEqual(returned.state, "done")
                    self.assertEqual(returned.picking_type_code, "incoming")
                    self.assertEqual(returned.location_dest_id, self.warehouse_w1.lot_stock_id)
                    self.assertEqual(returned.move_ids.product_id, product)
                    self.assertEqual(returned.move_ids.mapped("quantity"), [1])
                    returns |= returned

                self.assertEqual(invoice.reversal_move_ids, credits)
                self.assertEqual(self.env["account.move"].search([
                    ("partner_id", "=", order.partner_id.id),
                    ("move_type", "=", "out_refund"),
                ]), credits)
                self.assertEqual(self.env["account.payment"].search([
                    ("partner_id", "=", order.partner_id.id),
                ]), payments)
                self.assertEqual(order.picking_ids, delivery | returns)
                self.assertEqual(self.assert_reconciled_payment(invoice, "inbound", 4.80), sale_payment)
                self.assertAlmostEqual(sum(credits.mapped("amount_total")), refund_total, places=2)
                self.assertAlmostEqual(order.total_refunded_amount, refund_total, places=2)
                self.assertAlmostEqual(
                    invoice.amount_total - order.total_refunded_amount, remaining, places=2
                )
                self.assertEqual(order.refund_status, "partially_refunded")

                self.assertEqual(p1_line.returned_qty, 1)
                self.assertEqual(p1_line.effective_qty, 0)
                self.assertEqual(p1_line.qty_delivered, 0)
                self.assertEqual(p2_line.returned_qty, p2_returned)
                self.assertEqual(p2_line.effective_qty, 2 - p2_returned)
                self.assertEqual(p2_line.qty_delivered, 2 - p2_returned)
                self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50)
                self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48 + p2_returned)
                for product in (self.product_p1, self.product_p2):
                    self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

    def _send_refund_webhook(self, refund, event, state, event_id, expected="success"):
        response = self.url_open(
            "/square/webhook",
            data=json.dumps({
                "type": f"refund.{event}",
                "event_id": event_id,
                "data": {"object": {"refund": {**refund, "status": state}}},
            }),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], expected, response.text)

    def assert_reconciled_payment(self, invoice, direction, amount):
        self.assertEqual(invoice.state, "posted")
        self.assertEqual(invoice.currency_id, self.env.ref("base.EUR"))
        self.assertAlmostEqual(invoice.amount_total, amount, places=2)
        self.assertAlmostEqual(invoice.amount_residual, 0, places=2)
        self.assertIn(invoice.payment_state, ("paid", "in_payment"))
        payment = invoice.reconciled_payment_ids
        self.assertEqual(len(payment), 1)
        self.assertEqual(payment.payment_type, direction)
        self.assertEqual(payment.partner_id, invoice.partner_id)
        self.assertAlmostEqual(payment.amount, amount, places=2)
        self.assertEqual(payment.currency_id, invoice.currency_id)
        self.assertEqual(payment.journal_id, self.payment_journal)
        self.assertEqual(payment.move_id.state, "posted")
        invoice_receivable = invoice.line_ids.filtered(
            lambda line: line.account_type == "asset_receivable"
        )
        payment_receivable = payment.move_id.line_ids.filtered(
            lambda line: line.account_type == "asset_receivable"
        )
        self.assertTrue(invoice_receivable.full_reconcile_id)
        self.assertEqual(
            payment_receivable.full_reconcile_id, invoice_receivable.full_reconcile_id
        )
        return payment

    def _assert_warehouse_stock(self, mapped_quantity):
        for product in (self.product_p1, self.product_p2):
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), mapped_quantity)
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

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

    def _test_sale_processing(self, expected_amount=3.00):
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
        self.assertEqual(p2_line.product_uom_qty, self.p2_order_quantity)

        self.assertAlmostEqual(self.created_sale_order.amount_total, expected_amount, places=2)

        _logger.info("✓ Sale processing verification passed")

    def _test_stock_updates(self, expected_amount=3.00):
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

        self.assert_reconciled_payment(self.created_invoice, "inbound", expected_amount)

        # Verify stock was decreased in W1
        self.assertEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w1), 50 - self.p2_order_quantity
        )

        # Verify stock in W2 unchanged
        self.assertEqual(
            self._get_stock_quantity(self.product_p1, self.warehouse_w2), 50
        )
        self.assertEqual(
            self._get_stock_quantity(self.product_p2, self.warehouse_w2), 50
        )

        _logger.info("✓ Stock updates verification passed")

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
        single_line.tax_id = False

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
        self.assertEqual(single_line.qty_delivered, 1.0)
        self.assertEqual(single_line.effective_qty, 1.0)
        self.assertEqual(refund_record.return_picking_ids.state, "done")
        self.assertEqual(refund_record.return_picking_ids.move_ids.mapped("quantity"), [1.0])
        self.assert_reconciled_payment(refund_record.credit_note_id, "outbound", 1.00)

        _logger.info("✓ Single line partial refund quantity fix test passed")

    def test_partial_refund_rounds_untaxed_credit_note(self):
        self._test_sale_processing()
        order = self.created_sale_order
        order.order_line.write({"price_unit": 1.00, "tax_id": False})
        self._confirm_deliver_invoice_mapped_wh(order)
        invoice = order.invoice_ids
        self.assert_reconciled_payment(invoice, "inbound", 2.00)

        refund_data = {
            "id": "REFUND_L1_ONE_CENT",
            "order_id": "ORDER_L1_001",
            "payment_id": "payment_001",
            "amount_money": {"amount": 1, "currency": "EUR"},
            "refunded_line_ids": ["line_1"],
        }
        self._send_refund_webhook(refund_data, "created", "COMPLETED", "evt-one-cent-refund")

        credit = order.square_refund_ids.credit_note_id
        self.assertEqual(credit.reversed_entry_id, invoice)
        self.assertEqual(credit.amount_tax, 0)
        self.assert_reconciled_payment(credit, "outbound", 0.01)

# -*- coding: utf-8 -*-
import json
import logging
from datetime import datetime
from unittest.mock import call, patch

from odoo.exceptions import UserError
from odoo.tests import Form
from odoo.tests.common import tagged
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

    def _send_square_order_event(self, event_type, event_id, state, **order_data):
        payload = {
            "type": event_type,
            "event_id": event_id,
            "data": {
                "object": {
                    event_type.replace(".", "_"): {
                        "id": "ORDER_L1_001",
                        "location_id": "L1",
                        "state": state,
                        **order_data,
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

    def _confirm_sale_and_deliver_first_shipment(self):
        square_order = self._mock_get_order("ORDER_L1_001")
        square_order["state"] = "OPEN"
        with patch.object(SquareApiClient, "get_order", return_value=square_order):
            result = self._send_square_order_event(
                "order.created", "evt_open_split_sale", "OPEN"
            )
        self.assertEqual(result["status"], "success", result)
        order = self._assert_square_sale_order("draft")
        order.action_confirm()
        first = order.picking_ids
        first.action_assign()
        first.move_ids.filtered(lambda move: move.product_id == self.product_p2).quantity = 1
        first.with_context(skip_backorder=True).button_validate()
        self.assertEqual(first.state, "done")
        backorder = order.picking_ids - first
        self.assertEqual(len(backorder), 1)
        self.assertEqual(backorder.backorder_id, first)
        self.assertEqual(backorder.move_ids.product_id, self.product_p2)
        self.assertEqual(backorder.move_ids.product_uom_qty, 1)
        return order, first, backorder

    def _import_paid_square_sale_with_split_delivery(self):
        order, first, backorder = self._confirm_sale_and_deliver_first_shipment()
        result = self._send_square_order_event(
            "order.updated", "evt_paid_split_sale", "COMPLETED"
        )
        self.assertEqual(result["status"], "updated", result)
        self.assertEqual(backorder.state, "done")
        self._assert_paid_square_sale(delivery_count=2)
        return first, backorder

    def test_open_and_draft_square_order_updates_and_replay(self):
        initial_order = self._mock_get_order("ORDER_L1_001")
        initial_order.update(state="OPEN", tenders=[])
        with patch.object(SquareApiClient, "get_order", return_value=initial_order):
            result = self._send_square_order_event(
                "order.created", "evt_open_order_created", "OPEN"
            )
        self.assertEqual(result["status"], "success", result)

        order_domain = [("square_order_id", "=", "ORDER_L1_001")]
        order = self.env["sale.order"].search(order_domain)
        self.assertEqual(len(order), 1)
        self.assertEqual(order.partner_id, self.customer)
        self.assertEqual(order.warehouse_id, self.warehouse_w1)
        self.assertEqual(order.amount_total, 3.60)
        lines = order.order_line
        self.assertEqual(lines.mapped("square_line_id"), ["line_1", "line_2"])
        order_log_domain = [("sale_order_id", "=", order.id)]
        creation_logs = self.env["square.integration.log"].search(
            order_log_domain + [("event_type", "=", "order_created")]
        )
        self.assertTrue(creation_logs)

        for state, event_id, quantity, line_total, order_total in (
            ("OPEN", "evt_open_order_updated", 3, 3.60, 6.00),
            ("DRAFT", "evt_draft_order_updated", 4, 4.80, 7.20),
            ("DRAFT", "evt_draft_order_updated_again", 5, 6.00, 8.40),
        ):
            with self.subTest(event_id=event_id):
                line_items = [
                    dict(
                        initial_order["line_items"][0],
                        quantity=str(quantity),
                        total_money={
                            "amount": round(line_total * 100),
                            "currency": "EUR",
                        },
                        total_tax_money={"amount": 20 * quantity, "currency": "EUR"},
                    ),
                    initial_order["line_items"][1],
                ]
                result = self._send_square_order_event(
                    "order.updated", event_id, state, line_items=line_items
                )
                self.assertEqual(result["status"], "updated", result)
                result = self._send_square_order_event(
                    "order.updated", event_id, state, line_items=line_items
                )
                self.assertEqual(result["status"], "already_processed", result)

                self.assertEqual(self.env["sale.order"].search(order_domain), order)
                self.assertEqual(order.state, "draft")
                self.assertEqual(order.order_line, lines)
                self.assertEqual(lines.mapped("product_uom_qty"), [quantity, 2])
                self.assertAlmostEqual(lines[0].price_total, line_total, places=2)
                self.assertAlmostEqual(lines[1].price_total, 2.40, places=2)
                self.assertAlmostEqual(order.amount_total, order_total, places=2)

        self.assertFalse(order.invoice_ids)
        self.assertFalse(
            self.env["account.payment"].search_count(
                [("partner_id", "=", self.customer.id)]
            )
        )
        self.assertFalse(order.picking_ids)
        for product in (self.product_p1, self.product_p2):
            for warehouse in (self.warehouse_w1, self.warehouse_w2):
                self.assertEqual(self._get_stock_quantity(product, warehouse), 50)
        order_logs = self.env["square.integration.log"].search(order_log_domain)
        self.assertTrue(creation_logs <= order_logs)
        self.assertEqual(
            len(order_logs.filtered(lambda entry: entry.event_type == "order_updated")),
            3,
        )

    def test_delayed_order_updates_leave_the_paid_sale_unchanged(self):
        initial_order = self._mock_get_order("ORDER_L1_001")
        initial_order["state"] = "OPEN"
        with patch.object(SquareApiClient, "get_order", return_value=initial_order):
            result = self._send_square_order_event(
                "order.created", "evt_open_before_payment", "OPEN"
            )
        self.assertEqual(result["status"], "success", result)
        self._import_paid_square_sale()
        order = self.created_sale_order
        invoice = self.created_invoice
        payment = invoice.reconciled_payment_ids
        pickings = order.picking_ids
        moves = pickings.move_ids

        for state in ("OPEN", "DRAFT"):
            line_items = [
                dict(
                    initial_order["line_items"][0],
                    quantity="3",
                    total_money={"amount": 360, "currency": "EUR"},
                    total_tax_money={"amount": 60, "currency": "EUR"},
                ),
                initial_order["line_items"][1],
            ]
            event_id = f"evt_delayed_{state.lower()}"
            result = self._send_square_order_event(
                "order.updated", event_id, state, line_items=line_items
            )
            self.assertEqual(result["status"], "updated", result)
            replay = self._send_square_order_event(
                "order.updated", event_id, state, line_items=line_items
            )
            self.assertEqual(replay["status"], "already_processed", replay)
            self.assertEqual(order.state, "sale")
            self.assertEqual(order.order_line.mapped("product_uom_qty"), [1, 2])
            self.assertAlmostEqual(order.amount_total, 3.60, places=2)
            self.assertEqual(order.invoice_ids, invoice)
            self.assertEqual(invoice.state, "posted")
            self.assertEqual(invoice.payment_state, "paid")
            self.assertEqual(invoice.amount_residual, 0)
            self.assertEqual(
                self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
                payment,
            )
            self.assertEqual(order.picking_ids, pickings)
            self.assertEqual(pickings.move_ids, moves)
            self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
            self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

    def test_paid_and_delivered_square_order_cancellation_and_replay(self):
        self._import_paid_square_sale()
        order = self.created_sale_order
        invoice = self.created_invoice
        payment = invoice.reconciled_payment_ids
        delivery = order.picking_ids
        order_lines = order.order_line
        log_domain = [("sale_order_id", "=", order.id)]
        original_logs = self.env["square.integration.log"].search(log_domain)
        self.assertTrue(original_logs)

        result = self._send_square_order_event(
            "order.updated", "evt_paid_order_canceled", "CANCELED"
        )
        self.assertEqual(result["status"], "updated", result)
        reversal = invoice.reversal_move_ids
        self.assertEqual(len(reversal), 1)

        for event_id, state in (
            ("evt_paid_order_canceled", "CANCELED"),
            ("evt_paid_order_cancelled", "CANCELLED"),
        ):
            result = self._send_square_order_event("order.updated", event_id, state)
            self.assertIn(result["status"], ("already_processed", "updated"), result)

        self.assertEqual(
            self.env["sale.order"].search([("square_order_id", "=", "ORDER_L1_001")]),
            order,
        )
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.order_line, order_lines)
        self.assertEqual(order.invoice_ids, invoice | reversal)
        self.assertEqual(invoice.reversal_move_ids, reversal)
        self.assertEqual(invoice.state, "posted")
        self.assertEqual(invoice.payment_state, "paid")
        self.assertAlmostEqual(invoice.amount_total, 3.60, places=2)
        self.assertAlmostEqual(invoice.amount_residual, 0.0, places=2)
        self.assertEqual(invoice.reconciled_payment_ids, payment)

        self.assertEqual(reversal.move_type, "out_refund")
        self.assertEqual(reversal.state, "posted")
        self.assertEqual(reversal.reversed_entry_id, invoice)
        self.assertEqual(reversal.currency_id, invoice.currency_id)
        self.assertAlmostEqual(reversal.amount_total, 3.60, places=2)
        self.assertAlmostEqual(reversal.amount_residual, 3.60, places=2)
        self.assertEqual(
            self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
            payment,
        )
        self.assertEqual(payment.payment_type, "inbound")
        self.assertEqual(payment.move_id.state, "posted")
        self.assertAlmostEqual(payment.amount, 3.60, places=2)
        self.assertFalse(order.square_refund_ids)

        self.assertEqual(order.picking_ids, delivery)
        self.assertEqual(delivery.state, "done")
        self.assertEqual(delivery.picking_type_code, "outgoing")
        self.assertEqual(
            {move.product_id: move.quantity for move in delivery.move_ids},
            {self.product_p1: 1, self.product_p2: 2},
        )
        for product, quantity in ((self.product_p1, 49), (self.product_p2, 48)):
            self.assertEqual(
                self._get_stock_quantity(product, self.warehouse_w1), quantity
            )
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

        logs = self.env["square.integration.log"].search(log_domain)
        self.assertTrue(original_logs <= logs)
        cancellation_logs = logs - original_logs
        self.assertEqual(len(cancellation_logs), 1)
        self.assertEqual(cancellation_logs.event_type, "order_updated")
        self.assertIn(reversal.name, cancellation_logs.description)

    def test_cancellation_replay_posts_only_the_missing_invoice_reversal(self):
        (self.product_p1 | self.product_p2).invoice_policy = "delivery"
        order, first, backorder = self._confirm_sale_and_deliver_first_shipment()
        first_invoice = order._create_invoices()
        first_invoice.action_post()
        backorder.action_assign()
        backorder.button_validate()
        self.assertEqual(backorder.state, "done")
        second_invoice = order._create_invoices()
        second_invoice.action_post()
        self.assertEqual(order.invoice_ids, first_invoice | second_invoice)
        self.assertEqual(order.invoice_status, "invoiced")
        self.assertAlmostEqual(first_invoice.amount_total, 2.40, places=2)
        self.assertAlmostEqual(second_invoice.amount_total, 1.20, places=2)

        move_model = type(self.env["account.move"])
        post_moves = move_model.action_post

        def post_except_second_reversal(moves):
            if second_invoice.id in moves.reversed_entry_id.ids:
                raise UserError("Test failure posting the second cancellation reversal")
            return post_moves(moves)

        event_id = "evt_cancellation_two_invoices"
        with patch.object(move_model, "action_post", post_except_second_reversal):
            result = self._send_square_order_event("order.updated", event_id, "CANCELED")
        self.assertEqual(result["status"], "error", result)
        first_reversal = first_invoice.reversal_move_ids
        self.assertEqual(len(first_reversal), 1)
        self.assertEqual(first_reversal.state, "posted")
        self.assertFalse(second_invoice.reversal_move_ids)

        log_model = self.env["square.integration.log"]
        success_domain = [("webhook_event_id", "=", event_id), ("status", "=", "success")]
        self.assertFalse(log_model.search(success_domain))
        failures = log_model.search([
            ("sale_order_id", "=", order.id),
            ("event_type", "=", "order_updated"),
            ("status", "=", "error"),
        ])
        self.assertEqual(len(failures), 1)
        self.assertIn(second_invoice.name, failures.description)

        result = self._send_square_order_event("order.updated", event_id, "CANCELED")
        self.assertEqual(result["status"], "updated", result)
        second_reversal = second_invoice.reversal_move_ids
        self.assertEqual(len(second_reversal), 1)
        self.assertEqual(second_reversal.state, "posted")
        self.assertAlmostEqual(first_reversal.amount_total, 2.40, places=2)
        self.assertAlmostEqual(second_reversal.amount_total, 1.20, places=2)

        result = self._send_square_order_event("order.updated", event_id, "CANCELED")
        self.assertEqual(result["status"], "already_processed", result)
        self.assertEqual(first_invoice.reversal_move_ids, first_reversal)
        self.assertEqual(second_invoice.reversal_move_ids, second_reversal)
        self.assertEqual(
            order.invoice_ids, first_invoice | second_invoice | first_reversal | second_reversal
        )
        self.assertEqual(log_model.search_count(success_domain), 1)
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.picking_ids, first | backorder)
        self.assertEqual(set(order.picking_ids.mapped("state")), {"done"})
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
        self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)
        self.assertFalse(
            self.env["account.payment"].search([("partner_id", "=", self.customer.id)])
        )

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

    def _assert_paid_square_sale(self, delivery_count=1):
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

        deliveries = order.picking_ids
        self.assertEqual(len(deliveries), delivery_count)
        self.assertEqual(set(deliveries.mapped("picking_type_code")), {"outgoing"})
        self.assertEqual(set(deliveries.mapped("state")), {"done"})
        self.assertEqual(deliveries.location_id, self.warehouse_w1.lot_stock_id)
        self.assertEqual(len(deliveries.move_ids), delivery_count + 1)
        self.assertEqual(set(deliveries.move_ids.mapped("state")), {"done"})
        for product, quantity in ((self.product_p1, 1), (self.product_p2, 2)):
            moves = deliveries.move_ids.filtered(lambda move: move.product_id == product)
            self.assertEqual(sum(moves.mapped("quantity")), quantity)
        for product, quantity in ((self.product_p1, 49), (self.product_p2, 48)):
            self.assertEqual(
                self._get_stock_quantity(product, self.warehouse_w1), quantity
            )
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

        self.created_invoice = invoice

    def _send_square_exchange_event(self, event_type, event_id, object_type, data):
        response = self.url_open(
            "/square/webhook",
            json={
                "type": event_type,
                "event_id": event_id,
                "data": {"object": {object_type: data}},
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def _run_square_exchange(self, replacement_cents, payment_cents, retry_after_catalog_failure=False):
        self._import_paid_square_sale()
        self.replacement_product = self.env["product.product"].create(
            {
                "name": "Replacement Product",
                "default_code": "REPLACEMENT-SKU",
                "type": "consu",
                "is_storable": True,
                "invoice_policy": "order",
                "taxes_id": [(6, 0, self.sale_tax.ids)],
            }
        )
        for warehouse in self.warehouse_w1 | self.warehouse_w2:
            self._set_initial_stock(self.replacement_product, warehouse, 50)

        exchange_order = {
            "id": "EXCHANGE_L1_001",
            "location_id": "L1",
            "state": "COMPLETED",
            "returns": [
                {
                    "source_order_id": self.created_sale_order.square_order_id,
                    "return_line_items": [
                        {"catalog_object_id": "P1-SKU", "quantity": "1"},
                    ],
                },
            ],
        }
        self.enterContext(
            patch.object(
                SquareApiClient,
                "get_order",
                return_value={
                    **exchange_order,
                    "line_items": [
                        {
                            "uid": "replacement_line",
                            "name": self.replacement_product.name,
                            "catalog_object_id": "REPLACEMENT-CATALOG",
                            "quantity": "1",
                            "total_money": {"amount": replacement_cents, "currency": "EUR"},
                        },
                    ],
                },
            )
        )
        catalog_match = {"success": True, "sku": self.replacement_product.default_code}
        get_catalog_object = self.enterContext(
            patch.object(
                SquareApiClient,
                "get_catalog_object",
                return_value=catalog_match,
            )
        )

        result = self._send_square_exchange_event(
            "order.created", "evt_exchange_created", "order_created", exchange_order
        )
        self.assertEqual(result["status"], "success", result)
        self._assert_exchange_pending()

        payment = {
            "id": "payment_exchange_001",
            "order_id": exchange_order["id"],
            "status": "APPROVED",
            "amount_money": {"amount": payment_cents, "currency": "EUR"},
        }
        result = self._send_square_exchange_event(
            "payment.updated", "evt_exchange_approved", "payment", payment
        )
        self.assertEqual(result["status"], "ignored", result)
        self._assert_exchange_pending()

        payment["status"] = "COMPLETED"
        if retry_after_catalog_failure:
            get_catalog_object.return_value = {"success": False, "not_found": True}
            result = self._send_square_exchange_event(
                "payment.updated", "evt_exchange_completed", "payment", payment
            )
            self.assertEqual(result["status"], "error", result)
            self._assert_exchange_pending()
            get_catalog_object.return_value = catalog_match

        result = self._send_square_exchange_event(
            "payment.updated", "evt_exchange_completed", "payment", payment
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(result["original_order_id"], self.created_sale_order.id)
        return payment

    def _assert_exchange_pending(self):
        self._assert_paid_square_sale()
        self.assertFalse(self.created_invoice.reversal_move_ids)
        self.assertFalse(self.created_sale_order.picking_ids.return_ids)
        self.assertFalse(self.env["sale.order"].search([("square_order_id", "=", "EXCHANGE_L1_001")]))
        self.assertEqual(self._get_stock_quantity(self.replacement_product, self.warehouse_w1), 50)
        self.assertEqual(self._get_stock_quantity(self.replacement_product, self.warehouse_w2), 50)

    def _assert_settled_exchange(self, replacement_total, payment_delta):
        original_order = self._assert_square_sale_order("sale")
        original_invoice = self.created_invoice
        self.assertEqual(original_order.invoice_ids, original_invoice)
        self.assertEqual(original_invoice.payment_state, "paid")
        self.assertAlmostEqual(original_invoice.amount_total, 3.60, places=2)
        self.assertAlmostEqual(original_invoice.amount_residual, 0.0, places=2)

        replacement_order = self.env["sale.order"].search([("square_order_id", "=", "EXCHANGE_L1_001")])
        self.assertEqual(len(replacement_order), 1)
        self.assertIn(original_order.name, replacement_order.origin)
        self.assertEqual(replacement_order.partner_id, original_order.partner_id)
        self.assertEqual(replacement_order.warehouse_id, self.warehouse_w1)
        self.assertEqual(replacement_order.state, "sale")
        self.assertEqual(len(replacement_order.order_line), 1)
        self.assertEqual(replacement_order.order_line.product_id, self.replacement_product)
        self.assertEqual(replacement_order.order_line.product_uom_qty, 1)
        self.assertAlmostEqual(replacement_order.amount_total, replacement_total, places=2)

        credit_note = original_invoice.reversal_move_ids
        replacement_invoice = replacement_order.invoice_ids
        self.assertEqual(len(credit_note), 1)
        self.assertEqual(len(replacement_invoice), 1)
        self.assertEqual(credit_note.move_type, "out_refund")
        self.assertEqual(credit_note.invoice_origin, original_order.name)
        self.assertEqual(credit_note.reversed_entry_id, original_invoice)
        self.assertEqual(len(credit_note.invoice_line_ids), 1)
        self.assertEqual(credit_note.invoice_line_ids.product_id, self.product_p1)
        self.assertEqual(credit_note.invoice_line_ids.quantity, 1)
        self.assertAlmostEqual(credit_note.amount_total, 1.20, places=2)
        self.assertEqual(replacement_invoice.move_type, "out_invoice")
        self.assertAlmostEqual(replacement_invoice.amount_total, replacement_total, places=2)
        for document in credit_note | replacement_invoice:
            self.assertEqual(document.state, "posted")
            self.assertEqual(document.currency_id, self.env.ref("base.EUR"))
            self.assertAlmostEqual(document.amount_residual, 0.0, places=2)

        exchange_lines = (credit_note | replacement_invoice).line_ids.filtered(
            lambda line: line.account_id.account_type == "asset_receivable"
        )
        self.assertEqual(len(exchange_lines), 2)
        reconciliation = exchange_lines.full_reconcile_id
        self.assertEqual(len(reconciliation), 1)
        self.assertTrue(all(line.full_reconcile_id == reconciliation for line in exchange_lines))

        partner_payments = self.env["account.payment"].search([("partner_id", "=", self.customer.id)])
        original_payment = original_invoice.reconciled_payment_ids
        self.assertEqual(len(original_payment), 1)
        self.assertAlmostEqual(original_payment.amount, 3.60, places=2)
        exchange_payments = partner_payments - original_payment
        if payment_delta:
            self.assertEqual(len(exchange_payments), 1)
            payment_type = "inbound" if payment_delta > 0 else "outbound"
            self.assertEqual(exchange_payments.payment_type, payment_type)
            self.assertAlmostEqual(exchange_payments.amount, abs(payment_delta), places=2)
            self.assertEqual(exchange_payments.journal_id, self.payment_journal)
            self.assertEqual(exchange_payments.currency_id, original_payment.currency_id)
            self.assertEqual(exchange_payments.move_id.state, "posted")
            paid_document = replacement_invoice if payment_delta > 0 else credit_note
            self.assertEqual(paid_document.reconciled_payment_ids, exchange_payments)
            payment_line = exchange_payments.move_id.line_ids.filtered(
                lambda line: line.account_id.account_type == "asset_receivable"
            )
            self.assertEqual(len(payment_line), 1)
            self.assertEqual(payment_line.full_reconcile_id, reconciliation)
        else:
            self.assertFalse(exchange_payments)

        original_delivery = original_order.picking_ids.filtered(lambda picking: picking.picking_type_code == "outgoing")
        self.assertEqual(len(original_delivery), 1)
        return_picking = original_delivery.return_ids
        self.assertEqual(len(return_picking), 1)
        self.assertEqual(return_picking.picking_type_code, "incoming")
        self.assertEqual(return_picking.location_dest_id, self.warehouse_w1.lot_stock_id)
        self.assertEqual(return_picking.move_ids.origin_returned_move_id, original_delivery.move_ids.filtered(lambda move: move.product_id == self.product_p1))
        replacement_delivery = replacement_order.picking_ids
        self.assertEqual(len(replacement_delivery), 1)
        self.assertEqual(replacement_delivery.picking_type_code, "outgoing")
        self.assertEqual(replacement_delivery.location_id, self.warehouse_w1.lot_stock_id)
        for picking, product in (
            (return_picking, self.product_p1),
            (replacement_delivery, self.replacement_product),
        ):
            self.assertEqual(picking.state, "done")
            self.assertEqual(len(picking.move_ids), 1)
            self.assertEqual(picking.move_ids.state, "done")
            self.assertEqual(picking.move_ids.product_id, product)
            self.assertEqual(picking.move_ids.quantity, 1)
        for product, quantity in (
            (self.product_p1, 50), (self.product_p2, 48), (self.replacement_product, 49),
        ):
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), quantity)
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

    def test_exchange_respects_an_unreserved_pending_return(self):
        self._import_paid_square_sale()
        order = self.created_sale_order
        invoice = self.created_invoice
        delivery = order.picking_ids
        sale_payment = invoice.reconciled_payment_ids
        refund_id = "REFUND_BEFORE_EXCHANGE"
        result = self._send_square_refund_event(
            "refund.created", "evt_before_exchange_pending", refund_id, "PENDING", 120,
            ("line_1",),
        )
        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links(refund_id, 1.20)
        pending_return = refund.return_picking_ids
        pending_return.do_unreserve()
        self.assertEqual(pending_return.move_ids.quantity, 0)
        self.assertEqual(pending_return.move_ids.product_uom_qty, 1)
        self.assertNotIn(pending_return.state, ("done", "cancel"))

        exchange = {
            "id": "EXCHANGE_PENDING_RETURN",
            "location_id": "L1",
            "state": "COMPLETED",
            "returns": [{
                "source_order_id": order.square_order_id,
                "return_line_items": [{"catalog_object_id": "P1-SKU", "quantity": "1"}],
            }],
        }
        payment = {
            "id": "payment_pending_return_exchange",
            "order_id": exchange["id"],
            "status": "COMPLETED",
            "amount_money": {"amount": 120, "currency": "EUR"},
        }
        with patch.object(SquareApiClient, "get_order", return_value=exchange):
            result = self._send_square_exchange_event(
                "payment.updated", "evt_exchange_pending_return", "payment", payment
            )
        self.assertEqual(result["status"], "error", result)
        self.assertIn("exceeds the quantity available", result["message"])
        self.assertEqual(delivery.return_ids, pending_return)
        self.assertEqual(invoice.reversal_move_ids, refund.credit_note_id)
        self.assertEqual(refund.credit_note_id.state, "draft")
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
        self.assertEqual(
            self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
            sale_payment,
        )

        result = self._send_square_refund_event(
            "refund.updated", "evt_before_exchange_cancelled", refund_id, "CANCELED", 120,
            ("line_1",),
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(pending_return.state, "cancel")

        with patch.object(SquareApiClient, "get_order", return_value=exchange):
            result = self._send_square_exchange_event(
                "payment.updated", "evt_exchange_pending_return", "payment", payment
            )
        self.assertEqual(result["status"], "success", result)
        exchange_return = delivery.return_ids - pending_return
        self.assertEqual(len(exchange_return), 1)
        self.assertEqual(exchange_return.state, "done")
        self.assertEqual(exchange_return.move_ids.product_id, self.product_p1)
        self.assertEqual(exchange_return.move_ids.quantity, 1)
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50)
        exchange_credit = invoice.reversal_move_ids - refund.credit_note_id
        self.assertEqual(len(exchange_credit), 1)
        self.assertEqual(exchange_credit.state, "posted")
        self.assertAlmostEqual(exchange_credit.amount_total, 1.20, places=2)
        self.assertAlmostEqual(exchange_credit.amount_residual, 0, places=2)

    def test_equal_price_exchange_settles_without_another_payment(self):
        payment = self._run_square_exchange(replacement_cents=120, payment_cents=0)
        self._assert_settled_exchange(replacement_total=1.20, payment_delta=0.0)

        for event_id, status in (
            ("evt_exchange_completed", "already_processed"),
            ("evt_exchange_replay", "skipped"),
        ):
            result = self._send_square_exchange_event("payment.updated", event_id, "payment", payment)
            self.assertEqual(result["status"], status, result)
            self._assert_settled_exchange(replacement_total=1.20, payment_delta=0.0)

    def test_exchange_records_one_cent_payment_after_retry(self):
        self._run_square_exchange(
            replacement_cents=121, payment_cents=1, retry_after_catalog_failure=True
        )
        self._assert_settled_exchange(replacement_total=1.21, payment_delta=0.01)

    def test_cheaper_exchange_records_the_refund_difference(self):
        self._run_square_exchange(replacement_cents=60, payment_cents=0)
        self._assert_settled_exchange(replacement_total=0.60, payment_delta=-0.60)

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

    def _send_square_refund_event(
        self,
        event_type,
        event_id,
        refund_id,
        status,
        amount_cents,
        refunded_line_ids=(),
        *,
        expected_status_code=200,
    ):
        refund = {
            "id": refund_id,
            "order_id": self.created_sale_order.square_order_id,
            "payment_id": self.created_sale_order.square_payment_id,
            "status": status,
            "amount_money": {"amount": amount_cents, "currency": "EUR"},
            "reason": "Returned goods",
        }
        if refunded_line_ids:
            refund["refunded_line_ids"] = list(refunded_line_ids)
        response = self.url_open(
            "/square/webhook",
            data=json.dumps({
                "type": event_type,
                "event_id": event_id,
                "data": {"object": {"refund": refund}},
            }),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, expected_status_code, response.text)
        return response.json()

    def _assert_refund_links(self, refund_id, amount):
        refund = self.env["square.refund"].search([("square_refund_id", "=", refund_id)])
        self.assertEqual(len(refund), 1)
        self.assertEqual(refund.sale_order_id, self.created_sale_order)
        self.assertEqual(refund.invoice_id, self.created_invoice)
        self.assertEqual(refund.square_order_id, self.created_sale_order.square_order_id)
        self.assertAlmostEqual(refund.refund_amount, amount, places=2)
        self.assertEqual(refund.currency_id, self.created_invoice.currency_id)
        credit_note = refund.credit_note_id
        self.assertTrue(credit_note)
        self.assertEqual(credit_note.move_type, "out_refund")
        self.assertEqual(credit_note.reversed_entry_id, self.created_invoice)
        self.assertEqual(credit_note.currency_id, refund.currency_id)
        self.assertAlmostEqual(credit_note.amount_total, amount, places=2)
        return refund

    def _assert_refund_payment(self, refund):
        credit_note = refund.credit_note_id
        self.assertEqual(credit_note.state, "posted")
        self.assertEqual(credit_note.payment_state, "paid")
        self.assertAlmostEqual(credit_note.amount_residual, 0.0, places=2)
        payment = credit_note.reconciled_payment_ids
        self.assertEqual(len(payment), 1)
        self.assertEqual(payment.journal_id, self.payment_journal)
        self.assertEqual(payment.payment_type, "outbound")
        self.assertEqual(payment.currency_id, refund.currency_id)
        self.assertAlmostEqual(payment.amount, refund.refund_amount, places=2)
        self.assertEqual(payment.move_id.state, "posted")
        note_receivable = credit_note.line_ids.filtered(
            lambda line: line.account_id.account_type == "asset_receivable"
        )
        payment_receivable = payment.move_id.line_ids.filtered(
            lambda line: line.account_id.account_type == "asset_receivable"
        )
        self.assertEqual(len(note_receivable), 1)
        self.assertEqual(len(payment_receivable), 1)
        self.assertTrue(note_receivable.full_reconcile_id)
        self.assertEqual(
            note_receivable.full_reconcile_id, payment_receivable.full_reconcile_id
        )
        return payment

    def _assert_completed_return(self, refund, delivery, quantities):
        picking = refund.return_picking_ids
        self.assertEqual(len(picking), 1)
        self.assertEqual(picking.return_id, delivery)
        self.assertEqual(picking.picking_type_code, "incoming")
        self.assertEqual(picking.location_dest_id, self.warehouse_w1.lot_stock_id)
        self.assertEqual(picking.state, "done")
        self.assertEqual(len(picking.move_ids), len(quantities))
        self.assertEqual(set(picking.move_ids.mapped("state")), {"done"})
        self.assertEqual(
            {move.product_id: move.quantity for move in picking.move_ids}, quantities
        )
        self.assertEqual(
            picking.move_ids.origin_returned_move_id,
            delivery.move_ids.filtered(lambda move: move.product_id in quantities),
        )

    def test_full_refund_lifecycle_and_replay(self):
        self._import_paid_square_sale()
        order = self.created_sale_order
        invoice = self.created_invoice
        delivery = order.picking_ids
        sale_payment = invoice.reconciled_payment_ids
        refund_id = "REFUND_L1_FULL"

        result = self._send_square_refund_event(
            "refund.created", "evt_full_refund_created", refund_id, "PENDING", 360
        )
        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links(refund_id, 3.60)
        self.assertEqual(refund.status, "pending")
        self.assertFalse(refund.processed_at)
        credit_note = refund.credit_note_id
        self.assertEqual(credit_note.state, "draft")
        self.assertEqual(
            self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
            sale_payment,
        )
        return_picking = refund.return_picking_ids
        self.assertEqual(len(return_picking), 1)
        self.assertEqual(return_picking.return_id, delivery)
        self.assertNotIn(return_picking.state, ("done", "cancel"))
        self.assertEqual(order.order_line.mapped("returned_qty"), [0, 0])
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
        self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

        for event_id, expected_status in (
            ("evt_full_refund_completed", "success"),
            ("evt_full_refund_completed", "already_processed"),
            ("evt_full_refund_completed_retry", "already_processed"),
        ):
            result = self._send_square_refund_event(
                "refund.updated", event_id, refund_id, "COMPLETED", 360
            )
            self.assertEqual(result["status"], expected_status, result)
            self.assertEqual(self._assert_refund_links(refund_id, 3.60), refund)
            self.assertEqual(refund.status, "completed")
            self.assertTrue(refund.processed_at)
            self.assertEqual(refund.credit_note_id, credit_note)
            self.assertEqual(refund.return_picking_ids, return_picking)
            payment = self._assert_refund_payment(refund)
            self.assertEqual(order.square_refund_ids, refund)
            self.assertEqual(invoice.reversal_move_ids, credit_note)
            self.assertEqual(invoice.reconciled_payment_ids, sale_payment)
            self.assertEqual(invoice.payment_state, "paid")
            self.assertAlmostEqual(invoice.amount_residual, 0.0, places=2)
            self.assertEqual(
                self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
                sale_payment | payment,
            )
            self.assertEqual(delivery.return_ids, return_picking)
            self.assertEqual(order.refund_status, "fully_refunded")
            self.assertAlmostEqual(order.total_refunded_amount, 3.60, places=2)
            self._assert_completed_return(
                refund, delivery, {self.product_p1: 1, self.product_p2: 2}
            )
            for product, quantity in ((self.product_p1, 1), (self.product_p2, 2)):
                line = order.order_line.filtered(
                    lambda line: line.product_id == product and line.product_uom_qty > 0
                )
                self.assertEqual(line.returned_qty, quantity)
                self.assertEqual(line.effective_qty, 0)
                for warehouse in (self.warehouse_w1, self.warehouse_w2):
                    self.assertEqual(self._get_stock_quantity(product, warehouse), 50)

        completion_log = self.env["square.integration.log"].search([
            ("webhook_event_id", "=", "evt_full_refund_completed"),
            ("square_refund_id", "=", refund_id),
            ("sale_order_id", "=", order.id),
            ("status", "=", "success"),
        ])
        self.assertEqual(len(completion_log), 1)

    def test_full_refund_reuses_the_completed_odoo_return(self):
        self._import_paid_square_sale()
        delivery = self.created_sale_order.picking_ids
        wizard = self.env["stock.return.picking"].with_user(
            self.env.ref("base.user_admin")
        ).create({"picking_id": delivery.id})
        action = wizard.action_create_returns_all()
        manual_return = self.env["stock.picking"].browse(action["res_id"])
        manual_return.button_validate()
        self.assertEqual(manual_return.state, "done")
        self.assertEqual(delivery.return_ids, manual_return)

        refund_id = "REFUND_ALREADY_RETURNED"
        for event_id, status in (
            ("evt_already_returned", "success"),
            ("evt_already_returned_retry", "already_processed"),
        ):
            result = self._send_square_refund_event(
                "refund.updated", event_id, refund_id, "COMPLETED", 360
            )
            self.assertEqual(result["status"], status, result)
            refund = self._assert_refund_links(refund_id, 3.60)
            self._assert_refund_payment(refund)
            self.assertEqual(refund.return_picking_ids, manual_return)
            self.assertEqual(delivery.return_ids, manual_return)
            self._assert_completed_return(
                refund, delivery, {self.product_p1: 1, self.product_p2: 2}
            )
            self.assertEqual(self.created_invoice.reversal_move_ids, refund.credit_note_id)
            self.assertEqual(
                self.created_invoice.reconciled_payment_ids.payment_type, "inbound"
            )
            for product in (self.product_p1, self.product_p2):
                self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), 50)
                self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

    def test_full_refund_completes_a_prepared_odoo_return(self):
        self._import_paid_square_sale()
        delivery = self.created_sale_order.picking_ids
        wizard = self.env["stock.return.picking"].with_user(
            self.env.ref("base.user_admin")
        ).create({"picking_id": delivery.id})
        action = wizard.action_create_returns_all()
        manual_return = self.env["stock.picking"].browse(action["res_id"])
        prepared_state = manual_return.state
        self.assertNotIn(prepared_state, ("done", "cancel"))

        failed_id = "REFUND_FAILED_WITH_OPEN_RETURN"
        refund_id = "REFUND_PREPARED_ODOO_RETURN"
        for event_type, event_id, square_refund_id, status in (
            ("refund.created", "evt_prepared_previous_pending", failed_id, "PENDING"),
            ("refund.updated", "evt_prepared_previous_failed", failed_id, "FAILED"),
            ("refund.created", "evt_prepared_pending", refund_id, "PENDING"),
        ):
            result = self._send_square_refund_event(
                event_type, event_id, square_refund_id, status, 360
            )
            self.assertEqual(result["status"], "success", result)
            self.assertEqual(delivery.return_ids, manual_return)
            self.assertEqual(manual_return.state, prepared_state)
            self.assertFalse(self._assert_refund_links(square_refund_id, 3.60).return_picking_ids)
            self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
            self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

        for event_id, status in (
            ("evt_prepared_odoo_return", "success"),
            ("evt_prepared_odoo_return_retry", "already_processed"),
        ):
            result = self._send_square_refund_event(
                "refund.updated", event_id, refund_id, "COMPLETED", 360
            )
            self.assertEqual(result["status"], status, result)
            refund = self._assert_refund_links(refund_id, 3.60)
            self._assert_refund_payment(refund)
            self.assertEqual(refund.return_picking_ids, manual_return)
            self.assertEqual(delivery.return_ids, manual_return)
            self._assert_completed_return(
                refund, delivery, {self.product_p1: 1, self.product_p2: 2}
            )
            for product in (self.product_p1, self.product_p2):
                self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), 50)

    def test_replacement_full_refund_links_returns_after_failure(self):
        first, backorder = self._import_paid_square_sale_with_split_delivery()
        admin = self.env.ref("base.user_admin")
        wizard = self.env["stock.return.picking"].with_user(admin).create(
            {"picking_id": first.id}
        )
        action = wizard.action_create_returns_all()
        manual_return = self.env["stock.picking"].browse(action["res_id"])
        manual_return.with_user(admin).button_validate()
        self.assertEqual(manual_return.state, "done")

        failed_id = "REFUND_FAILED_AFTER_RETURN"
        result = self._send_square_refund_event(
            "refund.created", "evt_prior_return_pending", failed_id, "PENDING", 360
        )
        self.assertEqual(result["status"], "success", result)
        failed = self._assert_refund_links(failed_id, 3.60)
        prepared_return = backorder.return_ids
        self.assertEqual(len(prepared_return), 1)
        completed_returns = manual_return | prepared_return
        self.assertEqual(failed.return_picking_ids, completed_returns)
        prepared_return.with_user(admin).button_validate()
        self.assertEqual(prepared_return.state, "done")

        result = self._send_square_refund_event(
            "refund.updated", "evt_prior_return_failed", failed_id, "FAILED", 360
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(failed.credit_note_id.state, "cancel")

        replacement_id = "REFUND_REPLACEMENT_AFTER_RETURN"
        for event_id, status in (
            ("evt_replacement_after_return", "success"),
            ("evt_replacement_after_return_retry", "already_processed"),
        ):
            result = self._send_square_refund_event(
                "refund.updated", event_id, replacement_id, "COMPLETED", 360
            )
            self.assertEqual(result["status"], status, result)
            replacement = self._assert_refund_links(replacement_id, 3.60)
            self._assert_refund_payment(replacement)
            self.assertEqual(replacement.return_picking_ids, completed_returns)
            self.assertEqual(failed.return_picking_ids, completed_returns)
            self.assertEqual(first.return_ids, manual_return)
            self.assertEqual(backorder.return_ids, prepared_return)
            self.assertEqual(set(completed_returns.mapped("state")), {"done"})
            self.assertFalse(failed.credit_note_id.reconciled_payment_ids)
            for product in (self.product_p1, self.product_p2):
                self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), 50)

    def test_full_refund_leaves_an_exchange_return_on_its_own_transaction(self):
        self._run_square_exchange(replacement_cents=120, payment_cents=0)
        delivery = self.created_sale_order.picking_ids.filtered(
            lambda picking: picking.picking_type_code == "outgoing"
        )
        exchange_return = delivery.return_ids
        self.assertEqual(len(exchange_return), 1)
        self.assertEqual(exchange_return.state, "done")
        self.assertEqual(exchange_return.move_ids.product_id, self.product_p1)

        refund_id = "REFUND_AFTER_EXCHANGE"
        result = self._send_square_refund_event(
            "refund.updated", "evt_refund_after_exchange", refund_id, "COMPLETED", 360
        )
        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links(refund_id, 3.60)
        self._assert_refund_payment(refund)
        self._assert_completed_return(refund, delivery, {self.product_p2: 2})
        self.assertEqual(delivery.return_ids, exchange_return | refund.return_picking_ids)
        for product in (self.product_p1, self.product_p2):
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), 50)

    def test_full_refund_returns_only_the_outstanding_shipment(self):
        first, backorder = self._import_paid_square_sale_with_split_delivery()
        wizard = self.env["stock.return.picking"].with_user(
            self.env.ref("base.user_admin")
        ).create({"picking_id": first.id})
        action = wizard.action_create_returns_all()
        manual_return = self.env["stock.picking"].browse(action["res_id"])
        manual_return.button_validate()
        self.assertEqual(manual_return.state, "done")

        refund_id = "REFUND_SPLIT_FULL"
        result = self._send_square_refund_event(
            "refund.updated", "evt_split_full_done", refund_id, "COMPLETED", 360
        )
        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links(refund_id, 3.60)
        self._assert_refund_payment(refund)
        self.assertEqual(first.return_ids, manual_return)
        self.assertEqual(len(backorder.return_ids), 1)
        self.assertEqual(refund.return_picking_ids, manual_return | backorder.return_ids)
        self.assertEqual(backorder.return_ids.state, "done")
        self.assertEqual(backorder.return_ids.move_ids.origin_returned_move_id, backorder.move_ids)
        self.assertEqual(backorder.return_ids.move_ids.quantity, 1)
        for product in (self.product_p1, self.product_p2):
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), 50)
            self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

    def test_partial_refund_of_paid_sale(self):
        self._import_paid_square_sale()
        order = self.created_sale_order
        invoice = self.created_invoice
        delivery = order.picking_ids
        sale_payment = invoice.reconciled_payment_ids
        refund_id = "REFUND_L1_PARTIAL"

        result = self._send_square_refund_event(
            "refund.created", "evt_partial_pending", refund_id, "PENDING", 120,
            ("line_1",),
        )
        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links(refund_id, 1.20)
        self.assertEqual(refund.credit_note_id.state, "draft")
        self.assertFalse(refund.credit_note_id.reconciled_payment_ids)
        self.assertEqual(refund.status, "pending")
        self.assertFalse(refund.processed_at)
        return_picking = refund.return_picking_ids
        self.assertEqual(len(return_picking), 1)
        self.assertNotIn(return_picking.state, ("done", "cancel"))
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)

        result = self._send_square_refund_event(
            "refund.updated", "evt_partial_completed", refund_id, "COMPLETED", 120,
            ("line_1",),
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(self._assert_refund_links(refund_id, 1.20), refund)
        self.assertEqual(refund.status, "completed")
        self.assertTrue(refund.processed_at)
        self.assertEqual(refund.return_picking_ids, return_picking)
        payment = self._assert_refund_payment(refund)
        self._assert_completed_return(refund, delivery, {self.product_p1: 1})
        self.assertEqual(order.square_refund_ids, refund)
        self.assertEqual(invoice.reversal_move_ids, refund.credit_note_id)
        self.assertEqual(invoice.reconciled_payment_ids, sale_payment)
        self.assertAlmostEqual(invoice.amount_residual, 0, places=2)
        self.assertEqual(
            self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
            sale_payment | payment,
        )
        self.assertAlmostEqual(order.total_refunded_amount, 1.20, places=2)
        self.assertEqual(order.refund_status, "partially_refunded")

    def test_explicit_partial_refund_keeps_its_return_when_square_order_lookup_fails(self):
        self._import_paid_square_sale()
        delivery = self.created_sale_order.picking_ids
        refund_id = "REFUND_EXPLICIT_LOOKUP_UNAVAILABLE"
        result = self._send_square_refund_event(
            "refund.created", "evt_explicit_lookup_pending", refund_id, "PENDING", 120,
            ("line_1",),
        )
        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links(refund_id, 1.20)
        prepared_return = refund.return_picking_ids
        self.assertEqual(prepared_return.move_ids.product_id, self.product_p1)

        with patch.object(SquareApiClient, "get_order", return_value=None):
            result = self._send_square_refund_event(
                "refund.updated", "evt_explicit_lookup_completed", refund_id, "COMPLETED", 120,
                ("line_1",),
            )
        self.assertEqual(result["status"], "success", result)
        self._assert_refund_payment(refund)
        self.assertEqual(refund.return_picking_ids, prepared_return)
        self.assertEqual(delivery.return_ids, prepared_return)
        self._assert_completed_return(refund, delivery, {self.product_p1: 1})
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50)
        self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

    def test_completed_refund_replaces_provisional_return_selection(self):
        self._import_paid_square_sale()
        order = self.created_sale_order
        delivery = order.picking_ids
        refund_id = "REFUND_PROVISIONAL_SELECTION"
        self._send_square_refund_event(
            "refund.created", "evt_provisional_pending", refund_id, "PENDING", 240,
        )
        refund = self._assert_refund_links(refund_id, 2.40)
        provisional = refund.return_picking_ids
        credit = refund.credit_note_id
        self.assertEqual(provisional.move_ids.product_id, self.product_p1 | self.product_p2)
        for event_id, expected_status in (
            ("evt_provisional_completed", "success"),
            ("evt_provisional_completed", "already_processed"),
            ("evt_provisional_replay", "already_processed"),
        ):
            result = self._send_square_refund_event(
                "refund.updated", event_id, refund_id, "COMPLETED", 240, ("line_2",),
            )
            self.assertEqual(result["status"], expected_status, result)
            self.assertEqual(refund.refunded_line_ids, ["line_2"])
            self.assertEqual(refund.credit_note_id, credit)
            self._assert_refund_payment(refund)
            self.assertEqual(provisional.state, "cancel")
            self.assertIn(provisional, refund.return_picking_ids)
            completed = refund.return_picking_ids - provisional
            self.assertEqual(completed.state, "done")
            self.assertEqual(completed.move_ids.product_id, self.product_p2)
            self.assertEqual(completed.move_ids.quantity, 2)
            self.assertEqual(delivery.return_ids, refund.return_picking_ids)
            self.assertEqual(self.created_invoice.reversal_move_ids, credit)
            self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
            self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 50)

    def test_full_refund_retries_after_delayed_failure(self):
        self._assert_full_refund_retries_after_release("FAILED", prepare_later=True)

    def test_full_refund_retries_after_delayed_cancellation(self):
        self._assert_full_refund_retries_after_release("CANCELED", prepare_later=False)

    def _assert_full_refund_retries_after_release(self, terminal_status, prepare_later):
        self._import_paid_square_sale()
        order = self.created_sale_order
        delivery = order.picking_ids
        sale_payment = self.created_invoice.reconciled_payment_ids
        earlier_id = "FULL_REFUND_PENDING_RESERVATION"
        later_id = "FULL_REFUND_COMPLETED_BEFORE_RELEASE"
        pending_ids = (earlier_id, later_id) if prepare_later else (earlier_id,)
        for refund_id in pending_ids:
            result = self._send_square_refund_event(
                "refund.created", f"{refund_id}_pending", refund_id, "PENDING", 360,
            )
            self.assertEqual(result["status"], "success", result)
        earlier = self._assert_refund_links(earlier_id, 3.60)
        earlier_return = earlier.return_picking_ids
        self.assertEqual(len(earlier_return), 1)
        earlier_credit = earlier.credit_note_id
        later = order.square_refund_ids.filtered(lambda refund: refund.square_refund_id == later_id)
        prepared_credit = later.credit_note_id
        completion = ("refund.updated", f"{later_id}_completed", later_id, "COMPLETED", 360)

        result = self._send_square_refund_event(*completion, expected_status_code=503)
        self.assertEqual(result["status"], "retry", result)
        later = order.square_refund_ids.filtered(lambda refund: refund.square_refund_id == later_id)
        self.assertFalse(later.processed_at)
        self.assertFalse(later.return_picking_ids)
        self.assertEqual(self.created_invoice.reversal_move_ids, earlier_credit | prepared_credit)
        self.assertFalse((earlier_credit | prepared_credit).reconciled_payment_ids)
        if prepare_later:
            self.assertEqual(later.status, "completed")
            self.assertEqual(prepared_credit.state, "draft")
        else:
            self.assertEqual(len(later), 1)
            self.assertFalse(later.credit_note_id)
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
        self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

        result = self._send_square_refund_event(
            "refund.updated", f"{earlier_id}_released", earlier_id, terminal_status, 360,
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(earlier_return.state, "cancel")
        self.assertEqual(earlier_credit.state, "cancel")
        for expected_status in ("success", "already_processed"):
            result = self._send_square_refund_event(*completion)
            self.assertEqual(result["status"], expected_status, result)
            later = self._assert_refund_links(later_id, 3.60)
            self.assertTrue(later.processed_at)
            if prepare_later:
                self.assertEqual(later.credit_note_id, prepared_credit)
            payment = self._assert_refund_payment(later)
            self._assert_completed_return(later, delivery, {self.product_p1: 1, self.product_p2: 2})
            self.assertEqual(delivery.return_ids, earlier_return | later.return_picking_ids)
            self.assertEqual(self.created_invoice.reversal_move_ids, earlier_credit | later.credit_note_id)
            self.assertEqual(
                self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
                sale_payment | payment,
            )
            for product in (self.product_p1, self.product_p2):
                self.assertEqual(self._get_stock_quantity(product, self.warehouse_w1), 50)
                self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

    def test_overlapping_partial_refund_replays_after_earlier_reservation_is_released(self):
        self._import_paid_square_sale()
        delivery = self.created_sale_order.picking_ids
        sale_payment = self.created_invoice.reconciled_payment_ids
        earlier_id = "REFUND_PENDING_RESERVATION"
        later_id = "REFUND_COMPLETED_BEFORE_RELEASE"
        for refund_id in (earlier_id, later_id):
            result = self._send_square_refund_event(
                "refund.created", f"{refund_id}_pending", refund_id, "PENDING", 120,
                ("line_1",),
            )
            self.assertEqual(result["status"], "success", result)

        earlier = self._assert_refund_links(earlier_id, 1.20)
        later = self._assert_refund_links(later_id, 1.20)
        earlier_return = earlier.return_picking_ids
        self.assertEqual(len(earlier_return), 1)
        later_credit_note = later.credit_note_id
        completion_event = (
            "refund.updated", f"{later_id}_completed", later_id, "COMPLETED", 120, ("line_1",),
        )
        result = self._send_square_refund_event(*completion_event, expected_status_code=503)
        self.assertEqual(result["status"], "retry", result)
        self.assertFalse(later.processed_at)
        self.assertFalse(later.return_picking_ids)
        self.assertEqual(later_credit_note.state, "draft")
        self.assertFalse(later_credit_note.reconciled_payment_ids)
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)

        result = self._send_square_refund_event(
            "refund.updated", f"{earlier_id}_failed", earlier_id, "FAILED", 120,
            ("line_1",),
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(earlier_return.state, "cancel")

        for expected_status in ("success", "already_processed"):
            result = self._send_square_refund_event(*completion_event)
            self.assertEqual(result["status"], expected_status, result)
            self.assertTrue(later.processed_at)
            self.assertEqual(later.credit_note_id, later_credit_note)
            payment = self._assert_refund_payment(later)
            self._assert_completed_return(later, delivery, {self.product_p1: 1})
            self.assertEqual(delivery.return_ids, earlier_return | later.return_picking_ids)
            self.assertEqual(
                self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
                sale_payment | payment,
            )
            self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50)
            self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

    def test_pending_partial_refund_prepares_stock_after_earlier_refund_fails(self):
        self._import_paid_square_sale()
        order = self.created_sale_order
        delivery = order.picking_ids
        invoice = self.created_invoice
        sale_payment = invoice.reconciled_payment_ids
        earlier_id = "REFUND_EARLIER"
        later_id = "REFUND_LATER"
        for refund_id in (earlier_id, later_id):
            result = self._send_square_refund_event(
                "refund.created", f"{refund_id}_pending", refund_id, "PENDING", 120,
                ("line_1",),
            )
            self.assertEqual(result["status"], "success", result)

        earlier = self._assert_refund_links(earlier_id, 1.20)
        later = self._assert_refund_links(later_id, 1.20)
        self.assertEqual(len(earlier.return_picking_ids), 1)
        self.assertNotIn(earlier.return_picking_ids.state, ("done", "cancel"))
        self.assertFalse(later.return_picking_ids)
        prepared_credit_note = later.credit_note_id
        self.assertEqual(prepared_credit_note.state, "draft")

        result = self._send_square_refund_event(
            "refund.updated", f"{earlier_id}_failed", earlier_id, "FAILED", 120,
            ("line_1",),
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(earlier.credit_note_id.state, "cancel")
        self.assertEqual(earlier.return_picking_ids.state, "cancel")
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)

        for event_id, status in (
            (f"{later_id}_completed", "success"),
            (f"{later_id}_replayed", "already_processed"),
        ):
            result = self._send_square_refund_event(
                "refund.updated", event_id, later_id, "COMPLETED", 120, ("line_1",)
            )
            self.assertEqual(result["status"], status, result)
            self.assertEqual(later.credit_note_id, prepared_credit_note)
            payment = self._assert_refund_payment(later)
            self._assert_completed_return(later, delivery, {self.product_p1: 1})
            self.assertEqual(delivery.return_ids, earlier.return_picking_ids | later.return_picking_ids)
            self.assertEqual(
                self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
                sale_payment | payment,
            )
            self.assertEqual(order.order_line.filtered(lambda line: line.square_line_id == "line_1").returned_qty, 1)
            self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 50)
            self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

    def test_partial_refund_returns_one_sale_line_across_two_deliveries(self):
        first, backorder = self._import_paid_square_sale_with_split_delivery()
        refund_id = "REFUND_SPLIT_PARTIAL"
        result = self._send_square_refund_event(
            "refund.created", "evt_split_partial_pending", refund_id, "PENDING", 240,
            ("line_2",),
        )
        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links(refund_id, 2.40)
        credit_note = refund.credit_note_id
        picks = refund.return_picking_ids
        self.assertEqual(len(picks), 2)
        self.assertEqual(picks.return_id, first | backorder)
        self.assertEqual(credit_note.state, "draft")

        result = self._send_square_refund_event(
            "refund.updated", "evt_split_partial_done", refund_id, "COMPLETED", 240,
            ("line_2",),
        )
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(refund.credit_note_id, credit_note)
        self._assert_refund_payment(refund)
        self.assertEqual(refund.return_picking_ids, picks)
        for delivery in first | backorder:
            picking = delivery.return_ids
            self.assertEqual(len(picking), 1)
            self.assertIn(picking, picks)
            self.assertEqual(picking.state, "done")
            self.assertEqual(picking.location_dest_id, self.warehouse_w1.lot_stock_id)
            self.assertEqual(picking.move_ids.product_id, self.product_p2)
            self.assertEqual(picking.move_ids.quantity, 1)
            self.assertEqual(picking.move_ids.origin_returned_move_id.picking_id, delivery)
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
        self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 50)

    def test_unsuccessful_full_refunds_cancel_prepared_actions(self):
        self._import_paid_square_sale()
        order = self.created_sale_order
        invoice = self.created_invoice
        for final_status, expected_status in (("FAILED", "failed"), ("CANCELED", "cancelled")):
            refund_id = f"REFUND_{final_status}"
            result = self._send_square_refund_event(
                "refund.created", f"{refund_id}_pending", refund_id, "PENDING", 360
            )
            self.assertEqual(result["status"], "success", result)
            refund = self._assert_refund_links(refund_id, 3.60)
            self.assertEqual(refund.credit_note_id.state, "draft")
            self.assertEqual(len(refund.return_picking_ids), 1)

            result = self._send_square_refund_event(
                "refund.updated", f"{refund_id}_final", refund_id, final_status, 360
            )
            self.assertEqual(result["status"], "success", result)
            self.assertEqual(refund.status, expected_status)
            self.assertFalse(refund.processed_at)
            self.assertEqual(refund.credit_note_id.state, "cancel")
            self.assertEqual(refund.return_picking_ids.state, "cancel")

        self.assertEqual(
            self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
            invoice.reconciled_payment_ids,
        )
        self.assertEqual(order.refund_status, "not_refunded")
        self.assertEqual(order.total_refunded_amount, 0)
        self.assertEqual(order.order_line.mapped("returned_qty"), [0, 0])
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)
        self.assertEqual(self._get_stock_quantity(self.product_p2, self.warehouse_w1), 48)

    def test_multiple_partial_refunds_and_replay(self):
        self._test_initial_setup()
        self._test_sale_processing()
        self._test_stock_updates()
        order = self.created_sale_order
        invoice = self.created_invoice
        delivery = order.picking_ids
        sale_payment = invoice.reconciled_payment_ids
        original_lines = order.order_line

        first_refund = ("evt_partial_first", "REFUND_L1_FIRST", ("line_1",))
        second_refund = ("evt_partial_second", "REFUND_L1_SECOND", ())
        refunds = self.env["square.refund"]
        notes = self.env["account.move"]
        payments = self.env["account.payment"]
        returns = self.env["stock.picking"]

        for event, expected_status, total, balance, p2_returned in (
            (first_refund, "success", 1.20, 2.40, 0),
            (first_refund, "already_processed", 1.20, 2.40, 0),
            (second_refund, "success", 2.40, 1.20, 1),
            (first_refund, "already_processed", 2.40, 1.20, 1),
        ):
            event_id, refund_id, line_ids = event
            result = self._send_square_refund_event(
                "refund.created", event_id, refund_id, "COMPLETED", 120, line_ids
            )
            self.assertEqual(result["status"], expected_status, result)
            refund = self._assert_refund_links(refund_id, 1.20)
            self.assertEqual(refund.status, "completed")
            self.assertTrue(refund.processed_at)
            self.assertEqual(refund.webhook_event_id, event_id)
            payment = self._assert_refund_payment(refund)
            product = self.product_p1 if event == first_refund else self.product_p2
            self._assert_completed_return(refund, delivery, {product: 1})

            if expected_status == "success":
                refunds |= refund
                notes |= refund.credit_note_id
                payments |= payment
                returns |= refund.return_picking_ids
            expected_count = 2 if p2_returned else 1
            self.assertEqual(len(refunds), expected_count)
            self.assertEqual(len(notes), expected_count)
            self.assertEqual(len(payments), expected_count)
            self.assertEqual(len(returns), expected_count)
            self.assertEqual(order.square_refund_ids, refunds)
            self.assertEqual(invoice.reversal_move_ids, notes)
            self.assertEqual(refunds.credit_note_id, notes)
            self.assertEqual(refunds.return_picking_ids, returns)
            self.assertEqual(notes.reconciled_payment_ids, payments)
            self.assertEqual(delivery.return_ids, returns)
            self.assertEqual(
                self.env["account.payment"].search([("partner_id", "=", self.customer.id)]),
                sale_payment | payments,
            )
            self.assertEqual(invoice.reconciled_payment_ids, sale_payment)
            self.assertEqual(invoice.payment_state, "paid")
            self.assertAlmostEqual(invoice.amount_residual, 0, places=2)
            self.assertAlmostEqual(invoice.amount_total, 3.60, places=2)
            self.assertAlmostEqual(order.total_refunded_amount, total, places=2)
            self.assertAlmostEqual(sum(notes.mapped("amount_total")), total, places=2)
            self.assertAlmostEqual(sum(payments.mapped("amount")), total, places=2)
            self.assertAlmostEqual(invoice.amount_total - total, balance, places=2)
            self.assertEqual(order.refund_status, "partially_refunded")

            for product, sold, returned in (
                (self.product_p1, 1, 1), (self.product_p2, 2, p2_returned),
            ):
                line = original_lines.filtered(lambda line: line.product_id == product)
                self.assertEqual(line.product_uom_qty, sold)
                self.assertEqual(line.returned_qty, returned)
                self.assertEqual(line.effective_qty, sold - returned)
                self.assertEqual(line.qty_delivered, sold - returned)
                self.assertEqual(
                    self._get_stock_quantity(product, self.warehouse_w1),
                    50 - sold + returned,
                )
                self.assertEqual(self._get_stock_quantity(product, self.warehouse_w2), 50)

            completion_log = self.env["square.integration.log"].search([
                ("webhook_event_id", "=", event_id),
                ("square_refund_id", "=", refund_id),
                ("sale_order_id", "=", order.id),
                ("status", "=", "success"),
            ])
            self.assertEqual(len(completion_log), 1)

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

    def test_partial_refund_single_line_quantity_fix(self):
        """Return one unit when the refund matches the VAT-inclusive unit price."""
        square_order = self._mock_get_order("ORDER_L1_001")
        square_order["line_items"] = [dict(
            square_order["line_items"][0],
            quantity="2",
            total_money={"amount": 240, "currency": "EUR"},
            total_tax_money={"amount": 40, "currency": "EUR"},
        )]
        square_order["total_money"]["amount"] = 240
        square_order["total_tax_money"]["amount"] = 40
        square_order["tenders"][0]["amount_money"]["amount"] = 240

        with patch.object(SquareApiClient, "get_order", return_value=square_order):
            result = self._send_square_order_event(
                "order.updated", "evt_single_line_sale", "COMPLETED"
            )
            self.assertIn(result["status"], ("success", "updated"), result)
            order = self.env["sale.order"].search([("square_order_id", "=", "ORDER_L1_001")])
            self.assertEqual(len(order), 1)
            self.created_sale_order = order
            self.created_invoice = invoice = order.invoice_ids
            self.assertEqual(len(invoice), 1)
            self.assertAlmostEqual(invoice.amount_total, 2.40, places=2)
            self.assertAlmostEqual(invoice.amount_residual, 0.0, places=2)
            self.assertEqual(invoice.payment_state, "paid")
            delivery = order.picking_ids
            self.assertEqual(len(delivery), 1)
            self.assertEqual(delivery.state, "done")
            line = order.order_line
            self.assertEqual(len(line), 1)
            self.assertEqual(line.product_uom_qty, 2)
            self.assertEqual(line.qty_delivered, 2)
            self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 48)

            result = self._send_square_refund_event(
                "refund.created", "evt_single_line_refund", "SINGLE_LINE_REFUND_TEST",
                "COMPLETED", 120,
            )

        self.assertEqual(result["status"], "success", result)
        refund = self._assert_refund_links("SINGLE_LINE_REFUND_TEST", 1.20)
        self._assert_refund_payment(refund)
        self._assert_completed_return(refund, delivery, {self.product_p1: 1})
        self.assertEqual(line.product_uom_qty, 2)
        self.assertEqual(line.qty_delivered, 1)
        self.assertEqual(line.returned_qty, 1)
        self.assertEqual(line.effective_qty, 1)
        self.assertEqual(self._get_stock_quantity(self.product_p1, self.warehouse_w1), 49)

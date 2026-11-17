import json
from unittest.mock import patch

from odoo.tests.common import tagged
from odoo.tools import mute_logger

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient

from .common import SquareHttpCase


@tagged("post_install", "-at_install")
class TestPaidSquareSale(SquareHttpCase):
    @mute_logger("odoo.addons.odoo_square.controllers.square_webhook")
    def test_completed_checkout_records_payment_and_delivers_once(self):
        euro = self.env.ref("base.EUR")
        self.env.company.currency_id = euro
        partner = self.env["res.partner"].create(
            {"name": "Existing Customer", "email": "buyer@example.com"}
        )
        journal = self.env["account.journal"].create(
            {"name": "Checkout Receipts", "type": "bank", "code": "SQ"}
        )
        tax = self.env["account.tax"].create(
            {"name": "Square VAT 20%", "type_tax_use": "sale", "amount": 20}
        )
        product = self.env["product.product"].create(
            {
                "name": "Delivery Item",
                "default_code": "CHECKOUT-SKU",
                "type": "consu",
                "is_storable": True,
                "invoice_policy": "delivery",
                "taxes_id": [(6, 0, tax.ids)],
            }
        )
        other_warehouse = self.env["stock.warehouse"].search(
            [("company_id", "=", self.env.company.id)], limit=1
        )
        warehouse = self.env["stock.warehouse"].create(
            {"name": "Checkout Warehouse", "code": "SQWH"}
        )
        second_bin = self.env["stock.location"].create(
            {"name": "Second Bin", "location_id": warehouse.lot_stock_id.id}
        )
        for location, quantity in (
            (warehouse.lot_stock_id, 1),
            (second_bin, 1),
            (other_warehouse.lot_stock_id, 7),
        ):
            self.env["stock.quant"].create(
                {
                    "product_id": product.id,
                    "location_id": location.id,
                    "quantity": quantity,
                }
            )
        config = self.env["square.config"].create(
            {
                "name": "Checkout Integration",
                "square_application_id": "test_app_id",
                "square_webhook_signature_key": "test-pos-webhook-key",
                "square_access_token": "test_access_token",
                "payment_journal_id": journal.id,
            }
        )
        self.env["square.location.mapping"].create(
            {
                "config_id": config.id,
                "square_location_id": "checkout-location",
                "square_location_name": "Checkout Location",
                "warehouse_id": warehouse.id,
            }
        )

        order_notice = {
            "id": "paid-checkout",
            "location_id": "checkout-location",
            "state": "COMPLETED",
        }
        square_order = {
            **order_notice,
            "line_items": [
                {
                    "uid": "line-checkout",
                    "name": "Square Item Name",
                    "catalog_object_id": "CHECKOUT-CATALOG-ID",
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
                    "id": "checkout-payment",
                    "amount_money": {"amount": 3000, "currency": "EUR"},
                }
            ],
        }
        event_id = "evt-paid-checkout"
        webhook_data = json.dumps(
            {
                "event_id": event_id,
                "type": "order.created",
                "data": {"object": {"order_created": order_notice}},
            }
        )

        mapped_stock = product.with_context(warehouse_id=warehouse.id)
        other_stock = product.with_context(warehouse_id=other_warehouse.id)
        self.assertEqual(mapped_stock.qty_available, 2)
        self.assertEqual(other_stock.qty_available, 7)

        order_domain = [("square_order_id", "=", order_notice["id"])]
        invoice_domain = [("partner_id", "=", partner.id), ("move_type", "=", "out_invoice")]
        payment_domain = [("partner_id", "=", partner.id)]
        with (
            patch.object(SquareApiClient, "_get_order", return_value=square_order),
            patch.object(
                SquareApiClient,
                "_get_catalog_object",
                return_value={"success": True, "sku": "CHECKOUT-SKU"},
            ),
        ):
            records = None
            for expected_status in ("success", "already_processed"):
                response = self.post_square_webhook(
                    data=webhook_data,
                    headers={"Content-Type": "application/json"},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["status"], expected_status, response.text)

                order = self.env["sale.order"].search(order_domain)
                invoice = self.env["account.move"].search(invoice_domain)
                payment = self.env["account.payment"].search(payment_domain)
                self.assertEqual(len(order), 1)
                self.assertEqual(order.state, "sale")
                self.assertEqual(order.create_uid, self.env.ref("odoo_square.user_square_bot"))
                self.assertEqual(order.partner_id, partner)
                self.assertEqual(order.warehouse_id, warehouse)
                self.assertEqual(order.square_payment_id, "checkout-payment")
                self.assertEqual(order.currency_id, euro)
                self.assertAlmostEqual(order.amount_total, 30, places=2)
                self.assertEqual(len(order.order_line), 1)
                self.assertEqual(order.order_line.product_id, product)
                self.assertEqual(order.order_line.product_uom_qty, 2)
                self.assertAlmostEqual(order.order_line.price_unit, 12.50, places=2)

                delivery = order.picking_ids
                self.assertEqual(len(delivery), 1)
                self.assertEqual(delivery.picking_type_code, "outgoing")
                self.assertEqual(delivery.state, "done")
                self.assertEqual(delivery.location_id, warehouse.lot_stock_id)
                self.assertEqual(delivery.move_ids.product_id, product)
                self.assertEqual(sum(delivery.move_ids.mapped("quantity")), 2)
                self.assertEqual(order.order_line.qty_delivered, 2)
                self.assertEqual(mapped_stock.qty_available, 0)
                self.assertEqual(other_stock.qty_available, 7)

                self.assertEqual(len(invoice), 1)
                self.assertEqual(order.invoice_ids, invoice)
                self.assertEqual(invoice.state, "posted")
                self.assertEqual(invoice.currency_id, euro)
                self.assertAlmostEqual(invoice.amount_total, 30, places=2)
                self.assertAlmostEqual(invoice.amount_residual, 0, places=2)
                self.assertIn(invoice.payment_state, ("paid", "in_payment"))
                self.assertEqual(len(payment), 1)
                self.assertAlmostEqual(payment.amount, 30, places=2)
                self.assertEqual(payment.currency_id, euro)
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
                self.assertEqual(
                    payment_receivable.full_reconcile_id, invoice_receivable.full_reconcile_id
                )

                current_records = (order, delivery, invoice, payment)
                if records:
                    self.assertEqual(current_records, records)
                records = current_records

        logs = self.env["square.integration.log"].search(
            [("sale_order_id", "=", order.id), ("status", "=", "success")]
        )
        self.assertIn("stock_sync", logs.mapped("event_type"))
        self.assertIn("order_updated", logs.mapped("event_type"))

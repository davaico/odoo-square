import json
from unittest.mock import patch

from odoo.tests.common import tagged

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient

from .common import SquareHttpCase


@tagged("post_install", "-at_install")
class TestSquareExchange(SquareHttpCase):
    def setUp(self):
        super().setUp()
        self.env.company.currency_id = self.env.ref("base.EUR")
        self.partner = self.env["res.partner"].create(
            {"name": "Exchange Customer", "email": "exchange@example.com"}
        )
        self.journal = self.env["account.journal"].create(
            {"name": "Square Exchange Payments", "type": "bank", "code": "SQ"}
        )
        self.other_warehouse = self.env["stock.warehouse"].search(
            [("company_id", "=", self.env.company.id)], limit=1
        )
        self.warehouse = self.env["stock.warehouse"].create(
            {"name": "Exchange Warehouse", "code": "EXCH"}
        )
        config = self.env["square.config"].create(
            {
                "name": "Exchange Integration",
                "square_application_id": "test_app_id",
                "square_webhook_signature_key": "test-pos-webhook-key",
                "square_access_token": "test_access_token",
                "payment_journal_id": self.journal.id,
            }
        )
        for location_id, warehouse in (
            ("other-location", self.other_warehouse),
            ("exchange-location", self.warehouse),
        ):
            self.env["square.location.mapping"].create(
                {
                    "config_id": config.id,
                    "square_location_id": location_id,
                    "square_location_name": location_id,
                    "warehouse_id": warehouse.id,
                }
            )
        tax = self.env["account.tax"].create(
            {"name": "Square VAT 20%", "type_tax_use": "sale", "amount": 20}
        )
        self.original_product, self.replacement_product = self.env["product.product"].create(
            [
                {
                    "name": name,
                    "default_code": sku,
                    "type": "consu",
                    "is_storable": True,
                    "invoice_policy": "delivery",
                    "taxes_id": [(6, 0, tax.ids)],
                }
                for name, sku in (("Original Item", "ORIGINAL-SKU"), ("Replacement Item", "REPLACEMENT-SKU"))
            ]
        )
        for product in self.original_product | self.replacement_product:
            for warehouse, quantity in ((self.warehouse, 5), (self.other_warehouse, 9)):
                self.env["stock.quant"].create(
                    {
                        "product_id": product.id,
                        "location_id": warehouse.lot_stock_id.id,
                        "quantity": quantity,
                    }
                )
        self.original_order = {
            "id": "original-square-order",
            "location_id": "exchange-location",
            "state": "COMPLETED",
            "line_items": [
                {
                    "uid": "original-line",
                    "name": "Original Item",
                    "catalog_object_id": "ORIGINAL-CATALOG",
                    "quantity": "2",
                    "base_price_money": {"amount": 1000, "currency": "EUR"},
                    "total_money": {"amount": 2400, "currency": "EUR"},
                    "total_tax_money": {"amount": 400, "currency": "EUR"},
                }
            ],
            "total_money": {"amount": 2400, "currency": "EUR"},
            "total_tax_money": {"amount": 400, "currency": "EUR"},
            "fulfillments": [
                {"pickup_details": {"recipient": {"email_address": self.partner.email}}}
            ],
            "tenders": [
                {
                    "id": "original-square-payment",
                    "type": "CARD",
                    "card_details": {"status": "CAPTURED"},
                    "amount_money": {"amount": 2400, "currency": "EUR"},
                }
            ],
        }
        self.exchange_order = {
            "id": "exchange-square-order",
            "location_id": "exchange-location",
            "state": "COMPLETED",
            "returns": [
                {
                    "source_order_id": self.original_order["id"],
                    "return_line_items": [
                        {
                            "source_line_item_uid": "original-line",
                            "catalog_object_id": "ORIGINAL-CATALOG",
                            "quantity": "1",
                        }
                    ],
                }
            ],
        }
        orders = {order["id"]: order for order in (self.original_order, self.exchange_order)}
        get_order = patch.object(SquareApiClient, "_get_order", side_effect=orders.get)
        self.catalog = {
            "ORIGINAL-CATALOG": {"success": True, "sku": "ORIGINAL-SKU"},
            "REPLACEMENT-CATALOG": {"success": True, "sku": "REPLACEMENT-SKU"},
        }
        get_catalog = patch.object(
            SquareApiClient, "_get_catalog_object", side_effect=self.catalog.__getitem__
        )
        get_order.start()
        self.addCleanup(get_order.stop)
        get_catalog.start()
        self.addCleanup(get_catalog.stop)

    def _send_webhook(self, event_type, event_id, data, expected_status, *, http_status=200):
        response = self.post_square_webhook(
            data=json.dumps(
                {"type": event_type, "event_id": event_id, "data": {"object": data}}
            ),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, http_status, response.text)
        self.assertEqual(response.json()["status"], expected_status, response.text)
        return response.json()

    def _stock_quantities(self, warehouse):
        return tuple(
            product.with_context(warehouse_id=warehouse.id).qty_available
            for product in self.original_product | self.replacement_product
        )

    def _assert_movement(self, picking, picking_type, product, quantity):
        self.assertEqual(len(picking), 1)
        self.assertEqual(picking.picking_type_code, picking_type)
        self.assertEqual(picking.state, "done")
        self.assertEqual(picking.move_ids.product_id, product)
        self.assertEqual(sum(picking.move_ids.mapped("quantity")), quantity)

    def _set_replacement_price(self, amount, tax):
        self.exchange_order["line_items"] = [
            {
                "uid": "replacement-line",
                "name": "Replacement Item",
                "catalog_object_id": "REPLACEMENT-CATALOG",
                "quantity": "1",
                "base_price_money": {"amount": amount - tax, "currency": "EUR"},
                "total_money": {"amount": amount, "currency": "EUR"},
                "total_tax_money": {"amount": tax, "currency": "EUR"},
            }
        ]

    def _payment_notice(self, status, amount):
        return {
            "id": "exchange-square-payment",
            "order_id": self.exchange_order["id"],
            "status": status,
            "amount_money": {"amount": amount, "currency": "EUR"},
        }

    def _exchange_orders(self):
        return self.env["sale.order"].search(
            [("square_order_id", "in", [self.original_order["id"], self.exchange_order["id"]])]
        )

    def _invoices(self):
        return self.env["account.move"].search(
            [("partner_id", "=", self.partner.id), ("move_type", "in", ("out_invoice", "out_refund"))]
        )

    def _payments(self):
        return self.env["account.payment"].search([("journal_id", "=", self.journal.id)])

    def _import_original_sale(self):
        self._send_webhook(
            "order.created", "original-order-event", {"order_created": self.original_order}, "success"
        )
        original = self._exchange_orders()
        self.assertEqual(len(original), 1)
        self.assertEqual(original.square_order_id, self.original_order["id"])
        self.assertEqual(original.warehouse_id, self.warehouse)
        original_invoice = original.invoice_ids
        self.assertEqual(len(original_invoice), 1)
        self.assertEqual(original_invoice.state, "posted")
        self.assertAlmostEqual(original_invoice.amount_total, 24, places=2)
        self.assertAlmostEqual(original_invoice.amount_residual, 0, places=2)
        original_payment = self._payments()
        self.assertEqual(len(original_payment), 1)
        self.assertEqual(original_payment.payment_type, "inbound")
        self.assertAlmostEqual(original_payment.amount, 24, places=2)
        delivery = original.picking_ids
        self._assert_movement(delivery, "outgoing", self.original_product, 2)
        self.assertEqual(delivery.location_id, self.warehouse.lot_stock_id)
        self.assertEqual(self._stock_quantities(self.warehouse), (3, 5))
        return original, original_invoice, original_payment, delivery

    def _assert_exchange_not_applied(self, original, original_invoice, original_payment, delivery):
        self.assertEqual(self._exchange_orders(), original)
        self.assertEqual(self._invoices(), original_invoice)
        self.assertEqual(self._payments(), original_payment)
        self.assertFalse(delivery.return_ids)
        self.assertEqual(self._stock_quantities(self.warehouse), (3, 5))
        self.assertEqual(self._stock_quantities(self.other_warehouse), (9, 9))

    def _exercise_exchange(self, payment_amount, invoice_total, payment_direction=None, delta=0):
        original, original_invoice, original_payment, delivery = self._import_original_sale()
        notice = {
            "id": self.exchange_order["id"],
            "location_id": "exchange-location",
            "state": "COMPLETED",
        }
        result = self._send_webhook(
            "order.created", "exchange-order-event", {"order_created": notice}, "success"
        )
        self.assertTrue(result["is_exchange"])
        self._assert_exchange_not_applied(original, original_invoice, original_payment, delivery)
        approved = self._payment_notice("APPROVED", payment_amount)
        self._send_webhook("payment.updated", "exchange-approved", {"payment": approved}, "ignored")
        self._assert_exchange_not_applied(original, original_invoice, original_payment, delivery)

        payment = self._payment_notice("COMPLETED", payment_amount)
        records = None
        for event_id, status in (
            ("exchange-completed", "success"),
            ("exchange-completed", "already_processed"),
            ("exchange-completed-again", "skipped"),
        ):
            self._send_webhook("payment.updated", event_id, {"payment": payment}, status)
            orders = self._exchange_orders()
            self.assertEqual(len(orders), 2)
            replacement = orders - original
            self.assertEqual(replacement.square_order_id, self.exchange_order["id"])
            self.assertEqual(replacement.origin, f"Échange: {original.name}")
            self.assertEqual(replacement.state, "sale")
            self.assertEqual(replacement.partner_id, self.partner)
            self.assertEqual(replacement.warehouse_id, self.warehouse)
            self.assertEqual(replacement.order_line.product_id, self.replacement_product)
            self.assertEqual(replacement.order_line.product_uom_qty, 1)

            self.assertEqual(original.state, "sale")
            self.assertEqual(original.order_line.product_id, self.original_product)
            self.assertEqual(original.order_line.product_uom_qty, 2)
            self.assertAlmostEqual(original.amount_total, 24, places=2)
            self.assertAlmostEqual(original_invoice.amount_residual, 0, places=2)
            self._assert_movement(delivery, "outgoing", self.original_product, 2)

            returned = delivery.return_ids
            self._assert_movement(returned, "incoming", self.original_product, 1)
            self.assertEqual(returned.location_dest_id, self.warehouse.lot_stock_id)
            self.assertEqual(returned.move_ids.origin_returned_move_id, delivery.move_ids)

            replacement_delivery = replacement.picking_ids
            self._assert_movement(replacement_delivery, "outgoing", self.replacement_product, 1)
            self.assertEqual(replacement_delivery.location_id, self.warehouse.lot_stock_id)

            credit = original_invoice.reversal_move_ids
            invoice = replacement.invoice_ids
            self.assertEqual(len(credit), 1)
            self.assertEqual(credit.move_type, "out_refund")
            self.assertEqual(credit.invoice_line_ids.product_id, self.original_product)
            self.assertEqual(credit.invoice_line_ids.quantity, 1)
            self.assertEqual(len(invoice), 1)
            self.assertEqual(
                set(self._invoices().ids), set((original_invoice | credit | invoice).ids)
            )
            self.assertAlmostEqual(credit.amount_total, 12, places=2)
            self.assertAlmostEqual(invoice.amount_total, invoice_total, places=2)
            for document in credit | invoice:
                self.assertEqual(document.state, "posted")
                self.assertAlmostEqual(document.amount_residual, 0, places=2)
            receivables = (credit | invoice).line_ids.filtered(
                lambda line: line.account_type == "asset_receivable"
            )
            self.assertEqual(len(receivables), 2)
            self.assertTrue(all(receivables.mapped("reconciled")))
            self.assertEqual(len(receivables.full_reconcile_id), 1)
            payments = self._payments()
            self.assertIn(original_payment, payments)
            delta_payment = payments - original_payment
            if payment_direction:
                self.assertEqual(len(delta_payment), 1)
                self.assertEqual(delta_payment.partner_id, self.partner)
                self.assertEqual(delta_payment.payment_type, payment_direction)
                self.assertAlmostEqual(delta_payment.amount, delta, places=2)
                self.assertEqual(delta_payment.move_id.state, "posted")
                payment_receivable = delta_payment.move_id.line_ids.filtered(
                    lambda line: line.account_type == "asset_receivable"
                )
                self.assertEqual(payment_receivable.full_reconcile_id, receivables.full_reconcile_id)
            else:
                self.assertFalse(delta_payment)
            self.assertEqual(self._stock_quantities(self.warehouse), (4, 4))
            self.assertEqual(self._stock_quantities(self.other_warehouse), (9, 9))
            processed_records = (replacement, returned, replacement_delivery, credit, invoice, payments)
            if records:
                self.assertEqual(processed_records, records)
            records = processed_records

    def test_equal_exchange_settles_without_an_additional_payment(self):
        self._set_replacement_price(amount=1200, tax=200)
        self._exercise_exchange(payment_amount=0, invoice_total=12)

    def test_exchange_rounds_returnable_stock_after_converting_pending_returns(self):
        self._set_replacement_price(amount=1200, tax=200)
        _, _, _, delivery = self._import_original_sale()
        unit = self.original_product.uom_id
        unit.rounding = 0.01
        subunit = self.env["uom.uom"].create({
            "name": "Test subunit",
            "category_id": unit.category_id.id,
            "uom_type": "smaller",
            "factor": 250,
            "rounding": 1,
        })
        pending = self.env["stock.picking"].create({
            "picking_type_id": self.warehouse.in_type_id.id,
            "location_id": delivery.location_dest_id.id,
            "location_dest_id": delivery.location_id.id,
            "return_id": delivery.id,
        })
        # These draft moves commit 0.504 and 0.492 units of the original delivery.
        # One of the two delivered units can still be exchanged.
        self.env["stock.move"].create([
            {
                "name": f"Pending return of {quantity} subunits",
                "product_id": self.original_product.id,
                "product_uom": subunit.id,
                "product_uom_qty": quantity,
                "location_id": pending.location_id.id,
                "location_dest_id": pending.location_dest_id.id,
                "picking_id": pending.id,
                "origin_returned_move_id": delivery.move_ids.id,
            }
            for quantity in (126, 123)
        ])

        self._send_webhook(
            "payment.updated", "exchange-with-pending-returns",
            {"payment": self._payment_notice("COMPLETED", 0)}, "success",
        )
        self.assertEqual(pending.state, "draft")
        returned = delivery.return_ids - pending
        self._assert_movement(returned, "incoming", self.original_product, 1)
        self.assertEqual(self._stock_quantities(self.warehouse), (4, 4))

    def test_higher_price_exchange_records_only_the_extra_payment(self):
        self._set_replacement_price(amount=1800, tax=300)
        self._exercise_exchange(payment_amount=600, invoice_total=18, payment_direction="inbound", delta=6)

    def test_lower_price_exchange_records_only_the_refund_difference(self):
        self._set_replacement_price(amount=600, tax=100)
        self._exercise_exchange(payment_amount=0, invoice_total=6, payment_direction="outbound", delta=6)

    def test_exchange_waits_for_source_order_and_square_lookup(self):
        self._set_replacement_price(amount=1200, tax=200)
        payment = {"payment": self._payment_notice("COMPLETED", 0)}
        with patch.object(SquareApiClient, "_get_order", return_value=None):
            self._send_webhook("payment.updated", "early-exchange", payment, "retry", http_status=503)
        self._send_webhook("payment.updated", "early-exchange", payment, "retry", http_status=503)
        self.assertFalse(self._exchange_orders())
        self.assertFalse(self._payments())
        _, _, original_payment, delivery = self._import_original_sale()
        for status in ("success", "already_processed"):
            self._send_webhook("payment.updated", "early-exchange", payment, status)
            self.assertEqual(len(self._exchange_orders()), 2)
            self.assertEqual(self._payments(), original_payment)
            self._assert_movement(delivery.return_ids, "incoming", self.original_product, 1)

    def test_failed_exchange_rolls_back_and_the_same_event_can_retry(self):
        self._set_replacement_price(amount=1200, tax=200)
        self.catalog["REPLACEMENT-CATALOG"] = {"success": False, "not_found": True}
        original, invoice, original_payment, delivery = self._import_original_sale()
        payment_notice = {"payment": self._payment_notice("COMPLETED", 0)}
        self._send_webhook("payment.updated", "exchange-completed", payment_notice, "error")
        self._assert_exchange_not_applied(original, invoice, original_payment, delivery)

        self.catalog["REPLACEMENT-CATALOG"] = {"success": True, "sku": "REPLACEMENT-SKU"}
        self._send_webhook("payment.updated", "exchange-completed", payment_notice, "success")
        self.assertEqual(len(self._exchange_orders()), 2)
        self.assertEqual(len(self._invoices()), 3)
        self.assertEqual(self._payments(), original_payment)
        self._assert_movement(delivery.return_ids, "incoming", self.original_product, 1)
        self.assertEqual(self._stock_quantities(self.warehouse), (4, 4))

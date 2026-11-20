# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import base64
import hmac
import json
from unittest import SkipTest
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import uuid4

import requests

from odoo import Command, fields
from odoo.tests import tagged

from .test_checkout import SquareOnlineCheckoutCase


@tagged("post_install", "-at_install")
class TestSquareOnlinePosCoexistence(SquareOnlineCheckoutCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if "square.config" not in cls.env:
            raise SkipTest("Requires the optional Square POS integration")
        cls.pos_journal = cls.env["account.journal"].create({
            "name": "Square POS receipts", "type": "bank", "code": "PSQ",
        })
        cls.env["square.stock.sync"].create({"auto_sync_enabled": False})
        cls.pos = cls.env["square.config"].create({
            "name": "POS for the same Square merchant and location",
            "square_application_id": "sandbox-pos-app",
            "square_access_token": "pos-access-test",
            "square_webhook_signature_key": "pos-signature-test",
            "payment_journal_id": cls.pos_journal.id,
        })
        cls.warehouse = cls.env["stock.warehouse"].search([
            ("company_id", "=", cls.company.id),
        ], limit=1)
        cls.env["square.location.mapping"].create({
            "config_id": cls.pos.id,
            "square_location_id": "SQUARE-NY",
            "square_location_name": "Shared Store",
            "warehouse_id": cls.warehouse.id,
        })
        cls.connection.write({
            "webhook_notification_url": (
                f"https://shop.example.test/payment/square_online/webhook/{cls.connection.id}"
            ),
            "webhook_signature_key": "online-signature-test",
        })
        cls.goods = cls.env["product.product"].create({
            "name": "Shared stock item",
            "default_code": "SHARED-SKU",
            "type": "consu",
            "is_storable": True,
            "list_price": 23.50,
            "invoice_policy": "order",
            "website_published": True,
            "taxes_id": [Command.clear()],
        })
        cls.env["stock.quant"]._update_available_quantity(cls.goods, cls.warehouse.lot_stock_id, 12)
        cls.carrier = cls.env["delivery.carrier"].create({
            "name": "Shared stock delivery",
            "product_id": cls.product.id,
            "delivery_type": "fixed",
            "fixed_price": 0,
            "is_published": True,
        })

    def _notify(self, event_type, data, *, online=False, merchant="MERCHANT_TEST",
                environment="Sandbox", event_id=None):
        path = "/square/webhook"
        url = "https://" + urlsplit(self.base_url()).netloc + path
        key = b"pos-signature-test"
        if online:
            url = self.connection.webhook_notification_url
            path = urlsplit(url).path
            key = b"online-signature-test"
        resource_type = event_type.replace(".", "_") if event_type.startswith("order.") else (
            event_type.partition(".")[0]
        )
        resource_id = data.get("id") or data.get("order_id")
        raw = json.dumps({
            "type": event_type, "merchant_id": merchant, "event_id": event_id,
            "data": {"type": resource_type, "id": resource_id, "object": {resource_type: data}},
        }).encode()
        signature = base64.b64encode(hmac.digest(key, url.encode() + raw, "sha256")).decode()
        result = self.url_open(path, data=raw, headers={
            "Content-Type": "application/json",
            "X-Square-Hmacsha256-Signature": signature,
            "Square-Environment": environment,
        })
        self.env.invalidate_all()
        return result

    def _start_website_goods_checkout(self, square, *, pricelist=None):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order = self._make_order(self.goods)
        if pricelist:
            order.pricelist_id = pricelist
            order.order_line.price_unit = 23.50
        order._set_delivery_method(self.carrier)
        draft = self._prepare(order, self._open_payment_page(order))
        square.side_effect = requests.Timeout
        self.assertEqual(self._submit(draft)["state"], "pending")
        payment = self._completed_payment(*square.call_args.args, **square.call_args.kwargs).json()["payment"]
        payment["application_details"] = {
            "application_id": self.connection.application_id, "square_product": "ECOMMERCE_API",
        }
        self.env.invalidate_all()
        return order, order.transaction_ids, payment

    def _square_order(self, order_id, *, payment_id=None, source_order_id=None, currency="USD"):
        data = {
            "id": order_id, "location_id": "SQUARE-NY", "state": "COMPLETED",
            "total_money": {"amount": 4700, "currency": currency},
            "total_tax_money": {"amount": 0, "currency": currency},
            "line_items": [{
                "uid": "SHARED-LINE", "name": self.goods.name,
                "catalog_object_id": "SHARED-CATALOG", "quantity": "2",
                "total_money": {"amount": 4700, "currency": currency},
            }],
        }
        if payment_id:
            data["tenders"] = [{
                "id": payment_id, "payment_id": payment_id, "type": "CARD",
                "amount_money": {"amount": 4700, "currency": currency},
            }]
        if source_order_id:
            data["returns"] = [{"source_order_id": source_order_id}]
        return data

    def _pos_api(self, url, *, headers, **kwargs):
        self.assertEqual(urlsplit(url).netloc, "connect.squareupsandbox.com")
        self.assertEqual(headers["Authorization"], "Bearer pos-access-test")
        resource = urlsplit(url).path
        if resource in self.pos_objects:
            return self._square_response(self.pos_objects[resource])
        if resource == "/v2/catalog/search":
            return self._square_response({"objects": []})
        if resource.startswith("/v2/catalog/object/"):
            sku = {"SHARED-CATALOG": "SHARED-SKU", "REPLACEMENT-CATALOG": "REPLACEMENT-SKU"}[
                resource.rsplit("/", 1)[1]
            ]
            return self._square_response({"object": {
                "type": "ITEM_VARIATION", "item_variation_data": {"sku": sku},
            }})
        self.fail(f"Unexpected Square POS request: {resource!r}")

    def _order_notification(self, order_id, *, event_type="order.updated", event_id=None):
        return self._notify(event_type, {
            "order_id": order_id, "location_id": "SQUARE-NY", "state": "COMPLETED",
        }, event_id=event_id)

    @patch(SquareOnlineCheckoutCase.square_boundary)
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.get")
    def test_unresolved_website_checkout_does_not_block_independent_pos_drafts(self, pos_api, online_api):
        website_order, transaction, _payment = self._start_website_goods_checkout(online_api)
        self.pos_objects = {}
        pos_api.side_effect = self._pos_api

        def notify(order, event_type, event_id):
            self.pos_objects[f"/v2/orders/{order['id']}"] = {"order": order}
            return self._notify(event_type, {
                "order_id": order["id"], "state": order["state"], "location_id": order["location_id"],
            }, event_id=event_id)

        pos_order = self._square_order("INDEPENDENT-POS-DRAFT")
        pos_order.update(
            state="OPEN", created_at="2025-04-16T10:00:00Z", version=1,
        )
        self.assertEqual(notify(pos_order, "order.created", "pos-open").json()["status"], "success")
        imported = self.env["sale.order"].search([("square_order_id", "=", pos_order["id"])])
        self.assertEqual(imported.state, "draft")
        self.assertEqual(imported.order_line.product_uom_qty, 2)
        pos_order.update(state="DRAFT", version=2)
        pos_order["line_items"][0]["quantity"] = "3"
        self.assertEqual(notify(pos_order, "order.updated", "pos-draft-update").json()["status"], "updated")
        self.assertEqual(imported.order_line.product_uom_qty, 3)
        self.assertEqual(
            notify(pos_order, "order.updated", "pos-draft-update").json()["status"], "already_processed",
        )

        unrelated = self._square_order("UNRELATED-NEW-POS-DRAFT")
        unrelated.update(state="OPEN", total_money={"amount": 2350, "currency": "USD"})
        unrelated["line_items"][0].update(quantity="1", total_money=unrelated["total_money"])
        self.assertEqual(notify(unrelated, "order.created", "new-pos-open").json()["status"], "success")
        self.assertEqual(
            self.env["sale.order"].search([("square_order_id", "=", unrelated["id"])]).amount_total, 23.50,
        )

        candidate = self._square_order("UNVERIFIED-WEBSITE-ORDER")
        candidate["state"] = "OPEN"
        response = notify(candidate, "order.created", "possible-website-order")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["status"], "retry")
        self.assertFalse(self.env["sale.order"].search([("square_order_id", "=", candidate["id"])]))
        self.assertFalse(imported.invoice_ids)
        self.assertFalse(imported.picking_ids)
        self._assert_website_unpaid(website_order, transaction)
        online_api.assert_called_once()

    @patch(SquareOnlineCheckoutCase.square_boundary)
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.get")
    def test_website_return_and_payment_webhooks_cannot_create_pos_records(self, pos_api, online_api):
        order, transaction, payment = self._start_website_goods_checkout(online_api)
        self.assertFalse(transaction.provider_reference)
        self.assertFalse(transaction.square_online_order_reference)
        payments_before = self.env["account.payment"].search([])
        sales_before = self.env["sale.order"].search([])
        refund = {
            "id": "WEBSITE-REFUND", "status": "COMPLETED", "payment_id": payment["id"],
            "order_id": "WEBSITE-RETURN-ORDER", "location_id": payment["location_id"],
            "amount_money": {"amount": 1200, "currency": "USD"},
        }
        self.pos_objects = {
            f"/v2/payments/{payment['id']}": {"payment": payment},
            f"/v2/orders/{payment['order_id']}": {"order": self._square_order(
                payment["order_id"], payment_id=payment["id"],
            )},
            f"/v2/orders/{refund['order_id']}": {"order": self._square_order(
                refund["order_id"], source_order_id=payment["order_id"],
            )},
            f"/v2/refunds/{refund['id']}": {"refund": refund},
        }
        pos_api.side_effect = self._pos_api

        # A metadata-only source event can arrive before an authoritative read is possible.
        pos_api.side_effect = requests.Timeout
        deferred = self._order_notification(payment["order_id"], event_id="SOURCE-ORDER-DELIVERY")
        pos_api.side_effect = self._pos_api
        self.assertEqual(deferred.status_code, 503, deferred.text)
        self.assertEqual(self.env["sale.order"].search([]), sales_before)

        response = self._notify("refund.created", refund)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "ignored", response.text)

        # The distinct return order and the retry of its source both precede online IDs.
        for order_id in (refund["order_id"], payment["order_id"]):
            result = self._order_notification(
                order_id, event_id="SOURCE-ORDER-DELIVERY" if order_id == payment["order_id"] else None,
            )
            self.assertEqual(result.status_code, 200, result.text)
            self.assertEqual(result.json()["status"], "ignored", result.text)
        self.assertEqual(self.env["sale.order"].search([]), sales_before)
        response = self._notify("payment.updated", payment)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "ignored", response.text)
        self.assertFalse(transaction.provider_reference, "POS exclusion must not apply online status")
        self.assertFalse(transaction.child_transaction_ids)
        self.assertEqual(self.env["account.payment"].search([]), payments_before)

        def read_payment(method, url, *, headers, **kwargs):
            self.assertEqual(method, "GET")
            self.assertTrue(url.endswith("/v2/payments/" + payment["id"]))
            self.assertEqual(headers["Authorization"], "Bearer checkout-access-test")
            return self._square_response({"payment": payment})

        online_api.side_effect = read_payment
        for event_type, resource in (
            ("refund.updated", refund), ("payment.updated", payment),
            ("refund.created", {**refund, "status": "PENDING"}), ("refund.updated", refund),
        ):
            self.assertEqual(self._notify(event_type, resource, online=True).status_code, 200)
            self.assertEqual(self._notify(event_type, resource).json()["status"], "ignored")
        for order_id in (payment["order_id"], refund["order_id"]):
            replay = self._order_notification(order_id, event_type="order.created")
            self.assertEqual(replay.json()["status"], "ignored")

        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.ref("payment_square_online.cron_square_online_recover_status").method_direct_trigger()
        self.env.invalidate_all()
        child = transaction.child_transaction_ids
        self.assertEqual(order.state, "sale")
        self.assertEqual(len(order.transaction_ids), 1)
        self.assertEqual(transaction.provider_reference, payment["id"])
        self.assertEqual(child.provider_reference, refund["id"])
        self.assertEqual(child.square_online_order_reference, refund["order_id"])
        self.assertEqual(
            self.env["account.payment"].search([]) - payments_before,
            transaction.payment_id | child.payment_id,
        )
        self.assertEqual(transaction.payment_id.amount, 47)
        self.assertEqual(transaction.payment_id.payment_type, "inbound")
        self.assertEqual(child.payment_id.amount, 12)
        self.assertEqual(child.payment_id.payment_type, "outbound")
        self.assertEqual(child.payment_id.source_payment_id, transaction.payment_id)
        self.assertEqual((transaction.payment_id | child.payment_id).move_id.mapped("state"), ["posted", "posted"])
        self.assertEqual(order.invoice_ids.mapped("move_type"), ["out_invoice"])
        self.assertEqual(order.invoice_ids.amount_residual, 0)
        self.assertFalse(order.square_order_id or order.square_payment_id)
        self.assertTrue(order.picking_ids)
        self.assertNotIn("done", order.picking_ids.mapped("state"))
        self.assertEqual(order.order_line.filtered(lambda line: not line.is_delivery).qty_delivered, 0)
        self.assertEqual(self._physical_stock(self.goods), 12)
        self.assertFalse(self.env["square.refund"].search([("square_refund_id", "=", refund["id"])]))
        self.assertEqual(self.env["sale.order"].search([]), sales_before)
        self.assertEqual([call.args[0] for call in online_api.call_args_list].count("POST"), 1)

    @patch(SquareOnlineCheckoutCase.square_boundary)
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.get")
    def test_website_terminal_and_square_register_keep_their_own_sales(self, pos_api, online_api):
        if "pos.square.order" not in self.env:
            self.skipTest("Square Terminal is not installed")
        website, transaction, payment = self._start_website_goods_checkout(online_api)
        config = self.env["pos.config"].create({"name": "Terminal coexistence", "company_id": self.company.id})
        session = self.env["pos.session"].create({"config_id": config.id})
        pos_order = self.env["pos.order"].create({
            "uuid": str(uuid4()), "session_id": session.id, "state": "draft",
            "amount_tax": 0, "amount_total": 47, "amount_return": 0, "amount_paid": 0,
        })
        connection = self.env["pos.square.connection"].sudo().create({
            "name": "Terminal at shared store", "company_id": self.company.id, "environment": "sandbox",
            "application_id": "sandbox-sq0idb-terminal-app", "merchant_id": "MERCHANT_TEST",
        })
        location = self.env["pos.square.location"].sudo().create({
            "connection_id": connection.id, "square_id": "SQUARE-NY", "name": "Shared Store",
            "currency_code": "USD",
        })
        terminal = self._square_order("TERMINAL-ORDER")
        terminal["reference_id"] = "odoo-" + uuid4().hex
        self.env["pos.square.order"].sudo().create({
            "order_id": pos_order.id, "order_uuid": pos_order.uuid,
            "company_id": self.company.id, "connection_id": connection.id, "environment": "sandbox",
            "merchant_id": "MERCHANT_TEST", "location_id": location.id, "square_location_id": "SQUARE-NY",
            "reference": terminal["reference_id"], "currency_code": "USD", "currency_digits": 2,
            "total_minor": 4700, "create_request": {"order": terminal}, "square_order_id": terminal["id"],
        })
        register_payment = {
            "id": "REGISTER-PAYMENT", "order_id": "REGISTER-ORDER", "location_id": "SQUARE-NY",
            "status": "COMPLETED", "amount_money": {"amount": 4700, "currency": "USD"},
        }
        register = self._square_order(register_payment["order_id"], payment_id=register_payment["id"])
        website_order = self._square_order(payment["order_id"], payment_id=payment["id"])
        self.pos_objects = {
            f"/v2/orders/{order['id']}": {"order": order} for order in (terminal, register, website_order)
        }
        self.pos_objects.update({
            f"/v2/payments/{item['id']}": {"payment": item} for item in (payment, register_payment)
        })
        self.pos_objects["/v2/locations/SQUARE-NY"] = {
            "location": {"id": "SQUARE-NY", "merchant_id": "MERCHANT_TEST"},
        }
        pos_api.side_effect = self._pos_api

        self.assertEqual(self._notify("payment.updated", payment).json()["status"], "ignored")
        # Website and Terminal ownership must use one Square response per order
        # for this delivery, including missing reads and a return's original sale.
        terminal_return = self._square_order("TERMINAL-RETURN", source_order_id=terminal["id"])
        return_path = "/v2/orders/" + terminal_return["id"]
        original_path = "/v2/orders/" + terminal["id"]

        def retry_return():
            pos_api.reset_mock()
            response = self._order_notification(terminal_return["id"], event_id="return-retry")
            return response, [urlsplit(call.args[0]).path for call in pos_api.call_args_list]

        for missing_path, order, expected_reads in (
            (return_path, terminal_return, [return_path]),
            (original_path, terminal, [return_path, original_path]),
        ):
            self.pos_objects[missing_path] = {}
            response, reads = retry_return()
            self.assertEqual(response.status_code, 503, response.text)
            self.assertEqual(reads, expected_reads)
            self.pos_objects[missing_path] = {"order": order}

        response, reads = retry_return()
        ignored_terminal = {"status": "ignored", "message": "Odoo POS Square Terminal operation"}
        self.assertEqual(response.json(), ignored_terminal, response.text)
        self.assertEqual(reads.count(return_path), 1)
        self.assertEqual(reads.count(original_path), 1)

        response = self._notify("order.created", terminal, event_id="terminal-order")
        self.assertEqual(response.json(), ignored_terminal, response.text)
        response = self._notify("order.created", register, event_id="register-order")
        self.assertEqual(response.json()["status"], "success", response.text)
        self.assertFalse(transaction.provider_reference)

        def read_payment(method, url, *, headers, **kwargs):
            self.assertEqual(method, "GET")
            self.assertTrue(url.endswith("/v2/payments/" + payment["id"]))
            return self._square_response({"payment": payment})

        online_api.side_effect = read_payment
        self.assertEqual(self._notify("payment.updated", payment, online=True).status_code, 200)
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.assertEqual(website.state, "sale")
        self.assertEqual(transaction.payment_id.amount, 47)
        imported = self.env["sale.order"].search([
            ("square_order_id", "in", [order["id"] for order in (terminal, register, website_order)]),
        ])
        self.assertEqual(imported.square_order_id, register["id"])
        self.assertEqual(imported.invoice_ids.reconciled_payment_ids.amount, 47)
        self.assertEqual(imported.picking_ids.state, "done")
        self.assertEqual(self._physical_stock(self.goods), 10)

    def _physical_stock(self, product):
        return sum(self.env["stock.quant"].search([
            ("product_id", "=", product.id),
            ("location_id", "=", self.warehouse.lot_stock_id.id),
        ]).mapped("quantity"))

    def _start_overlapping_eur_sale(self, pos_api, online_api):
        # The existing POS refund implementation supports EUR. Both routes use the
        # same real Odoo company, Square merchant, environment and location here.
        eur = self.env.ref("base.EUR")
        eur.active = True
        self.company.currency_id = eur
        self.connection.location_id.write({"country_code": "FR", "currency_code": "EUR"})
        pricelist = self.env["product.pricelist"].create({
            "name": "Shared Square EUR",
            "currency_id": eur.id,
            "company_id": self.company.id,
            "website_id": self.website.id,
        })
        self.partner.property_product_pricelist = pricelist
        website_order, transaction, _ = self._start_website_goods_checkout(online_api, pricelist=pricelist)
        self.assertFalse(transaction.provider_reference)
        self.assertEqual(website_order.currency_id, eur)

        self.pos_payment = {
            "id": "POS-PAYMENT", "order_id": "POS-ORDER", "status": "COMPLETED",
            "location_id": "SQUARE-NY", "source_type": "CARD",
            "reference_id": "other-square-checkout",
            "application_details": {"square_product": "ECOMMERCE_API"},
            "amount_money": {"amount": 4700, "currency": "EUR"},
        }
        self.pos_order = self._square_order("POS-ORDER", payment_id="POS-PAYMENT", currency="EUR")
        self.pos_order["fulfillments"] = [{
            "pickup_details": {"recipient": {"email_address": self.partner.email}},
        }]
        self.pos_objects = {
            "/v2/orders/POS-ORDER": {"order": self.pos_order},
            "/v2/payments/POS-PAYMENT": {"payment": self.pos_payment},
        }
        pos_api.side_effect = self._pos_api
        self.enterContext(patch(
            "odoo.addons.odoo_square.models.square_api_client.requests.post",
            side_effect=self._pos_api,
        ))
        online_api.side_effect = self._read_pos_payment_through_online_api

        for event_type, data in (
            ("order.updated", {"order_id": "POS-ORDER", "location_id": "SQUARE-NY", "state": "COMPLETED"}),
            ("payment.updated", self.pos_payment),
        ):
            self.assertEqual(self._notify(event_type, data, online=True).status_code, 200)
            response = self._notify(event_type, data, event_id=f"POS-PAID-{event_type}")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["status"], "success", response.text)
        pos_order = self.env["sale.order"].search([("square_order_id", "=", "POS-ORDER")])
        self.assertEqual(len(pos_order), 1)
        self.assertEqual(pos_order.state, "sale")
        self.assertEqual(pos_order.square_payment_id, "POS-PAYMENT")
        invoice = pos_order.invoice_ids
        self.assertEqual(invoice.amount_total, 47)
        self.assertEqual(invoice.amount_residual, 0)
        self.assertEqual(invoice.state, "posted")
        paid = invoice.reconciled_payment_ids
        self.assertEqual(len(paid), 1)
        self.assertEqual(paid.payment_type, "inbound")
        self.assertEqual(paid.amount, 47)
        self.assertEqual(paid.currency_id, eur)
        self.assertEqual(paid.journal_id, self.pos_journal)
        self.assertEqual(pos_order.picking_ids.state, "done")
        self.assertEqual(pos_order.picking_ids.move_ids.quantity, 2)
        self.assertEqual(self._physical_stock(self.goods), 10)
        return website_order, transaction, pos_order

    def _read_pos_payment_through_online_api(self, method, url, *, headers, **kwargs):
        self.assertEqual(method, "GET")
        self.assertEqual(headers["Authorization"], "Bearer checkout-access-test")
        resource = urlsplit(url).path
        self.assertIn(resource, ("/v2/payments/POS-PAYMENT", "/v2/payments/POS-EXCHANGE-PAYMENT"))
        return self._square_response(self.pos_objects[resource])

    def _assert_website_unpaid(self, order, transaction):
        self.assertEqual(order.state, "draft")
        self.assertEqual(order.transaction_ids, transaction)
        self.assertEqual(transaction.state, "pending")
        self.assertFalse(transaction.provider_reference or transaction.payment_id or transaction.child_transaction_ids)
        self.assertFalse(order.invoice_ids or order.picking_ids)

    def _assert_pos_refund(self, pos_api, online_api, *, full):
        website_order, transaction, pos_order = self._start_overlapping_eur_sale(pos_api, online_api)
        invoice = pos_order.invoice_ids
        payments_before = self.env["account.payment"].search([])
        amount = 4700 if full else 2350
        units = 2 if full else 1
        refund = {
            "id": "POS-REFUND", "order_id": "POS-RETURN", "payment_id": self.pos_payment["id"],
            "location_id": self.pos_payment["location_id"], "status": "PENDING",
            "amount_money": {"amount": amount, "currency": "EUR"},
        }
        # POS can identify this refund by its separate return order or payment.
        self.pos_objects["/v2/orders/POS-RETURN"] = {"order": {
            "id": "POS-RETURN", "state": "COMPLETED", "location_id": "SQUARE-NY",
            "returns": [{"source_order_id": "POS-ORDER"}],
        }}
        for event_type, status in (
            ("refund.created", "PENDING"), ("refund.updated", "COMPLETED"), ("refund.updated", "COMPLETED"),
        ):
            resource = {**refund, "status": status}
            self.assertEqual(self._notify(event_type, resource, online=True).status_code, 200)
            response = self._notify(event_type, resource, event_id=f"POS-RETURN-{event_type}")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn(response.json()["status"], ("success", "already_processed"), response.text)
        reversal = invoice.reversal_move_ids
        payments = self.env["account.payment"].search([]) - payments_before
        self.assertEqual(len(payments), 1)
        self.assertEqual(payments.payment_type, "outbound")
        self.assertEqual(payments.amount, amount / 100)
        self.assertEqual(payments.move_id.state, "posted")
        self.assertEqual(pos_order.square_refund_ids.status, "completed")
        self.assertEqual(pos_order.square_refund_ids.return_picking_ids.state, "done")
        self.assertEqual(pos_order.square_refund_ids.return_picking_ids.move_ids.quantity, units)
        self.assertEqual(self._physical_stock(self.goods), 10 + units)
        self.assertEqual(len(reversal), 1)
        self.assertEqual(reversal.state, "posted")
        self.assertEqual(reversal.amount_total, amount / 100)
        self.assertEqual(reversal.amount_residual, 0)
        self._assert_website_unpaid(website_order, transaction)

    @patch(SquareOnlineCheckoutCase.square_boundary)
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.get")
    def test_shared_location_preserves_full_pos_refunds(self, pos_api, online_api):
        self._assert_pos_refund(pos_api, online_api, full=True)

    @patch(SquareOnlineCheckoutCase.square_boundary)
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.get")
    def test_shared_location_preserves_partial_pos_refunds(self, pos_api, online_api):
        self._assert_pos_refund(pos_api, online_api, full=False)

    @patch(SquareOnlineCheckoutCase.square_boundary)
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.get")
    def test_shared_location_preserves_pos_exchange(self, pos_api, online_api):
        website_order, transaction, pos_order = self._start_overlapping_eur_sale(pos_api, online_api)
        self.env["account.tax"].create({
            "name": "20% Square POS VAT", "company_id": self.company.id,
            "amount": 20, "amount_type": "percent",
            "type_tax_use": "sale", "price_include_override": "tax_excluded",
        })
        replacement = self.env["product.product"].create({
            "name": "POS Replacement", "default_code": "REPLACEMENT-SKU",
            "type": "consu", "is_storable": True, "list_price": 23.50,
            "invoice_policy": "order", "taxes_id": [Command.clear()],
        })
        self.env["stock.quant"]._update_available_quantity(replacement, self.warehouse.lot_stock_id, 8)
        exchange = self._square_order(
            "POS-EXCHANGE", payment_id="POS-EXCHANGE-PAYMENT", source_order_id="POS-ORDER", currency="EUR",
        )
        exchange["returns"][0]["return_line_items"] = [{
            "catalog_object_id": "SHARED-CATALOG", "quantity": "1",
        }]
        exchange["line_items"] = [{
            "uid": "REPLACEMENT-LINE", "name": replacement.name,
            "catalog_object_id": "REPLACEMENT-CATALOG", "quantity": "1",
            "base_price_money": {"amount": 2350, "currency": "EUR"},
            "total_money": {"amount": 2350, "currency": "EUR"},
        }]
        exchange_payment = {
            **self.pos_payment, "id": "POS-EXCHANGE-PAYMENT", "order_id": exchange["id"],
            "reference_id": transaction.square_online_operation_key,
            "application_details": {"application_id": "sandbox-pos-app", "square_product": "ECOMMERCE_API"},
            "amount_money": {"amount": 0, "currency": "EUR"},
        }
        self.pos_objects.update({
            "/v2/orders/POS-EXCHANGE": {"order": exchange},
            "/v2/payments/POS-EXCHANGE-PAYMENT": {"payment": exchange_payment},
        })
        payments_before = self.env["account.payment"].search([])
        self.assertEqual(self._notify("payment.updated", exchange_payment, online=True).status_code, 200)
        for _delivery in range(2):
            response = self._notify("payment.updated", exchange_payment, event_id="POS-EXCHANGE-EVENT")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn(response.json()["status"], ("success", "already_processed"), response.text)
        sale = self.env["sale.order"].search([("square_order_id", "=", "POS-EXCHANGE")])
        self.assertEqual(len(sale), 1)
        self.assertEqual(sale.state, "sale")
        self.assertEqual(sale.invoice_ids.amount_total, 23.50)
        self.assertEqual(sale.invoice_ids.amount_residual, 0.0)
        self.assertEqual(sale.invoice_ids.state, "posted")
        self.assertEqual(pos_order.invoice_ids.reversal_move_ids.amount_total, 23.50)
        self.assertEqual(pos_order.invoice_ids.reversal_move_ids.amount_residual, 0.0)
        self.assertEqual(pos_order.picking_ids.return_ids.move_ids.quantity, 1)
        self.assertEqual(sale.picking_ids.state, "done")
        self.assertEqual(sale.picking_ids.move_ids.quantity, 1)
        self.assertEqual(self._physical_stock(self.goods), 11)
        self.assertEqual(self._physical_stock(replacement), 7)
        self.assertEqual(self.env["account.payment"].search([]), payments_before)
        self._assert_website_unpaid(website_order, transaction)

    def _pos_action(self, model, method, args, *, button=False):
        endpoint = "call_button" if button else "call_kw"
        result = self.make_jsonrpc_request(
            f"/web/dataset/{endpoint}/{model}/{method}",
            {"model": model, "method": method, "args": args, "kwargs": {}},
        )
        self.env.invalidate_all()
        return result

    @patch(SquareOnlineCheckoutCase.square_boundary)
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.get")
    @patch("odoo.addons.odoo_square.models.square_api_client.requests.post")
    def test_pos_resync_and_existing_queue_cannot_reimport_website_orders(self, pos_post, pos_get, online_api):
        order, transaction, payment = self._start_website_goods_checkout(online_api)
        sale_orders = self.env["sale.order"].search([])
        payments = self.env["account.payment"].search([])
        square_order = self._square_order(payment["order_id"], payment_id=payment["id"])
        square_order["created_at"] = fields.Datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.pos_objects = {
            f"/v2/orders/{square_order['id']}": {"order": square_order},
            f"/v2/payments/{payment['id']}": {"payment": payment},
            "/v2/orders/search": {"orders": [square_order]},
        }
        pos_get.side_effect = pos_post.side_effect = self._pos_api
        self.authenticate("admin", "admin")

        wizard_model = "square.manual.resync.wizard"
        wizard = self.env[wizard_model].browse(self._pos_action(
            wizard_model, "create", [{"config_id": self.pos.id}],
        ))
        self._pos_action(wizard_model, "action_scan_missing_orders", [wizard.ids], button=True)
        self.assertEqual(wizard.square_total, 0)
        self.assertFalse(wizard.line_ids, "A scan must not offer a website order for POS import")

        # A previously selected line or queue entry must be checked again at execution.
        self._pos_action("square.manual.resync.line", "create", [{
            "wizard_id": wizard.id, "square_order_id": square_order["id"], "selected": True,
        }])
        self._pos_action(wizard_model, "action_validate_resync", [wizard.ids], button=True)
        self.assertEqual(wizard.state, "done")
        self.assertEqual((wizard.processed_count, wizard.error_count), (0, 0))
        queue = self.env["square.webhook.queue"].create({
            "webhook_event_id": "OLD-POS-WEBSITE-NOTIFICATION",
            "event_type": "order.created", "depends_on_order": False,
            "square_order_id": square_order["id"], "order_data": json.dumps({
                "order_id": square_order["id"], "state": "COMPLETED", "location_id": "SQUARE-NY",
            }),
            "next_retry_time": fields.Datetime.now(),
        })
        self._pos_action("square.webhook.queue", "process_pending_events", [])
        self.assertEqual(queue.state, "failed")
        self.assertEqual(self.env["sale.order"].search([]), sale_orders)
        self.assertEqual(self.env["account.payment"].search([]), payments)
        self.assertFalse(self.env["square.refund"].search([]))
        self.assertEqual(self._physical_stock(self.goods), 12)
        self._assert_website_unpaid(order, transaction)

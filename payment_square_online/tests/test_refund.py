# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import base64
from datetime import UTC, datetime, timedelta
import hmac
import json
from unittest.mock import patch

import requests

from odoo import Command, fields
from odoo.tests import tagged
from odoo.tests.common import JsonRpcException, new_test_user
from odoo.tools.safe_eval import safe_eval

from .test_checkout import SquareOnlineCheckoutCase


@tagged("post_install", "-at_install")
class TestSquareOnlineRefund(SquareOnlineCheckoutCase):
    square_boundary = SquareOnlineCheckoutCase.square_boundary

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.manager = new_test_user(
            cls.env, login="square-refund-manager", groups="account.group_account_manager",
            company_id=cls.company.id, company_ids=[Command.set(cls.company.ids)],
        )

    def _capture(self, external, order=None):
        order = order or self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))
        external.side_effect = self._completed_payment
        self.assertEqual(self._submit(draft)["state"], "done")
        charge = self._completed_payment(*external.call_args.args, **external.call_args.kwargs)
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()
        return order, order.transaction_ids, charge.json()["payment"]

    def _dispatch(self):
        self.env.ref("payment_square_online.cron_square_online_dispatch_refunds").method_direct_trigger()
        self.env.invalidate_all()

    def _make_refundable_goods_order(self):
        goods = self.env["product.product"].create({
            "name": "Square refundable physical goods",
            "type": "consu",
            "is_storable": True,
            "list_price": 23.50,
            "website_published": True,
            "taxes_id": [Command.clear()],
        })
        order = self._make_order(goods)
        carrier = self.env["delivery.carrier"].create({
            "name": "Square refund shipping",
            "product_id": self.product.id,
            "delivery_type": "fixed",
            "fixed_price": 0,
            "is_published": True,
        })
        order._set_delivery_method(carrier)
        return order

    def _refund_notification(
        self, refund, *, signing_key="refund-webhook-test-key", event_type="refund.updated",
        merchant="MERCHANT_TEST", environment="Sandbox", valid_signature=True,
    ):
        path = f"/payment/square_online/webhook/{self.connection.id}"
        url = "https://shop.example.test" + path
        self.connection.write({
            "webhook_notification_url": url,
            "webhook_signature_key": signing_key,
        })
        raw = json.dumps({
            "type": event_type,
            "merchant_id": merchant,
            "data": {"type": "refund", "id": refund["id"], "object": {"refund": refund}},
        }).encode()
        key = signing_key if valid_signature else "incorrect-key"
        signature = base64.b64encode(hmac.digest(key.encode(), url.encode() + raw, "sha256"))
        return self.url_open(path, data=raw, headers={
            "Content-Type": "application/json",
            "X-Square-Hmacsha256-Signature": signature.decode(),
            "Square-Environment": environment,
        })

    @patch(square_boundary)
    def test_square_dashboard_partial_refund_updates_history_and_accounts_once(self, external):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order, tx, original = self._capture(external, self._make_refundable_goods_order())
        invoice = order.invoice_ids
        pickings = order.picking_ids
        self.assertTrue(invoice and pickings)
        refund = {
            "id": "DASHBOARD-PARTIAL", "order_id": "SQUARE-RETURN-ORDER",
            "payment_id": original["id"], "location_id": original["location_id"],
            "status": "PENDING", "amount_money": {"amount": 1234, "currency": "USD"},
            "reason": "Customer requested an adjustment",
        }
        external.reset_mock()
        self.assertEqual(self._refund_notification(refund, event_type="refund.created").status_code, 200)
        self.env.invalidate_all()
        child = tx.child_transaction_ids
        self.assertEqual(child.state, "pending")
        self.assertFalse(child.payment_id)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 34.66)

        completed = {**refund, "status": "COMPLETED"}
        for event in (completed, refund, completed):
            self.assertEqual(self._refund_notification(event).status_code, 200)
        self.env.ref("payment_square_online.cron_square_online_recover_status").method_direct_trigger()
        self.env.invalidate_all()
        external.assert_not_called()
        self.assertEqual(tx.child_transaction_ids, child)
        self.assertEqual(child.state, "done")
        self.assertEqual(child.provider_reference, "DASHBOARD-PARTIAL")
        self.assertEqual(child.square_online_order_reference, "SQUARE-RETURN-ORDER")
        self.assertEqual(child.square_online_refund_origin, "square")
        self.assertFalse(child.square_online_refund_requested_by)
        self.assertFalse(child.square_online_dispatch_at)
        self.assertFalse(child.invoice_ids)
        self.assertFalse(child.sale_order_ids)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 34.66)
        payment = child.payment_id
        self.assertEqual(payment.payment_type, "outbound")
        self.assertEqual(payment.amount, 12.34)
        self.assertEqual(payment.currency_id, tx.currency_id)
        self.assertEqual(payment.journal_id, self.journal)
        self.assertEqual(payment.source_payment_id, tx.payment_id)
        self.assertEqual(payment.move_id.state, "posted")
        self.assertEqual(sum(payment.move_id.line_ids.mapped("debit")), 12.34)
        self.assertEqual(sum(payment.move_id.line_ids.mapped("credit")), 12.34)
        self.assertEqual(order.invoice_ids, invoice)
        self.assertEqual(invoice.amount_residual, 0)
        self.assertEqual(order.picking_ids, pickings)
        self.assertTrue(all(picking.state != "done" for picking in pickings))

    @patch(square_boundary)
    def test_square_dashboard_full_refund_arrives_before_the_original_payment_response(self, external):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order = self._make_refundable_goods_order()
        draft = self._prepare(order, self._open_payment_page(order))
        external.side_effect = requests.Timeout
        self.assertEqual(self._submit(draft)["state"], "pending")
        original = self._completed_payment(*external.call_args.args, **external.call_args.kwargs).json()["payment"]
        self.env.invalidate_all()
        tx = order.transaction_ids
        self.assertFalse(tx.provider_reference)
        self.assertFalse(tx.payment_id)
        refund = {
            "id": "DASHBOARD-FULL", "order_id": "DASHBOARD-FULL-RETURN-ORDER",
            "payment_id": original["id"], "location_id": original["location_id"],
            "status": "COMPLETED", "amount_money": original["amount_money"],
        }
        external.reset_mock()
        external.side_effect = None
        self.assertEqual(self._refund_notification(refund, valid_signature=False).status_code, 403)
        self.assertEqual(self._refund_notification(refund, merchant="OTHER-MERCHANT").status_code, 200)
        self.assertEqual(self._refund_notification(refund, environment="Production").status_code, 200)
        self.assertEqual(self._refund_notification({
            **refund, "location_id": "OTHER-LOCATION",
        }).status_code, 200)
        external.assert_not_called()

        def read_square(method, url, *, headers, **kwargs):
            self.assertEqual(method, "GET")
            self.assertEqual(headers["Authorization"], "Bearer checkout-access-test")
            if url.endswith("/POS-PAYMENT"):
                return self._square_response({"payment": {
                    **original, "id": "POS-PAYMENT", "reference_id": "f" * 32,
                }})
            self.assertTrue(url.endswith("/v2/payments/" + original["id"]), url)
            return self._square_response({"payment": original})

        external.side_effect = read_square
        self.assertEqual(self._refund_notification({
            **refund, "id": "POS-REFUND", "payment_id": "POS-PAYMENT",
        }).status_code, 200)
        self.env.invalidate_all()
        self.assertFalse(tx.child_transaction_ids)
        self.assertFalse(tx.provider_reference)
        self.assertEqual(self._refund_notification(refund).status_code, 200)
        self.env.invalidate_all()
        child = tx.child_transaction_ids
        self.assertEqual(child.state, "done")
        self.assertEqual(tx.provider_reference, original["id"])
        self.assertEqual(tx.square_online_order_reference, original["order_id"])
        self.assertEqual(child.square_online_order_reference, refund["order_id"])
        self.assertFalse(tx.payment_id)
        # Exercise native post-processing with the refund selected before the capture.
        child.sudo().action_post_process()
        self.env.invalidate_all()
        self.assertEqual(order.state, "sale")
        self.assertEqual(tx.payment_id.payment_type, "inbound")
        self.assertEqual(tx.payment_id.amount, 47)
        self.assertEqual(tx.payment_id.move_id.state, "posted")
        self.assertEqual(tx.payment_id.amount_available_for_refund, 0)
        self.assertEqual(child.payment_id.payment_type, "outbound")
        self.assertEqual(child.payment_id.amount, 47)
        self.assertEqual(child.payment_id.move_id.state, "posted")
        self.assertEqual(child.payment_id.source_payment_id, tx.payment_id)
        self.assertEqual(order.invoice_ids.mapped("move_type"), ["out_invoice"])
        self.assertEqual(order.invoice_ids.amount_residual, 0)
        self.assertTrue(order.picking_ids)
        self.assertTrue(all(picking.state != "done" for picking in order.picking_ids))
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "GET"])

    @patch(square_boundary)
    def test_refresh_discovers_a_distinct_square_refund_for_the_same_amount_as_an_odoo_refund(self, external):
        _, tx, original = self._capture(external)
        self.authenticate(self.manager.login, self.manager.login)
        self._call(
            "payment.refund.wizard", "action_refund", self._open_refund(tx.payment_id, 15), button=True,
        )
        self.env.invalidate_all()
        local_refund = tx.child_transaction_ids
        external.side_effect = [
            self._square_response({"payment": original}),
            requests.Timeout,
            self._square_response({"payment": original}),
            self._square_response({"refunds": []}),
        ]
        self._dispatch()
        self.assertEqual(local_refund.state, "pending")
        self.assertFalse(local_refund.provider_reference)
        external.reset_mock()

        refund = {
            "id": "EXTERNAL-SAME-AMOUNT", "payment_id": original["id"], "location_id": original["location_id"],
            "status": "PENDING", "amount_money": {"amount": 1500, "currency": "USD"},
            "reason": "Adjustment made at Square",
        }
        external.side_effect = [
            self._square_response({"payment": {
                **original, "refund_ids": [refund["id"]], "refunded_money": refund["amount_money"],
            }}),
            self._square_response({"refund": refund}),
        ]
        self.assertEqual(self._call(
            "payment.transaction", "action_square_online_refresh_status", tx.ids, button=True,
        )["tag"], "reload")
        self.env.invalidate_all()
        square_refund = tx.child_transaction_ids - local_refund
        self.assertEqual(square_refund.provider_reference, refund["id"])
        self.assertEqual(square_refund.state, "pending")
        self.assertEqual(square_refund.square_online_refund_origin, "square")
        self.assertFalse(square_refund.square_online_refund_requested_by)
        self.assertFalse(square_refund.square_online_dispatch_at)
        self.assertFalse(local_refund.provider_reference)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 17)
        history = self.env.ref("payment_square_online.action_square_online_transaction_history")
        domain = safe_eval(history.domain) + [("id", "in", [tx.id, square_refund.id, local_refund.id])]
        self.assertCountEqual(
            self._call("payment.transaction", "search", domain),
            [tx.id, square_refund.id, local_refund.id],
        )
        other_company = self.env["res.company"].create({"name": "Other Refund Company"})
        other_manager = new_test_user(
            self.env, login="other-refund-manager", groups="account.group_account_manager",
            company_id=other_company.id, company_ids=[Command.set(other_company.ids)],
        )
        self.authenticate(other_manager.login, other_manager.login)
        self.assertFalse(self._call("payment.transaction", "search", domain))
        with self.assertRaises(JsonRpcException):
            self._call("payment.transaction", "action_square_online_refresh_status", square_refund.ids, button=True)

        external.side_effect = [self._square_response({"refund": {**refund, "status": "REJECTED"}})]
        self.env.ref("payment_square_online.cron_square_online_recover_status").method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(square_refund.state, "error")
        self.assertEqual(local_refund.state, "pending")
        self.assertFalse(square_refund.payment_id)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 32)
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "GET", "GET"])
        self.assertTrue(external.call_args.args[1].endswith("/v2/refunds/" + refund["id"]))

    @patch(square_boundary)
    def test_scheduled_discovery_pages_through_square_refunds_when_no_webhook_arrives(self, external):
        _, tx, original = self._capture(external)
        refund = {
            "id": "EXTERNAL-WEBHOOK-MISSED", "order_id": "EXTERNAL-WEBHOOK-MISSED-ORDER",
            "payment_id": original["id"], "location_id": original["location_id"],
            "status": "COMPLETED", "amount_money": {"amount": 987, "currency": "USD"},
        }
        wrong_location = {**refund, "id": "OTHER-LOCATION-REFUND", "location_id": "OTHER-LOCATION"}
        pos = {**refund, "id": "POS-REFUND", "payment_id": "POS-PAYMENT"}
        external.reset_mock()
        external.side_effect = [
            self._square_response({"refunds": [wrong_location, pos], "cursor": "second-page"}),
            self._square_response({"refunds": [refund]}),
        ]
        self.env.ref("payment_square_online.cron_square_online_discover_refunds").method_direct_trigger()
        self.env.ref("payment_square_online.cron_square_online_recover_status").method_direct_trigger()
        self.env.invalidate_all()
        child = tx.child_transaction_ids
        self.assertEqual(child.provider_reference, refund["id"])
        self.assertEqual(child.state, "done")
        self.assertEqual(child.payment_id.amount, 9.87)
        self.assertEqual(child.payment_id.move_id.state, "posted")
        self.assertEqual(tx.payment_id.amount_available_for_refund, 37.13)
        self.assertEqual([call.args for call in external.call_args_list], [
            ("GET", "https://connect.squareupsandbox.com/v2/refunds"),
            ("GET", "https://connect.squareupsandbox.com/v2/refunds"),
        ])
        self.assertEqual(external.call_args.kwargs["params"]["cursor"], "second-page")
        self.assertEqual(external.call_args.kwargs["params"]["location_id"], original["location_id"])
        self.assertEqual(external.call_args.kwargs["headers"]["Authorization"], "Bearer checkout-access-test")
        self.assertEqual(self._refund_notification(refund).status_code, 200)
        self.env.invalidate_all()
        self.assertEqual(tx.child_transaction_ids, child)
        self.assertEqual(external.call_count, 2)

    @patch(square_boundary)
    def test_successive_partial_refunds_and_default_remaining_amount(self, external):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order, tx, original = self._capture(external, self._make_refundable_goods_order())
        invoice = order.invoice_ids
        pickings = order.picking_ids
        self.assertTrue(invoice)
        self.assertTrue(pickings)
        self.authenticate(self.manager.login, self.manager.login)
        refunds = {}

        def square(method, url, *, headers, json, **kwargs):
            if method == "GET" and url.endswith("/v2/payments/" + original["id"]):
                return self._square_response({"payment": {
                    **original,
                    "refunded_money": {"amount": sum(refunded), "currency": "USD"},
                    "refund_ids": list(refunds),
                }})
            if method == "GET":
                return self._square_response({"refund": refunds[url.rsplit("/", 1)[-1]]})
            self.assertEqual((method, url), ("POST", "https://connect.squareupsandbox.com/v2/refunds"))
            self.assertEqual(json["payment_id"], original["id"])
            self.assertEqual(json["amount_money"], {"amount": expected_minor, "currency": "USD"})
            refund_id = f"PARTIAL-{len(refunds)}"
            refunds[refund_id] = {
                "id": refund_id, "payment_id": original["id"], "location_id": original["location_id"],
                "reason": json["reason"], "status": "COMPLETED", "amount_money": json["amount_money"],
            }
            refunded.append(expected_minor)
            return self._square_response({"refund": refunds[refund_id]})

        refunded = []
        external.side_effect = square
        for amount, expected_minor, remaining in ((12.31, 1231, 34.69), (8.22, 822, 26.47), (None, 2647, 0)):
            wizard_ids = self._open_refund(tx.payment_id, amount)
            wizard, = self._call("payment.refund.wizard", "read", wizard_ids, fields=["amount_to_refund"])
            self.assertEqual(wizard["amount_to_refund"], expected_minor / 100)
            self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
            self._dispatch()
            self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(tx.payment_id.amount_available_for_refund, remaining)
        children = tx.child_transaction_ids
        self.assertEqual(set(children.mapped("provider_reference")), set(refunds))
        self.assertEqual(children.mapped("square_online_refund_requested_by"), self.manager)
        self.assertEqual(sorted(children.mapped("payment_id.amount")), [8.22, 12.31, 26.47])
        self.assertEqual(sum(children.mapped("payment_id.amount")), 47)
        self.assertEqual(set(children.mapped("payment_id.payment_type")), {"outbound"})
        self.assertEqual(set(children.mapped("payment_id.move_id.state")), {"posted"})
        self.assertEqual(children.mapped("payment_id.source_payment_id"), tx.payment_id)
        self.assertEqual(order.invoice_ids, invoice)
        self.assertEqual(invoice.amount_residual, 0)
        self.assertEqual(order.picking_ids, pickings)
        self.assertTrue(all(picking.state != "done" for picking in pickings))

    @patch(square_boundary)
    def test_refund_rejects_invalid_amounts_before_native_currency_rounding(self, external):
        _, tx, _ = self._capture(external)
        self.authenticate(self.manager.login, self.manager.login)
        wizard_ids = self._open_refund(tx.payment_id, 9)
        for amount in (0, -1, 47.01, 9.001, "NaN", "Infinity"):
            with self.subTest(amount=amount):
                with self.assertRaises(JsonRpcException):
                    self._open_refund(tx.payment_id, amount)
                with self.assertRaises(JsonRpcException):
                    self._call("payment.refund.wizard", "write", wizard_ids, vals={"amount_to_refund": amount})
                with self.assertRaises(JsonRpcException):
                    self._call(
                        "payment.transaction", "action_refund", tx.ids, button=True, amount_to_refund=amount,
                    )
        self.env.invalidate_all()
        self.assertFalse(tx.child_transaction_ids)
        external.assert_called_once()

    @patch(square_boundary)
    def test_partial_timeout_leaves_remaining_amount_but_replaying_wizard_cannot_refund_again(self, external):
        _, tx, original = self._capture(external)
        self.authenticate(self.manager.login, self.manager.login)
        stale_wizard_ids = self._open_refund(tx.payment_id, 40)
        wizard_ids = self._open_refund(tx.payment_id, 20)
        self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
        self.env.invalidate_all()
        child = tx.child_transaction_ids

        external.reset_mock()
        external.side_effect = [
            self._square_response({"payment": original}),
            requests.Timeout,
            self._square_response({"payment": original}),
            self._square_response({"refunds": []}),
        ]
        self._dispatch()
        self.assertEqual(child.state, "pending")
        self.assertEqual(tx.payment_id.amount_available_for_refund, 27)
        self.assertEqual(external.call_args_list[1].kwargs["json"]["idempotency_key"],
                         child.square_online_operation_key)
        with self.assertRaises(JsonRpcException):
            self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
        with self.assertRaises(JsonRpcException):
            self._call("payment.refund.wizard", "action_refund", stale_wizard_ids, button=True)
        self.assertEqual(len(tx.child_transaction_ids), 1)
        self.assertEqual(external.call_count, 4)

        new_wizard_ids = self._open_refund(tx.payment_id)
        wizard, = self._call("payment.refund.wizard", "read", new_wizard_ids, fields=[
            "amount_to_refund", "refunded_amount", "has_pending_refund",
        ])
        self.assertEqual(wizard["amount_to_refund"], 27)
        self.assertEqual(wizard["refunded_amount"], 0)
        self.assertTrue(wizard["has_pending_refund"])
        self._call("payment.refund.wizard", "action_refund", new_wizard_ids, button=True)
        self.env.invalidate_all()
        self.assertEqual(sorted(tx.child_transaction_ids.mapped("amount")), [-27, -20])
        self.assertEqual(tx.payment_id.amount_available_for_refund, 0)

        external.reset_mock()
        external.side_effect = [
            self._square_response({"payment": {**original, "refund_ids": ["PARTIAL-LOST"]}}),
            self._square_response({"refund": {
                "id": "PARTIAL-LOST", "payment_id": original["id"], "location_id": original["location_id"],
                "reason": child.square_online_refund_reason, "status": "FAILED",
                "amount_money": {"amount": 2000, "currency": "USD"},
            }}),
        ]
        self._call("payment.transaction", "action_square_online_refresh_status", child.ids, button=True)
        self.env.invalidate_all()
        self.assertEqual(child.state, "error")
        self.assertFalse(child.payment_id)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 20)
        self.assertEqual(self._call("payment.refund.wizard", "read", self._open_refund(tx.payment_id), fields=[
            "amount_to_refund",
        ])[0]["amount_to_refund"], 20)
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "GET"])

    @patch(square_boundary)
    def test_dispatch_counts_disjoint_square_activity_and_pending_odoo_refunds_once(self, external):
        _, tx, original = self._capture(external)
        self.authenticate(self.manager.login, self.manager.login)
        self._call(
            "payment.refund.wizard", "action_refund", self._open_refund(tx.payment_id, 20), button=True,
        )
        self.env.invalidate_all()
        child = tx.child_transaction_ids
        base_refund = {"payment_id": original["id"], "location_id": original["location_id"]}
        pending = {
            **base_refund, "id": "PENDING-ODOO", "reason": child.square_online_refund_reason,
            "status": "PENDING", "amount_money": {"amount": 2000, "currency": "USD"},
        }
        external.side_effect = [
            self._square_response({"payment": original}), self._square_response({"refund": pending}),
        ]
        self._dispatch()
        self.assertEqual(child.state, "pending")

        remote = {
            pending["id"]: pending,
            "EXTERNAL-COMPLETED": {
                **base_refund, "id": "EXTERNAL-COMPLETED", "status": "COMPLETED",
                "amount_money": {"amount": 1400, "currency": "USD"},
            },
            "EXTERNAL-PENDING": {
                **base_refund, "id": "EXTERNAL-PENDING", "status": "PENDING",
                "amount_money": {"amount": 400, "currency": "USD"},
            },
        }
        # Square's Payment.refunded_money includes pending as well as completed refunds.
        square_payment = {
            **original, "refunded_money": {"amount": 3800, "currency": "USD"}, "refund_ids": list(remote),
        }

        def square(method, url, *, headers, json, **kwargs):
            if method == "GET" and url.endswith("/v2/payments/" + original["id"]):
                return self._square_response({"payment": square_payment})
            if method == "GET":
                return self._square_response({"refund": remote[url.rsplit("/", 1)[-1]]})
            self.assertEqual(method, "POST")
            self.assertEqual(json["amount_money"], {"amount": 900, "currency": "USD"})
            return self._square_response({"refund": {
                **base_refund, "id": "AVAILABLE-BALANCE", "status": "COMPLETED",
                "reason": json["reason"], "amount_money": json["amount_money"],
            }})

        external.reset_mock()
        external.side_effect = square
        self._call(
            "payment.refund.wizard", "action_refund", self._open_refund(tx.payment_id, 10), button=True,
        )
        self._dispatch()
        refused = tx.child_transaction_ids.filtered(lambda tx: tx.state == "error")
        self.assertEqual(refused.state, "error")
        self.assertFalse(refused.square_online_dispatch_at)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 9)
        self.assertCountEqual(
            (tx.child_transaction_ids - child - refused).mapped("provider_reference"),
            ["EXTERNAL-COMPLETED", "EXTERNAL-PENDING"],
        )
        self.assertEqual({call.args[0] for call in external.call_args_list}, {"GET"})

        self._call(
            "payment.refund.wizard", "action_refund", self._open_refund(tx.payment_id, 9), button=True,
        )
        self._dispatch()
        self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
        self.env.invalidate_all()
        completed = tx.child_transaction_ids.filtered(lambda tx: tx.provider_reference == "AVAILABLE-BALANCE")
        self.assertEqual(completed.provider_reference, "AVAILABLE-BALANCE")
        self.assertEqual(completed.payment_id.amount, 9)
        self.assertEqual(completed.payment_id.move_id.state, "posted")
        self.assertEqual(sum(call.args[0] == "POST" for call in external.call_args_list), 1)

    @patch(square_boundary)
    def test_square_amount_rejection_rechecks_original_payment_and_releases_attempt(self, external):
        _, tx, original = self._capture(external)
        self.authenticate(self.manager.login, self.manager.login)
        self._call(
            "payment.refund.wizard", "action_refund", self._open_refund(tx.payment_id, 40), button=True,
        )
        self.env.invalidate_all()
        refund = tx.child_transaction_ids
        external.reset_mock()
        external.side_effect = [
            self._square_response({"payment": original}),
            self._square_response({"errors": [{"code": "REFUND_AMOUNT_INVALID"}]}, status=400),
            self._square_response({"payment": {
                **original, "refunded_money": {"amount": 3200, "currency": "USD"},
            }}),
            self._square_response({"refunds": []}),
        ]
        self._dispatch()
        self.assertEqual(refund.state, "error")
        self.assertFalse(refund.payment_id)
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "POST", "GET", "GET"])
        self.assertEqual(tx.payment_id.amount_available_for_refund, 15)

    @patch(square_boundary)
    def test_manager_full_refund_posts_native_outbound_payment(self, external):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order, tx, original = self._capture(external, self._make_refundable_goods_order())
        invoice = order.invoice_ids
        pickings = order.picking_ids
        self.assertTrue(invoice)
        self.assertTrue(pickings)
        self.authenticate(self.manager.login, self.manager.login)
        wizard_ids = self._open_refund(tx.payment_id)
        self.assertEqual(self._call("payment.refund.wizard", "read", wizard_ids, fields=[
            "amount_to_refund", "support_refund",
        ]), [{"id": wizard_ids[0], "amount_to_refund": 47, "support_refund": "partial"}])
        self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
        self.env.invalidate_all()
        child = tx.child_transaction_ids
        self.assertEqual(child.state, "pending")
        self.assertEqual(child.amount, -47)
        self.assertEqual(child.square_online_refund_requested_by, self.manager)
        self.assertFalse(child.payment_id)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 0)
        external.assert_called_once()  # Only the original charge.

        def square(method, url, *, headers, json, **kwargs):
            self.assertEqual(headers["Authorization"], "Bearer checkout-access-test")
            if method == "GET":
                self.assertTrue(url.endswith("/v2/payments/SQUARE-PAYMENT-TEST"), url)
                return self._square_response({"payment": original})
            self.assertEqual(method, "POST")
            self.assertEqual(url, "https://connect.squareupsandbox.com/v2/refunds")
            self.assertEqual(json, {
                "payment_id": original["id"],
                "idempotency_key": child.square_online_operation_key,
                "reason": child.square_online_refund_reason,
                "amount_money": {"amount": 4700, "currency": "USD"},
            })
            with self.env.registry.cursor() as cr:
                cr.execute(
                    "SELECT state, square_online_dispatch_at FROM payment_transaction WHERE id = %s",
                    [child.id],
                )
                state, dispatch_at = cr.fetchone()
                self.assertEqual(state, "pending")
                self.assertTrue(dispatch_at)
            return self._square_response({"refund": {
                "id": "SQUARE-REFUND-TEST",
                "payment_id": json["payment_id"],
                "location_id": "SQUARE-NY",
                "reason": json["reason"],
                "status": "COMPLETED",
                "amount_money": json["amount_money"],
            }})

        # Changes to provider configuration do not reroute an existing payment.
        self.provider.state = "disabled"
        new_journal = self.journal.copy({"name": "Replacement Square journal", "code": "RSJ"})
        self.provider.journal_id = new_journal
        external.reset_mock()
        external.side_effect = square
        self._dispatch()
        self._dispatch()
        self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "POST"])
        self.assertEqual(child.state, "done")
        self.assertEqual(child.provider_reference, "SQUARE-REFUND-TEST")
        self.assertFalse(child.invoice_ids)
        refund = child.payment_id
        self.assertEqual(refund.payment_type, "outbound")
        self.assertEqual(refund.amount, 47)
        self.assertEqual(refund.source_payment_id, tx.payment_id)
        self.assertEqual(refund.currency_id, self.currency)
        self.assertEqual(refund.journal_id, self.journal)
        self.assertEqual(refund.move_id.state, "posted")
        self.assertEqual(sum(refund.move_id.line_ids.mapped("debit")), 47)
        self.assertEqual(sum(refund.move_id.line_ids.mapped("credit")), 47)
        self.assertEqual(order.invoice_ids, invoice)
        self.assertEqual(invoice.amount_residual, 0)
        self.assertEqual(order.picking_ids, pickings)
        self.assertTrue(all(picking.state != "done" for picking in pickings))

    @patch(square_boundary)
    def test_timeout_keeps_balance_reserved_and_recovers_exact_refund_without_another_post(self, external):
        order, tx, original = self._capture(external)
        self.authenticate(self.manager.login, self.manager.login)
        wizard_ids = self._open_refund(tx.payment_id)
        self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
        self.env.invalidate_all()
        child = tx.child_transaction_ids
        refund = {
            "id": "REFUND-LOST-RESPONSE",
            "payment_id": original["id"],
            "location_id": original["location_id"],
            "amount_money": original["amount_money"],
            "status": "COMPLETED",
            "reason": child.square_online_refund_reason,
        }

        external.reset_mock()
        external.side_effect = [
            self._square_response({"payment": original}),
            requests.Timeout,
            self._square_response({"payment": original}),
            self._square_response({"refunds": [{**refund, "id": "UNRELATED", "reason": "Same amount"}]}),
        ]
        self._dispatch()
        self.assertEqual(child.state, "pending")
        self.assertFalse(child.provider_reference)
        self.assertFalse(child.payment_id)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 0)
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "POST", "GET", "GET"])

        self._dispatch()
        with self.assertRaises(JsonRpcException):
            self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
        with self.assertRaises(JsonRpcException):
            self._call("payment.transaction", "action_refund", tx.ids, button=True)
        self.assertEqual(len(tx.child_transaction_ids), 1)
        self.assertEqual(external.call_count, 4)

        wrong_location = {**refund, "location_id": "OTHER-LOCATION"}
        self.assertEqual(self._refund_notification(wrong_location).status_code, 200)
        self.env.invalidate_all()
        self.assertFalse(child.provider_reference)
        self.assertEqual(child.state, "pending")

        external.side_effect = [
            self._square_response({"payment": {**original, "refund_ids": [refund["id"]]}}),
            self._square_response({"refund": refund}),
        ]
        self.assertEqual(self._call(
            "payment.transaction", "action_square_online_refresh_status", child.ids, button=True,
        )["tag"], "reload")
        self.assertEqual(self._refund_notification({**refund, "status": "PENDING"}).status_code, 200)
        self.assertEqual(self._refund_notification(refund).status_code, 200)
        self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(child.state, "done")
        self.assertEqual(child.provider_reference, refund["id"])
        self.assertEqual(child.payment_id.amount, 47)
        self.assertEqual(child.payment_id.move_id.state, "posted")
        self.assertEqual(order.state, "sale")
        self.assertEqual([call.args[0] for call in external.call_args_list], [
            "GET", "POST", "GET", "GET", "GET", "GET",
        ])
        self.assertTrue(external.call_args.args[1].endswith("/v2/refunds/REFUND-LOST-RESPONSE"))

    @patch(square_boundary)
    def test_definitive_failure_releases_reservation_without_posting_a_payment(self, external):
        order, tx, original = self._capture(external)
        administrator = new_test_user(
            self.env, login="square-refund-administrator",
            groups="base.group_system,account.group_account_invoice,sales_team.group_sale_manager",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        self.authenticate(administrator.login, administrator.login)
        wizard_ids = self._open_refund(tx.payment_id)
        self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
        self.env.invalidate_all()
        child = tx.child_transaction_ids
        with self.assertRaises(JsonRpcException):
            self._call("payment.transaction", "unlink", child.ids)
        external.reset_mock()
        external.side_effect = [
            self._square_response({"payment": original}),
            self._square_response({"refund": {
                "id": "REFUND-REJECTED",
                "payment_id": original["id"],
                "location_id": original["location_id"],
                "amount_money": original["amount_money"],
                "status": "REJECTED",
                "reason": child.square_online_refund_reason,
            }}),
        ]
        self._dispatch()
        self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(child.state, "error")
        self.assertEqual(child.provider_reference, "REFUND-REJECTED")
        self.assertEqual(child.square_online_refund_requested_by, administrator)
        self.assertFalse(child.payment_id)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 47)
        self._call("sale.order", "action_cancel", order.ids, button=True)
        self.env.invalidate_all()
        self.assertEqual(order.state, "cancel")
        self.assertEqual(tx.child_transaction_ids, child)
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "POST"])

    @patch(square_boundary)
    def test_refund_routes_enforce_role_and_selected_company(self, external):
        _, tx, _original = self._capture(external)
        other_company = self.env["res.company"].create({"name": "Other refund company"})
        manager = new_test_user(
            self.env, login="square-refund-other-company",
            groups="account.group_account_manager,base.group_multi_company",
            company_id=other_company.id, company_ids=[Command.set((self.company + other_company).ids)],
        )
        staff = new_test_user(
            self.env, login="square-refund-billing-user", groups="account.group_account_invoice",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        for user, company in (
            (self.portal, self.company), (staff, self.company), (manager, other_company),
        ):
            with self.subTest(user=user.login):
                self.authenticate(user.login, user.login)
                context = {"allowed_company_ids": company.ids}
                with self.assertRaises(JsonRpcException):
                    self._call(
                        "account.payment", "action_refund_wizard", tx.payment_id.ids,
                        button=True, context=context,
                    )
                with self.assertRaises(JsonRpcException):
                    self._call(
                        "payment.transaction", "action_refund", tx.ids,
                        button=True, context=context, amount_to_refund=47,
                    )
        self.env.invalidate_all()
        self.assertFalse(tx.child_transaction_ids)
        external.assert_called_once()

    def _capture_in_market(self, external, country, code, unit_price, captured_minor):
        currency = self.env.ref(f"base.{code}")
        if currency == self.currency:
            return self._capture(external)
        currency.active = True
        self.env["res.currency.rate"].create({
            "currency_id": currency.id, "company_id": self.company.id,
            "name": fields.Date.today(), "rate": 2,
        })
        merchant = f"MERCHANT-{country}"
        location = f"LOCATION-{country}"
        token = f"access-{country}"
        connection = self.env["square.online.connection"].create({
            "name": f"{country} Sandbox",
            "company_id": self.company.id,
            "application_id": f"sandbox-test-{country}",
            "application_secret": "test-secret",
        })
        external.side_effect = [
            self._square_response({
                "merchant_id": merchant,
                "access_token": token, "refresh_token": "test-refresh", "token_type": "bearer",
                "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }),
            self._square_response({"locations": [{
                "id": location, "merchant_id": merchant, "name": country,
                "country": country, "currency": code, "status": "ACTIVE",
                "capabilities": ["CREDIT_CARD_PROCESSING"],
            }]}),
        ]
        connection._square_online_authorize("test-oauth-code")
        connection.location_id = connection.available_location_ids
        self.provider.square_online_sandbox_connection_id = connection
        pricelist = self.env["product.pricelist"].create({
            "name": f"{code} refunds", "currency_id": currency.id,
            "company_id": self.company.id, "website_id": self.website.id,
            "item_ids": [Command.create({
                "applied_on": "1_product", "product_tmpl_id": self.product.product_tmpl_id.id,
                "compute_price": "fixed", "fixed_price": unit_price,
            })],
        })
        order = self._make_order()
        order.pricelist_id = pricelist
        order.order_line.price_unit = unit_price
        draft = self._prepare(order, self._open_payment_page(order))
        original = {
            "id": f"PAYMENT-{country}", "source_type": "CARD", "status": "COMPLETED",
            "location_id": location, "amount_money": {"amount": captured_minor, "currency": code},
        }

        def capture(method, url, *, headers, json, **kwargs):
            self.assertEqual(method, "POST")
            self.assertTrue(url.endswith("/v2/payments"))
            self.assertEqual(headers["Authorization"], f"Bearer {token}")
            self.assertEqual(json["amount_money"], original["amount_money"])
            original["reference_id"] = json["reference_id"]
            return self._square_response({"payment": original})

        external.side_effect = capture
        self.assertEqual(self._submit(draft)["state"], "done")
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()
        return order, order.transaction_ids, original

    @patch(square_boundary)
    def test_partial_refunds_preserve_minor_units_and_post_in_all_supported_currencies(self, external):
        markets = (("US", "USD"), ("AU", "AUD"), ("CA", "CAD"), ("FR", "EUR"), ("GB", "GBP"), ("JP", "JPY"))
        for country, code in markets:
            with self.subTest(currency=code):
                if code == "JPY":
                    unit_price, captured_minor = 187, 374
                    amounts = ((124, 124, 250), (76, 76, 174), (None, 174, 0))
                else:
                    unit_price, captured_minor = 23.50, 4700
                    amounts = ((12.31, 1231, 34.69), (8.22, 822, 26.47), (None, 2647, 0))
                order, tx, original = self._capture_in_market(external, country, code, unit_price, captured_minor)
                self.authenticate(self.manager.login, self.manager.login)
                if code == "JPY":
                    with self.assertRaises(JsonRpcException):
                        self._open_refund(tx.payment_id, 124.01)
                refunded = {}

                def square(method, url, *, headers, json, **kwargs):
                    if method == "GET" and url.endswith("/v2/payments/" + original["id"]):
                        return self._square_response({"payment": {
                            **original, "refund_ids": list(refunded),
                            "refunded_money": {
                                "amount": sum(refund["amount_money"]["amount"] for refund in refunded.values()),
                                "currency": code,
                            },
                        }})
                    if method == "GET":
                        return self._square_response({"refund": refunded[url.rsplit("/", 1)[-1]]})
                    self.assertEqual(method, "POST")
                    self.assertEqual(json["payment_id"], original["id"])
                    self.assertEqual(json["amount_money"], {"amount": expected_minor, "currency": code})
                    refund_id = f"REFUND-{country}-{len(refunded)}"
                    refunded[refund_id] = {
                        "id": refund_id, "payment_id": original["id"], "location_id": original["location_id"],
                        "status": "COMPLETED", "reason": json["reason"], "amount_money": json["amount_money"],
                    }
                    return self._square_response({"refund": refunded[refund_id]})

                external.side_effect = square
                for amount, expected_minor, remaining in amounts:
                    wizard_ids = self._open_refund(tx.payment_id, amount)
                    self._call("payment.refund.wizard", "action_refund", wizard_ids, button=True)
                    self._dispatch()
                    self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
                    self.env.invalidate_all()
                    self.assertEqual(tx.payment_id.amount_available_for_refund, remaining)
                refunds = tx.child_transaction_ids
                payments = refunds.payment_id
                self.assertEqual(set(refunds.mapped("provider_reference")), set(refunded))
                self.assertEqual(sum(refunds.mapped("square_online_amount_minor")), captured_minor)
                self.assertEqual(sum(payments.mapped("amount")), 374 if code == "JPY" else 47)
                self.assertEqual(set(payments.mapped("payment_type")), {"outbound"})
                self.assertEqual(set(payments.mapped("move_id.state")), {"posted"})
                self.assertEqual(payments.currency_id.name, code)
                self.assertEqual(payments.journal_id, self.journal)
                self.assertEqual(payments.source_payment_id, tx.payment_id)
                self.assertFalse(refunds.invoice_ids)
                self.assertFalse(order.picking_ids)
                if code == "JPY":
                    receivable = payments.move_id.line_ids.filtered(lambda line: line.account_type == "asset_receivable")
                    outstanding = payments.move_id.line_ids - receivable
                    self.assertEqual(sum(receivable.mapped("amount_currency")), 374)
                    self.assertEqual(sum(receivable.mapped("balance")), 187)
                    self.assertEqual(sum(outstanding.mapped("amount_currency")), -374)
                    self.assertEqual(sum(outstanding.mapped("balance")), -187)

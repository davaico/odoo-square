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

    def _call(self, model, method, ids, *, button=False, **kwargs):
        endpoint = "call_button" if button else "call_kw"
        return self.make_jsonrpc_request(f"/web/dataset/{endpoint}/{model}/{method}", {
            "model": model, "method": method, "args": [ids], "kwargs": kwargs,
        })

    def _capture(self, external, order=None):
        order = order or self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))
        external.side_effect = self._completed_payment
        self.assertEqual(self._submit(draft)["state"], "done")
        charge = self._completed_payment(*external.call_args.args, **external.call_args.kwargs)
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()
        return order, order.transaction_ids, charge.json()["payment"]

    def _open_refund(self, payment, amount=None):
        action = self._call("account.payment", "action_refund_wizard", payment.ids, button=True)
        self.assertEqual(action["res_model"], "payment.refund.wizard")
        values = {"payment_id": payment.id}
        if amount is not None:
            values["amount_to_refund"] = amount
        wizard_id = self._call("payment.refund.wizard", "create", values)
        return [wizard_id]

    def _dispatch(self):
        self.env.ref("payment_square_online.cron_square_online_dispatch_refunds").method_direct_trigger()
        self.env.invalidate_all()

    def _refund_notification(self, refund, *, signing_key="refund-webhook-test-key"):
        path = f"/payment/square_online/webhook/{self.connection.id}"
        url = "https://shop.example.test" + path
        self.connection.write({
            "webhook_notification_url": url,
            "webhook_signature_key": signing_key,
        })
        raw = json.dumps({
            "type": "refund.updated",
            "merchant_id": "MERCHANT_TEST",
            "data": {"type": "refund", "id": refund["id"], "object": {"refund": refund}},
        }).encode()
        signature = base64.b64encode(hmac.digest(signing_key.encode(), url.encode() + raw, "sha256"))
        return self.url_open(path, data=raw, headers={
            "Content-Type": "application/json",
            "X-Square-Hmacsha256-Signature": signature.decode(),
            "Square-Environment": "Sandbox",
        })

    @patch(square_boundary)
    def test_manager_full_refund_posts_native_outbound_payment(self, external):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
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
        order, tx, original = self._capture(external, order)
        invoice = order.invoice_ids
        pickings = order.picking_ids
        self.assertTrue(invoice)
        self.assertTrue(pickings)
        self.authenticate(self.manager.login, self.manager.login)
        wizard_ids = self._open_refund(tx.payment_id)
        self.assertEqual(self._call("payment.refund.wizard", "read", wizard_ids, fields=[
            "amount_to_refund", "support_refund",
        ]), [{"id": wizard_ids[0], "amount_to_refund": 47, "support_refund": "full_only"}])
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

    @patch(square_boundary)
    def test_jpy_full_refund_posts_exact_foreign_and_company_currency(self, external):
        jpy = self.env.ref("base.JPY")
        jpy.active = True
        self.env["res.currency.rate"].create({
            "currency_id": jpy.id, "company_id": self.company.id,
            "name": fields.Date.today(), "rate": 2,
        })
        connection = self.env["square.online.connection"].create({
            "name": "Japan Sandbox",
            "company_id": self.company.id,
            "application_id": "sandbox-japan",
            "application_secret": "test-japan-secret",
        })
        external.side_effect = [
            self._square_response({
                "merchant_id": "MERCHANT-JP",
                "access_token": "japan-access", "refresh_token": "japan-refresh", "token_type": "bearer",
                "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }),
            self._square_response({"locations": [{
                "id": "LOCATION-JP", "merchant_id": "MERCHANT-JP", "name": "Japan",
                "country": "JP", "currency": "JPY", "status": "ACTIVE",
                "capabilities": ["CREDIT_CARD_PROCESSING"],
            }]}),
        ]
        connection._square_online_authorize("code-japan")
        connection.location_id = connection.available_location_ids
        self.provider.square_online_sandbox_connection_id = connection
        pricelist = self.env["product.pricelist"].create({
            "name": "Japanese yen", "currency_id": jpy.id,
            "company_id": self.company.id, "website_id": self.website.id,
            "item_ids": [Command.create({
                "applied_on": "1_product", "product_tmpl_id": self.product.product_tmpl_id.id,
                "compute_price": "fixed", "fixed_price": 187,
            })],
        })
        order = self._make_order()
        order.pricelist_id = pricelist
        order.order_line.price_unit = 187
        draft = self._prepare(order, self._open_payment_page(order))
        original = {
            "id": "PAYMENT-JP", "source_type": "CARD", "status": "COMPLETED",
            "reference_id": draft["reference"], "location_id": "LOCATION-JP",
            "amount_money": {"amount": 374, "currency": "JPY"},
        }

        def capture(method, url, *, headers, json, **kwargs):
            self.assertEqual(method, "POST")
            self.assertTrue(url.endswith("/v2/payments"))
            self.assertEqual(headers["Authorization"], "Bearer japan-access")
            self.assertEqual(json["amount_money"], {"amount": 374, "currency": "JPY"})
            return self._square_response({"payment": {**original, "reference_id": json["reference_id"]}})

        external.side_effect = capture
        self.assertEqual(self._submit(draft)["state"], "done")
        original["reference_id"] = external.call_args.kwargs["json"]["reference_id"]
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()
        tx = order.transaction_ids
        self.authenticate(self.manager.login, self.manager.login)
        self._call(
            "payment.refund.wizard", "action_refund", self._open_refund(tx.payment_id), button=True,
        )
        self.env.invalidate_all()
        child = tx.child_transaction_ids

        def refund(method, url, *, headers, json, **kwargs):
            self.assertEqual(headers["Authorization"], "Bearer japan-access")
            if method == "GET":
                self.assertTrue(url.endswith("/v2/payments/PAYMENT-JP"))
                return self._square_response({"payment": original})
            self.assertEqual(method, "POST")
            self.assertTrue(url.endswith("/v2/refunds"))
            self.assertEqual(json["amount_money"], {"amount": 374, "currency": "JPY"})
            return self._square_response({"refund": {
                **original, "id": "REFUND-JP", "payment_id": original["id"], "reason": json["reason"],
            }})

        external.side_effect = refund
        self._dispatch()
        self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
        self.env.invalidate_all()
        payment = child.payment_id
        self.assertEqual(payment.payment_type, "outbound")
        self.assertEqual(payment.amount, 374)
        self.assertEqual(payment.currency_id, jpy)
        self.assertEqual(payment.journal_id, self.journal)
        self.assertEqual(payment.move_id.state, "posted")
        receivable = payment.move_id.line_ids.filtered(lambda line: line.account_type == "asset_receivable")
        outstanding = payment.move_id.line_ids - receivable
        self.assertEqual((receivable.amount_currency, receivable.balance), (374, 187))
        self.assertEqual((outstanding.amount_currency, outstanding.balance), (-374, -187))

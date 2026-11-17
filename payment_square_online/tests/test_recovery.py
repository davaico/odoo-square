# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import base64
import hmac
import json
from unittest.mock import patch

import requests

from odoo import Command, fields
from odoo.tests import tagged
from odoo.tests.common import JsonRpcException, new_test_user

from .test_checkout import SquareOnlineCheckoutCase


@tagged("post_install", "-at_install")
class TestSquareOnlineRecovery(SquareOnlineCheckoutCase):
    square_boundary = SquareOnlineCheckoutCase.square_boundary
    webhook_key = "square-test-notification-key"

    def _configure_webhook(self):
        self.webhook_path = f"/payment/square_online/webhook/{self.connection.id}"
        self.webhook_url = "https://shop.example.test" + self.webhook_path
        self.connection.write({
            "webhook_notification_url": self.webhook_url,
            "webhook_signature_key": self.webhook_key,
        })

    def _accepted_charge_timeout(self, external):
        order = self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))
        external.side_effect = requests.Timeout
        self.assertEqual(self._submit(draft)["state"], "pending")
        self.env.invalidate_all()
        payment_payload = external.call_args.kwargs["json"]
        payment = {
            "id": "SQUARE-PAYMENT-TEST",
            "source_type": "CARD",
            "reference_id": payment_payload["reference_id"],
            "status": "COMPLETED",
            "location_id": payment_payload["location_id"],
            "amount_money": payment_payload["amount_money"],
            "order_id": "SQUARE-ORDER-TEST",
        }
        self.assertTrue(order.transaction_ids.square_online_dispatch_at)
        self.assertFalse(order.transaction_ids.provider_reference)
        self.assertEqual(order.state, "draft")
        return order, payment

    def _signed_webhook(
        self, payment, *, merchant="MERCHANT_TEST", environment="Sandbox",
        signature_url=None, location=None, event_type="payment.updated", transform=None,
    ):
        body = json.dumps({
            "type": event_type,
            "merchant_id": merchant,
            "location_id": location,
            "event_id": "square-test-event",
            "data": {"type": "payment", "id": payment["id"], "object": {"payment": payment}},
        }, indent=2).encode() + b"\n"
        signed = (signature_url or self.webhook_url).encode() + body
        signature = base64.b64encode(hmac.digest(self.webhook_key.encode(), signed, "sha256")).decode()
        return self.url_open(
            self.webhook_path, data=transform(body) if transform else body,
            headers={
                "Content-Type": "application/json",
                "X-Square-Hmacsha256-Signature": signature,
                "Square-Environment": environment,
            },
        )

    @patch(square_boundary)
    def test_signed_notification_recovers_charge_after_browser_closes(self, external):
        self._configure_webhook()
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order, payment = self._accepted_charge_timeout(external)
        self.authenticate(None, None)

        self.assertEqual(self._signed_webhook(payment).status_code, 200)
        native_post_process = self.env.ref("payment.cron_post_process_payment_tx")
        native_post_process.method_direct_trigger()
        self.env.invalidate_all()
        tx = order.transaction_ids
        self.assertEqual(tx.state, "done")
        self.assertEqual(tx.provider_reference, payment["id"])
        self.assertEqual(tx.square_online_connection_id, self.connection)
        self.assertEqual(tx.payment_id.amount, 47)
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.invoice_ids.state, "posted")
        self.assertEqual(order.invoice_ids.amount_residual, 0)
        external.assert_called_once()

    @patch(square_boundary)
    def test_forged_and_mismatched_notifications_leave_the_order_unpaid(self, external):
        self._configure_webhook()
        order, payment = self._accepted_charge_timeout(external)
        self.assertEqual(self._signed_webhook(payment, transform=lambda raw: raw.rstrip()).status_code, 403)
        self.assertEqual(self._signed_webhook(payment, signature_url=self.base_url() + self.webhook_path).status_code, 403)
        self.assertEqual(self._signed_webhook(payment, merchant="OTHER-MERCHANT").status_code, 200)
        self.assertEqual(self._signed_webhook(payment, environment="Production").status_code, 200)
        self.assertEqual(self._signed_webhook(payment, location="OTHER-LOCATION").status_code, 200)
        for wrong in (
            {"reference_id": "a" * 32},
            {"location_id": "ANOTHER-LOCATION"},
            {"amount_money": {"amount": 4700, "currency": "CAD"}},
            {"amount_money": {"amount": 4701, "currency": "USD"}},
            {"application_details": {"application_id": "sandbox-other-app"}},
            {"source_type": "CASH"},
        ):
            with self.subTest(wrong=wrong):
                self.assertEqual(self._signed_webhook({**payment, **wrong}).status_code, 200)
        self.env.invalidate_all()
        tx = order.transaction_ids
        self.assertEqual(tx.state, "pending")
        self.assertFalse(tx.provider_reference)
        self.assertFalse(tx.payment_id)
        self.assertFalse(order.invoice_ids)
        self.assertEqual(order.state, "draft")
        external.assert_called_once()

    @patch(square_boundary)
    def test_late_capture_keeps_the_cancelled_order_and_accounts_only_once(self, external):
        self._configure_webhook()
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order, payment = self._accepted_charge_timeout(external)
        order.action_cancel()
        self.assertEqual(self._signed_webhook(payment).status_code, 200)
        self.env.invalidate_all()
        self.assertTrue(order.transaction_ids.square_online_has_order_mismatch)
        self.assertEqual(self._signed_webhook(payment).status_code, 200)
        self.assertEqual(self._signed_webhook({**payment, "status": "APPROVED"}).status_code, 200)
        self.assertEqual(self._signed_webhook({**payment, "status": "CANCELED"}).status_code, 200)
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()

        tx = order.transaction_ids
        self.assertEqual(order.state, "cancel")
        self.assertTrue(tx.square_online_has_order_mismatch)
        self.assertEqual(len(order.message_ids.filtered(
            lambda message: "Review the payment" in message.body
        )), 1)
        self.assertFalse(order.invoice_ids)
        self.assertEqual(tx.state, "done")
        self.assertEqual(tx.payment_id.amount, 47)
        self.assertEqual(tx.payment_id.move_id.state, "posted")
        self.assertFalse(tx.child_transaction_ids)
        self.authenticate(self.portal.login, self.portal.login)
        portal_order = self.url_open(f"/my/orders/{order.id}")
        self.assertIn("Square collected your payment. Please contact us about this order.", portal_order.text)
        external.assert_called_once()

    def _call_refresh(self, tx):
        method = "action_square_online_refresh_status"
        return self.make_jsonrpc_request(f"/web/dataset/call_button/payment.transaction/{method}", {
            "model": "payment.transaction", "method": method, "args": [tx.ids], "kwargs": {},
        })

    @patch(square_boundary)
    def test_account_manager_refreshes_the_original_location_and_follows_list_pages(self, external):
        order, payment = self._accepted_charge_timeout(external)
        tx = order.transaction_ids
        self.provider.state = "disabled"
        manager = new_test_user(
            self.env, login="square-account-manager", groups="account.group_account_manager",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        self.authenticate(manager.login, manager.login)
        external.side_effect = None
        external.return_value = self._square_response({
            "errors": [{"code": "UNAUTHORIZED", "detail": "private authorization error"}],
        }, 401)
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.env.invalidate_all()
        self.assertEqual(tx.state, "pending")
        self.assertIn("Reauthorize the original connection.", tx.square_online_recovery_message)
        self.assertNotIn("private authorization error", tx.square_online_recovery_message)
        external.side_effect = [
            self._square_response({"payments": [
                {**payment, "reference_id": "a" * 32},
                {**payment, "location_id": "ANOTHER-LOCATION"},
            ], "cursor": "next-page"}),
            self._square_response({"payments": [payment]}),
        ]
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.env.invalidate_all()
        self.assertEqual(tx.state, "done")
        self.assertEqual(tx.provider_reference, payment["id"])
        self.assertEqual(tx.square_online_connection_id, self.connection)
        self.assertFalse(tx.square_online_recovery_message)
        self.assertTrue(tx.square_online_recovery_checked_at)
        self.assertEqual([call.args[0] for call in external.call_args_list], ["POST", "GET", "GET", "GET"])
        for call in external.call_args_list[1:]:
            self.assertEqual(call.args[1], "https://connect.squareupsandbox.com/v2/payments")
            self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer checkout-access-test")
            params = call.kwargs["params"]
            self.assertEqual(params["location_id"], "SQUARE-NY")
            expected_start = fields.Datetime.subtract(tx.square_online_dispatch_at, minutes=5)
            self.assertEqual(params["begin_time"], expected_start.isoformat(timespec="seconds") + "Z")
        self.assertEqual(external.call_args.kwargs["params"]["cursor"], "next-page")
        self.env.ref("payment.cron_post_process_payment_tx").method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(order.state, "sale")
        self.assertEqual(tx.payment_id.journal_id, self.journal)

    @patch(square_boundary)
    def test_scheduled_recovery_after_a_list_miss_and_with_a_known_payment_id(self, external):
        order, payment = self._accepted_charge_timeout(external)
        self.authenticate(None, None)
        tx = order.transaction_ids
        tx.square_online_dispatch_at = fields.Datetime.subtract(fields.Datetime.now(), days=10)
        cron = self.env.ref("payment_square_online.cron_square_online_recover_status")
        external.side_effect = None
        external.return_value = self._square_response({"payments": []})
        cron.method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(tx.state, "pending")
        self.assertFalse(tx.provider_reference)
        self.assertFalse(tx.payment_id)
        self.assertEqual(order.state, "draft")

        tx.square_online_recovery_checked_at = fields.Datetime.subtract(fields.Datetime.now(), minutes=6)
        external.return_value = self._square_response({"payments": [{**payment, "status": "APPROVED"}]})
        cron.method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(tx.provider_reference, payment["id"])
        self.assertEqual(tx.state, "pending")
        self.assertEqual(order.state, "draft")
        self.assertFalse(tx.payment_id)

        tx.square_online_recovery_checked_at = fields.Datetime.subtract(fields.Datetime.now(), minutes=6)
        external.return_value = self._square_response({"payment": payment})
        cron.method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(tx.state, "done")
        self.assertTrue(tx.is_post_processed)
        self.assertEqual(order.state, "sale")
        self.assertEqual(tx.payment_id.amount, 47)
        self.assertEqual(external.call_args.args, (
            "GET", "https://connect.squareupsandbox.com/v2/payments/" + payment["id"],
        ))
        self.assertEqual([call.args[0] for call in external.call_args_list], ["POST", "GET", "GET", "GET"])

    @patch(square_boundary)
    def test_refresh_and_history_require_accounting_role_and_the_current_company(self, external):
        order, _payment = self._accepted_charge_timeout(external)
        tx = order.transaction_ids
        other_company = self.env["res.company"].create({"name": "Other Square Company"})
        other_admin = new_test_user(
            self.env, login="other-square-admin", groups="base.group_system",
            company_id=other_company.id, company_ids=[Command.set(other_company.ids)],
        )
        billing_user = new_test_user(
            self.env, login="square-billing-user", groups="account.group_account_invoice",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        method = "read"
        for user in (self.portal, other_admin, billing_user):
            with self.subTest(user=user.login):
                self.authenticate(user.login, user.login)
                with self.assertRaises(JsonRpcException):
                    self._call_refresh(tx)
                if user != billing_user:
                    with self.assertRaises(JsonRpcException):
                        self.make_jsonrpc_request(
                            f"/web/dataset/call_kw/payment.transaction/{method}",
                            {"model": "payment.transaction", "method": method, "args": [tx.ids], "kwargs": {}},
                        )
        external.assert_called_once()

    @patch(square_boundary)
    def test_shop_status_is_bound_to_the_shopper_session_and_order_access(self, external):
        document, payment = self._accepted_charge_timeout(external)
        self.assertIn("Square is processing your payment.", self.url_open("/payment/status").text)
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "pending")
        outsider = new_test_user(self.env, login="square-other-shopper", groups="base.group_portal")
        self.authenticate(outsider.login, outsider.login)
        self.assertNotIn("Square is processing your payment.", self.url_open("/payment/status").text)
        self.assertNotIn("Square is processing your payment.", self.url_open(f"/my/orders/{document.id}").text)

        self._configure_webhook()
        self.assertEqual(self._signed_webhook({**payment, "status": "CANCELED"}).status_code, 200)
        self.authenticate(self.portal.login, self.portal.login)
        portal_order = self.url_open(f"/my/orders/{document.id}")
        self.assertEqual(portal_order.status_code, 200)
        self.assertIn("The card payment was cancelled.", portal_order.text)
        self.assertNotIn("checkout-access-test", portal_order.text)
        self.env.invalidate_all()
        self.assertEqual(document.state, "draft")
        self.assertFalse(document.transaction_ids.payment_id)
        external.assert_called_once()

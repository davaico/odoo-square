# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import base64
import hmac
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit

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

    def _expire_access_token(self, connection):
        self.env.flush_all()
        self.env.cr.execute(
            "UPDATE square_online_connection SET token_expires_at = %s WHERE id = %s",
            (fields.Datetime.subtract(fields.Datetime.now(), minutes=1), connection.id),
        )
        connection.invalidate_recordset(["token_expires_at"])

    @patch(square_boundary)
    def test_refresh_status_renews_expired_access_and_recovers_original_payment(self, external):
        order, payment = self._accepted_charge_timeout(external)
        tx = order.transaction_ids
        self._expire_access_token(self.connection)
        manager = new_test_user(
            self.env, login="square-renewal-manager", groups="account.group_account_manager",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        self.authenticate(manager.login, manager.login)
        external.side_effect = [
            self._square_response({
                "merchant_id": "MERCHANT_TEST",
                "access_token": "square-renewed-access-test",
                "refresh_token": "checkout-refresh-test",
                "token_type": "bearer",
                "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }),
            self._square_response({"payments": [payment]}),
        ]

        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.env.invalidate_all()
        self.assertEqual(tx.state, "done")
        self.assertEqual(tx.provider_reference, payment["id"])
        self.assertEqual(tx.square_online_connection_id, self.connection)
        self.assertFalse(tx.square_online_recovery_message)
        self.assertGreater(self.connection.token_expires_at, fields.Datetime.now())
        self.assertNotEqual(self.connection.access_token, "square-renewed-access-test")
        self.assertEqual([call.args for call in external.call_args_list], [
            ("POST", "https://connect.squareupsandbox.com/v2/payments"),
            ("POST", "https://connect.squareupsandbox.com/oauth2/token"),
            ("GET", "https://connect.squareupsandbox.com/v2/payments"),
        ])
        renewal, check = external.call_args_list[1:]
        self.assertEqual(renewal.kwargs["json"], {
            "grant_type": "refresh_token",
            "client_id": self.connection.application_id,
            "client_secret": "square-checkout-test-secret",
            "refresh_token": "checkout-refresh-test",
        })
        self.assertEqual(check.kwargs["headers"]["Authorization"], "Bearer square-renewed-access-test")
        self.assertEqual(check.kwargs["params"]["location_id"], "SQUARE-NY")

    @patch(square_boundary)
    def test_refresh_status_retries_only_its_read_when_square_reports_an_expired_token(self, external):
        order, payment = self._accepted_charge_timeout(external)
        manager = new_test_user(
            self.env, login="square-expiry-manager", groups="account.group_account_manager",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        self.authenticate(manager.login, manager.login)
        external.side_effect = [
            self._square_response({"errors": [{"code": "ACCESS_TOKEN_EXPIRED"}]}, 401),
            self._square_response({
                "merchant_id": "MERCHANT_TEST",
                "access_token": "square-renewed-after-rejection",
                "refresh_token": "checkout-refresh-test",
                "token_type": "bearer",
                "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }),
            self._square_response({"payments": [payment]}),
        ]

        tx = order.transaction_ids
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.env.invalidate_all()
        self.assertEqual(tx.state, "done")
        self.assertEqual(tx.provider_reference, payment["id"])
        self.assertEqual([call.args for call in external.call_args_list], [
            ("POST", "https://connect.squareupsandbox.com/v2/payments"),
            ("GET", "https://connect.squareupsandbox.com/v2/payments"),
            ("POST", "https://connect.squareupsandbox.com/oauth2/token"),
            ("GET", "https://connect.squareupsandbox.com/v2/payments"),
        ])
        initial_read, _, retry = external.call_args_list[1:]
        self.assertEqual(initial_read.kwargs["params"], retry.kwargs["params"])
        self.assertEqual(retry.kwargs["headers"]["Authorization"], "Bearer square-renewed-after-rejection")

    def _signed_webhook(
        self, payment, *, merchant="MERCHANT_TEST", environment="Sandbox",
        signature_url=None, location=None, event_type="payment.updated", transform=None,
    ):
        return self._send_webhook({
            "type": event_type,
            "merchant_id": merchant,
            "location_id": location,
            "event_id": "square-test-event",
            "data": {"type": "payment", "id": payment["id"], "object": {"payment": payment}},
        }, environment, signature_url, transform)

    def _send_webhook(self, event, environment="Sandbox", signature_url=None, transform=None):
        body = json.dumps(event, indent=2).encode() + b"\n"
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
    def test_expired_grant_rejected_before_dispatch_keeps_checkout_unpaid(self, external):
        order = self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))
        self._expire_access_token(self.connection)
        external.return_value = self._square_response({"error": "invalid_grant"}, 400)

        with self.assertRaises(JsonRpcException):
            self._submit(draft)
        self.env.invalidate_all()
        self.assertFalse(order.transaction_ids.square_online_dispatch_at)
        self.assertEqual(order.transaction_ids.state, "draft")
        self.assertEqual(order.state, "draft")
        external.assert_called_once()
        self.assertEqual(external.call_args.args, ("POST", "https://connect.squareupsandbox.com/oauth2/token"))

    @patch(square_boundary)
    def test_square_rejects_revoked_access_during_charge_without_a_second_post(self, external):
        order = self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))
        external.return_value = self._square_response({
            "errors": [{"code": "ACCESS_TOKEN_REVOKED", "detail": "private Square diagnostics"}],
        }, 401)
        result = self._submit(draft)
        self.assertEqual(result["state"], "error")
        self.assertIn("Square is temporarily unavailable", result["state_message"])
        self.assertNotIn("private Square diagnostics", result["state_message"])

        self.env.invalidate_all()
        self.assertEqual(self.connection.oauth_health, "reconnect")
        self.assertFalse(order.transaction_ids.provider_reference)
        self.assertFalse(order.transaction_ids.payment_id)
        self.assertEqual(order.state, "draft")
        self.assertEqual(self._submit(draft)["state"], "error")
        external.assert_called_once()
        self.assertEqual(external.call_args.args, ("POST", "https://connect.squareupsandbox.com/v2/payments"))

    @patch(square_boundary)
    def test_square_rejects_access_with_a_non_json_response(self, external):
        order = self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))
        response = requests.Response()
        response.status_code = 401
        response._content = b"<html>private Square diagnostics</html>"
        external.return_value = response

        result = self._submit(draft)
        self.assertEqual(result["state"], "error")
        self.assertIn("Square is temporarily unavailable", result["state_message"])
        self.assertNotIn("private Square diagnostics", result["state_message"])
        self.env.invalidate_all()
        self.assertEqual(self.connection.oauth_health, "reconnect")
        self.assertEqual(order.state, "draft")
        self.assertFalse(order.transaction_ids.payment_id)
        external.assert_called_once()
        self.assertEqual(external.call_args.args, ("POST", "https://connect.squareupsandbox.com/v2/payments"))

    def _authorize_connection(self, connection, external, *, merchant, access, location="SQUARE-NY"):
        method = "action_square_online_connect"
        action = self.make_jsonrpc_request(f"/web/dataset/call_button/square.online.connection/{method}", {
            "model": "square.online.connection", "method": method, "args": [connection.ids],
            "kwargs": {},
        })
        state = parse_qs(urlsplit(action["url"]).query)["state"][0]
        external.side_effect = [
            self._square_response({
                "merchant_id": merchant,
                "access_token": access,
                "refresh_token": access + "-refresh",
                "token_type": "bearer",
                "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }),
            self._square_response({"locations": [{
                "id": location,
                "name": "Reconnect Store",
                "merchant_id": merchant,
                "country": "US",
                "currency": "USD",
                "status": "ACTIVE",
                "capabilities": ["CREDIT_CARD_PROCESSING"],
            }]}),
        ]
        callback = "/payment/square_online/oauth/callback"
        params = urlencode({"state": state, "code": "square-reconnect-code"})
        self.assertEqual(self.url_open(f"{callback}?{params}", allow_redirects=False).status_code, 303)
        self.env.invalidate_all()

    def _revocation_event(self):
        return {
            "type": "oauth.authorization.revoked",
            "merchant_id": "MERCHANT_TEST",
            "event_id": "square-oauth-revocation",
            "data": {
                "type": "revocation",
                "id": "square-revocation-id",
                "object": {"revocation": {
                    "revoked_at": datetime.now(UTC).isoformat(), "revoker_type": "MERCHANT",
                }},
            },
        }

    def _authenticate_connection_admin(self):
        self.env.ref("base.user_admin").password = "square-reconnect-admin"
        self.env["ir.config_parameter"].set_param("web.base.url", "https://shop.example.test")
        self.env["ir.config_parameter"].set_param("web.base.url.freeze", "True")
        self.authenticate("admin", "square-reconnect-admin")

    def _select_replacement_connection(self, external):
        replacement = self.env["square.online.connection"].create({
            "name": "Replacement Sandbox",
            "company_id": self.company.id,
            "application_id": "sandbox-sq0idp-replacement",
            "application_secret": "replacement-secret",
        })
        self._authorize_connection(
            replacement, external, merchant="OTHER_MERCHANT", access="replacement-token", location="OTHER-LOC",
        )
        replacement.location_id = replacement.available_location_ids
        self.provider.square_online_sandbox_connection_id = replacement

    @patch(square_boundary)
    def test_revoked_connection_recovers_only_after_original_merchant_reconnects(self, external):
        self._configure_webhook()
        order, payment = self._accepted_charge_timeout(external)
        tx = order.transaction_ids
        other_order = self._make_order()
        draft = self._prepare(other_order, self._open_payment_page(other_order))
        event = self._revocation_event()

        self.assertEqual(self._send_webhook(event, environment="Production").status_code, 200)
        self.assertEqual(self._send_webhook(event, transform=lambda raw: raw.rstrip()).status_code, 403)
        self.env.invalidate_all()
        self.assertTrue(self.connection.access_token)

        self.assertEqual(self._send_webhook(event).status_code, 200)
        with self.assertRaises(JsonRpcException):
            self._submit(draft)
        self.env.invalidate_all()
        self.assertFalse(self.connection.access_token)
        self.assertEqual(self.connection.oauth_health, "reconnect")
        self.assertEqual(tx.state, "pending")
        self.assertEqual(other_order.transaction_ids.state, "draft")
        external.assert_called_once()

        self._authenticate_connection_admin()
        self._authorize_connection(self.connection, external, merchant="OTHER_MERCHANT", access="wrong-token")
        self.assertIn("different merchant", self.connection.oauth_status_message)
        self.assertEqual(self.connection.oauth_health, "reconnect")
        self.assertEqual(self.connection.merchant_id, "MERCHANT_TEST")

        self._select_replacement_connection(external)
        external.reset_mock()
        external.side_effect = AssertionError("Recovery must wait for the original merchant.")
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.env.invalidate_all()
        self.assertIn("Reauthorize the original connection", tx.square_online_recovery_message)
        self.assertEqual(tx.state, "pending")
        self.assertEqual(tx.square_online_connection_id, self.connection)
        external.assert_not_called()

        self._authorize_connection(self.connection, external, merchant="MERCHANT_TEST", access="restored-token")
        self.assertEqual(self._send_webhook(event).status_code, 200)
        self.env.invalidate_all()
        self.assertEqual(self.connection.oauth_health, "authorized")
        external.side_effect = [self._square_response({"payments": [payment]})]
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.env.invalidate_all()
        self.assertEqual(tx.state, "done")
        self.assertEqual(tx.square_online_connection_id, self.connection)
        self.assertEqual(tx.square_online_location_id, "SQUARE-NY")
        self.assertFalse(tx.square_online_recovery_message)
        self.assertEqual(external.call_args.args, ("GET", "https://connect.squareupsandbox.com/v2/payments"))
        self.assertEqual(external.call_args.kwargs["headers"]["Authorization"], "Bearer restored-token")

    @patch(square_boundary)
    def test_captured_payment_and_pending_refund_keep_the_original_connection(self, external):
        self._configure_webhook()
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order, payment = self._accepted_charge_timeout(external)
        self.assertEqual(self._signed_webhook(payment).status_code, 200)
        post_process = self.env.ref("payment.cron_post_process_payment_tx")
        post_process.method_direct_trigger()
        self.env.invalidate_all()
        tx = order.transaction_ids
        incoming = tx.payment_id
        invoice = order.invoice_ids
        self.assertEqual(incoming.move_id.state, "posted")
        self.assertEqual(invoice.amount_residual, 0)

        manager = new_test_user(
            self.env, login="square-historical-refund-manager", groups="account.group_account_manager",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        self.authenticate(manager.login, manager.login)
        first_wizard = self._open_refund(incoming, amount=12)
        self._call("payment.refund.wizard", "action_refund", first_wizard, button=True)
        self.env.invalidate_all()
        pending = tx.child_transaction_ids
        first_refund = {
            "id": "SQUARE-PARTIAL-REFUND",
            "payment_id": payment["id"],
            "location_id": "SQUARE-NY",
            "amount_money": {"amount": 1200, "currency": "USD"},
            "reason": pending.square_online_refund_reason,
            "status": "PENDING",
        }
        dispatch = self.env.ref("payment_square_online.cron_square_online_dispatch_refunds")
        external.side_effect = [
            self._square_response({"payment": payment}),
            self._square_response({"refund": first_refund}),
        ]
        dispatch.method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(pending.state, "pending")
        self.assertEqual(pending.provider_reference, first_refund["id"])
        self.assertFalse(pending.payment_id)
        self.assertEqual(incoming.amount_available_for_refund, 35)

        self.assertEqual(self._send_webhook(self._revocation_event()).status_code, 200)
        self._authenticate_connection_admin()
        self._select_replacement_connection(external)
        self.authenticate(manager.login, manager.login)
        external.reset_mock()
        external.side_effect = AssertionError("The replacement connection cannot recover historical operations.")
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.assertEqual(self._call_refresh(pending)["tag"], "reload")
        self.env.invalidate_all()
        self.assertEqual(tx.state, "done")
        self.assertEqual(pending.state, "pending")
        for operation in tx | pending:
            self.assertIn("Reauthorize the original connection", operation.square_online_recovery_message)
        external.assert_not_called()

        self._authenticate_connection_admin()
        self._authorize_connection(self.connection, external, merchant="MERCHANT_TEST", access="restored-token")
        self._expire_access_token(self.connection)
        self.authenticate(manager.login, manager.login)
        first_refund["status"] = "COMPLETED"
        captured = {
            **payment, "refund_ids": [first_refund["id"]],
            "refunded_money": first_refund["amount_money"],
        }
        external.reset_mock()
        external.side_effect = [
            self._square_response({
                "merchant_id": "MERCHANT_TEST",
                "access_token": "renewed-historical-token",
                "refresh_token": "restored-token-refresh",
                "token_type": "bearer",
                "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }),
            self._square_response({"payment": captured}),
            self._square_response({"refund": {**first_refund, "status": "PENDING"}}),
            self._square_response({"refund": first_refund}),
        ]
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.assertEqual(self._call_refresh(pending)["tag"], "reload")
        post_process.method_direct_trigger()
        self.env.invalidate_all()
        self.assertEqual(pending.state, "done")
        self.assertEqual(pending.payment_id.amount, 12)
        self.assertEqual(incoming.amount_available_for_refund, 35)

        next_wizard = self._open_refund(incoming)
        self._call("payment.refund.wizard", "action_refund", next_wizard, button=True)
        self.env.invalidate_all()
        remainder = tx.child_transaction_ids - pending
        external.side_effect = [
            self._square_response({"payment": captured}),
            self._square_response({"refund": first_refund}),
            self._square_response({"refund": {
                **first_refund, "id": "SQUARE-REMAINDER-REFUND", "reason": remainder.square_online_refund_reason,
                "amount_money": {"amount": 3500, "currency": "USD"},
            }}),
        ]
        dispatch.method_direct_trigger()
        post_process.method_direct_trigger()
        self.env.invalidate_all()
        api = "https://connect.squareupsandbox.com/v2"
        self.assertEqual([call.args for call in external.call_args_list], [
            ("POST", "https://connect.squareupsandbox.com/oauth2/token"),
            ("GET", f"{api}/payments/{payment['id']}"),
            ("GET", f"{api}/refunds/{first_refund['id']}"),
            ("GET", f"{api}/refunds/{first_refund['id']}"),
            ("GET", f"{api}/payments/{payment['id']}"),
            ("GET", f"{api}/refunds/{first_refund['id']}"),
            ("POST", f"{api}/refunds"),
        ])
        renewal, *requests = external.call_args_list
        self.assertEqual(renewal.kwargs["json"]["grant_type"], "refresh_token")
        self.assertEqual(renewal.kwargs["json"]["client_id"], self.connection.application_id)
        self.assertEqual(renewal.kwargs["json"]["refresh_token"], "restored-token-refresh")
        for call in requests:
            self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer renewed-historical-token")
        self.assertEqual(external.call_args.kwargs["json"]["payment_id"], payment["id"])
        self.assertEqual(external.call_args.kwargs["json"]["amount_money"], {"amount": 3500, "currency": "USD"})
        self.assertEqual(remainder.state, "done")
        self.assertEqual(remainder.payment_id.amount, 35)
        self.assertEqual(incoming.amount_available_for_refund, 0)
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.invoice_ids, invoice)
        self.assertEqual(invoice.amount_residual, 0)
        self.assertEqual(tx.payment_id, incoming)
        for operation in tx | pending | remainder:
            self.assertEqual(operation.square_online_connection_id, self.connection)
            self.assertEqual(operation.square_online_location_id, "SQUARE-NY")
            self.assertFalse(operation.square_online_recovery_message)
        for refund in (pending | remainder).payment_id:
            self.assertEqual(refund.source_payment_id, incoming)
            self.assertEqual(refund.journal_id, self.journal)
            self.assertEqual(refund.payment_type, "outbound")
            self.assertEqual(refund.move_id.state, "posted")

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
        return self._call("payment.transaction", "action_square_online_refresh_status", tx.ids, button=True)

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
            "errors": [{"code": "SERVICE_UNAVAILABLE", "detail": "private retrieval error"}],
        }, 503)
        self.assertEqual(self._call_refresh(tx)["tag"], "reload")
        self.env.invalidate_all()
        self.assertEqual(tx.state, "pending")
        self.assertIn("Check again later.", tx.square_online_recovery_message)
        self.assertNotIn("private retrieval error", tx.square_online_recovery_message)
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

# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlencode, urlsplit

from odoo.exceptions import AccessError, ValidationError
from odoo.tests import Form, HttpCase, tagged
from odoo.tests.common import new_test_user


@tagged("post_install", "-at_install")
class TestSquareOnlineOAuth(HttpCase):
    callback_path = "/payment/square_online/oauth/callback"
    merchant_id = "SELLER_TEST"
    sandbox_host = "https://connect.squareupsandbox.com"
    production_host = "https://connect.squareup.com"
    square_boundary = "odoo.addons.payment_square_online.square_api.requests.request"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env["ir.config_parameter"].set_param("web.base.url", "https://checkout.example")
        cls.env["ir.config_parameter"].set_param("web.base.url.freeze", "True")
        cls.env.ref("base.user_admin").password = "square-admin-test"

    def setUp(self):
        super().setUp()
        self.authenticate("admin", "square-admin-test")

    def _create_connection(self, environment="sandbox"):
        prefix = "sandbox-" if environment == "sandbox" else ""
        with Form(self.env["square.online.connection"]) as form:
            form.name = f"Square {environment}"
            form.environment = environment
            form.application_id = f"{prefix}sq0idp-test-application"
            form.application_secret = f"square-{environment}-application-secret"
        return form.record

    def _call_connect(self, connection):
        return self.url_open(
            "/web/dataset/call_button/square.online.connection/action_square_online_connect",
            data=json.dumps({
                "jsonrpc": "2.0",
                "method": "call",
                "params": {
                    "model": "square.online.connection",
                    "method": "action_square_online_connect",
                    "args": [[connection.id]],
                    "kwargs": {},
                },
            }),
            headers={"Content-Type": "application/json"},
        ).json()
    def _start(self, connection):
        response = self._call_connect(connection)
        self.assertIn("result", response, response)
        url = response["result"]["url"]
        return url, parse_qs(urlsplit(url).query)["state"][0]

    def _callback(self, **params):
        response = self.url_open(
            f"{self.callback_path}?{urlencode(params)}", allow_redirects=False
        )
        self.env.invalidate_all()
        return response

    def _token(self, merchant_id=None):
        return {
            "merchant_id": merchant_id or self.merchant_id,
            "access_token": "square-test-access-token",
            "refresh_token": "square-test-refresh-token",
            "token_type": "bearer",
            "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
        }

    def _locations(self, merchant_id=None):
        return {"locations": [{
            "id": "SQUARE-PARIS",
            "name": "Paris Store",
            "merchant_id": merchant_id or self.merchant_id,
            "country": "FR",
            "currency": "EUR",
            "status": "ACTIVE",
            "capabilities": ["CREDIT_CARD_PROCESSING"],
        }]}

    def _square_response(self, body, status=200):
        return Mock(status_code=status, json=Mock(return_value=body))

    @patch(square_boundary)
    def test_authorizes_merchant_and_loads_eligible_location(self, square_request):
        connection = self._create_connection()
        authorization_url, state = self._start(connection)
        self.assertTrue(authorization_url.startswith(f"{self.sandbox_host}/oauth2/authorize?"))

        square_request.side_effect = [
            self._square_response(self._token()),
            self._square_response(self._locations()),
        ]
        with self.assertLogs("werkzeug", level="INFO") as captured:
            response = self._callback(state=state, code="square-auth-code")
        self.assertEqual(response.status_code, 303)
        self.assertIn(f"GET {self.callback_path} HTTP/1.1", "\n".join(captured.output))
        self.assertNotIn("square-auth-code", "\n".join(captured.output))
        self.assertNotIn(state, "\n".join(captured.output))
        self.assertNotIn("square-auth-code", response.headers["Location"])
        self.assertEqual(response.headers["Referrer-Policy"], "no-referrer")
        self.assertEqual(connection.merchant_id, self.merchant_id)
        location = connection.available_location_ids
        self.assertEqual(location.name, "Paris Store")
        self.assertEqual(location.currency_code, "EUR")
        self.assertEqual(location.country_code, "FR")

        with Form(connection) as form:
            form.location_id = location
        self.assertEqual(connection.location_id.square_id, "SQUARE-PARIS")
        self.assertFalse(connection.application_secret)
        self.assertNotEqual(connection.access_token, "square-test-access-token")
        self.assertNotEqual(connection.refresh_token, "square-test-refresh-token")
        urls = [call.args[1] for call in square_request.call_args_list]
        self.assertEqual(urls, [
            f"{self.sandbox_host}/oauth2/token",
            f"{self.sandbox_host}/v2/locations",
        ])

    @patch(square_boundary)
    def test_denial_consumes_state_without_connecting(self, square_request):
        connection = self._create_connection()
        _, state = self._start(connection)

        denied = self._callback(state=state, error="access_denied", error_description="ignored")
        self.assertEqual(denied.status_code, 303)
        self.assertEqual(connection.oauth_status_message, "Square authorization was declined.")
        self.assertFalse(connection.merchant_id)
        self.assertFalse(connection.available_location_ids)
        replay = self._callback(state=state, code="square-auth-code")
        self.assertEqual(replay.status_code, 400)
        square_request.assert_not_called()

    @patch(square_boundary)
    def test_callback_requires_original_admin_session_and_one_time_state(self, square_request):
        connection = self._create_connection()
        _, first_state = self._start(connection)
        self.assertEqual(self._callback(state="tampered", code="square-auth-code").status_code, 400)
        self.assertEqual(
            self._callback(state=first_state, code="square-auth-code", error="unknown").status_code,
            303,
        )
        self.assertFalse(connection.merchant_id)
        _, state = self._start(connection)
        self.authenticate(None, None)
        self.assertEqual(self._callback(state=state, code="square-auth-code").status_code, 403)
        self.authenticate("admin", "square-admin-test")
        self.assertEqual(self._callback(state=state, code="square-auth-code").status_code, 400)
        self.assertFalse(connection.merchant_id)
        square_request.assert_not_called()

        _, state = self._start(connection)
        square_request.side_effect = [
            self._square_response(self._token()),
            self._square_response(self._locations()),
        ]
        self.assertEqual(self._callback(state=state, code="square-auth-code").status_code, 303)
        self.assertEqual(connection.merchant_id, self.merchant_id)
        self.assertEqual(self._callback(state=state, code="square-auth-code").status_code, 400)
        self.assertEqual(square_request.call_count, 2)

    @patch(square_boundary)
    def test_oauth_rejects_locations_for_a_different_merchant(self, square_request):
        connection = self._create_connection()
        _, state = self._start(connection)
        square_request.side_effect = [
            self._square_response(self._token()),
            self._square_response(self._locations("OTHER_MERCHANT")),
        ]
        response = self._callback(state=state, code="square-auth-code")
        self.assertEqual(response.status_code, 303)
        self.assertIn("could not verify", connection.oauth_status_message)
        self.assertFalse(connection.access_token)
        self.assertFalse(connection.merchant_id)
        self.assertFalse(connection.available_location_ids)

    @patch(square_boundary)
    def test_switching_provider_state_keeps_environment_credentials_isolated(self, square_request):
        sandbox = self._create_connection()
        production = self._create_connection("production")
        self.env["ir.config_parameter"].set_param("web.base.url", "http://localhost:8070")
        sandbox_url, sandbox_state = self._start(sandbox)
        self.assertTrue(sandbox_url.startswith(f"{self.sandbox_host}/oauth2/authorize?"))
        self.env["ir.config_parameter"].set_param("web.base.url", "https://checkout.example")
        production_url, production_state = self._start(production)
        self.assertTrue(production_url.startswith(f"{self.production_host}/oauth2/authorize?"))
        self.assertEqual(parse_qs(urlsplit(production_url).query)["session"], ["false"])

        production_merchant = "PRODUCTION_MERCHANT"
        production_token = {**self._token(production_merchant), "access_token": "production-token"}
        square_request.side_effect = [
            self._square_response(self._token()),
            self._square_response(self._locations()),
            self._square_response(production_token),
            self._square_response({
                "client_id": production.application_id,
                "merchant_id": production_merchant,
                "scopes": ["MERCHANT_PROFILE_READ", "PAYMENTS_READ", "PAYMENTS_WRITE"],
            }),
            self._square_response(self._locations(production_merchant)),
        ]
        self.assertEqual(self._callback(state=sandbox_state, code="sandbox-code").status_code, 303)
        self.assertFalse(production.merchant_id)
        self.assertEqual(self._callback(state=production_state, code="production-code").status_code, 303)

        sandbox.location_id = sandbox.available_location_ids
        production.location_id = production.available_location_ids
        self.assertEqual(sandbox.merchant_id, self.merchant_id)
        self.assertEqual(production.merchant_id, production_merchant)
        requests = square_request.call_args_list
        self.assertEqual(requests[0].kwargs["json"]["client_secret"], "square-sandbox-application-secret")
        self.assertEqual(requests[2].kwargs["json"]["client_secret"], "square-production-application-secret")
        self.assertEqual(requests[3].args[1], f"{self.production_host}/oauth2/token/status")
        self.assertEqual(requests[4].kwargs["headers"]["Authorization"], "Bearer production-token")

        provider = self.env.ref("payment_square_online.payment_provider_square_online")
        with self.assertRaises(ValidationError):
            with self.env.cr.savepoint():
                provider.square_online_sandbox_connection_id = production

        journal = self.env["account.journal"].search([
            ("company_id", "=", provider.company_id.id), ("type", "=", "bank"),
        ], limit=1)
        self.assertTrue(journal)
        with Form(provider) as form:
            form.square_online_sandbox_connection_id = sandbox
            form.square_online_production_connection_id = production
            form.state = "test"
            form.journal_id = journal
        self.assertEqual(provider.state, "test")
        self.assertEqual(provider.journal_id, journal)
        self.assertIn(
            provider,
            self.env["payment.provider"]._get_compatible_providers(
                provider.company_id.id, self.env.user.partner_id.id, 5.0, self.env.ref("base.EUR").id,
            ),
        )
        self.assertNotIn(
            provider,
            self.env["payment.provider"]._get_compatible_providers(
                provider.company_id.id, self.env.user.partner_id.id, 5.0, self.env.ref("base.USD").id,
            ),
        )
        provider.state = "enabled"
        self.assertEqual(provider.state, "enabled")
        provider.state = "disabled"
        self.assertEqual(provider.state, "disabled")

    def test_direct_setup_and_credentials_require_an_administrator_in_the_owner_company(self):
        other_company = self.env["res.company"].create({"name": "Separate Merchant"})
        external_admin = new_test_user(
            self.env, "square-separate-admin", groups="base.group_system",
            company_id=other_company.id, company_ids=[(6, 0, other_company.ids)],
        )
        portal = new_test_user(self.env, "square-portal-test", groups="base.group_portal")
        connection = self._create_connection()
        for user in (portal, external_admin):
            with self.subTest(user=user.login):
                with self.assertRaises(AccessError):
                    connection.with_user(user).read(["application_secret_encrypted"])
                self.authenticate(user.login, user.login)
                response = self._call_connect(connection)
                self.assertIn("AccessError", response["error"]["data"]["name"])

        with self.assertRaises(AccessError):
            connection.write({"access_token": "forged"})
        with self.assertRaises(AccessError):
            self.env["square.online.connection"].with_user(external_admin).create({
                "name": "Wrong Company",
                "company_id": connection.company_id.id,
                "application_id": "sandbox-sq0idp-test-application",
            })

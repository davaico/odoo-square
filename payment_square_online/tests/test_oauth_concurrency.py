# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from time import monotonic, sleep
from unittest.mock import Mock, patch

from odoo import SUPERUSER_ID, api, fields
from odoo.orm.registry import DummyRLock, Registry
from odoo.tests import TransactionCase, tagged

from ..square_api import SquareAPIError


@tagged("post_install", "-at_install")
class TestSquareOnlineOAuthConcurrency(TransactionCase):
    square_boundary = "odoo.addons.payment_square_online.square_api.requests.request"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Odoo's test loader holds the process-level registry lock. Use its
        # HTTP-test lock override without replacing the real PostgreSQL cursors.
        cls.startClassPatcher(patch.object(Registry, "_lock", DummyRLock()))

    def _connections(self, cr):
        return api.Environment(cr, SUPERUSER_ID, {})["square.online.connection"]

    @staticmethod
    def _response(body, status=200):
        return Mock(status_code=status, json=Mock(return_value=body))

    def _token(self, access_token):
        return {
            "merchant_id": "SQUARE_CONCURRENT_MERCHANT",
            "access_token": access_token,
            "refresh_token": "square-concurrent-refresh-test",
            "token_type": "bearer",
            "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
        }

    def _create_committed_connection(self):
        # The usual HTTP test cursor serializes requests on a single PostgreSQL
        # session. Here each worker must see a committed fixture in its own session.
        with self.env.registry.cursor() as cr:
            connection = self._connections(cr).create({
                "name": "Concurrent Square OAuth fixture",
                "application_id": "sandbox-sq0idp-concurrent",
                "application_secret": "square-concurrent-secret-test",
            })
            with patch(self.square_boundary) as external:
                external.side_effect = [
                    self._response(self._token("original-access-test")),
                    self._response({"locations": []}),
                ]
                connection._square_online_authorize("concurrent-fixture-code")
            connection_id = connection.id
        self.addCleanup(self._delete_connection, connection_id)
        return connection_id

    def _delete_connection(self, connection_id):
        with self.env.registry.cursor() as cr:
            self._connections(cr).browse(connection_id).unlink()

    def _expire_access(self, connection_id):
        with self.env.registry.cursor() as cr:
            cr.execute(
                "UPDATE square_online_connection SET token_expires_at = %s WHERE id = %s",
                (fields.Datetime.subtract(fields.Datetime.now(), minutes=1), connection_id),
            )

    def _read_payment(self, connection_id, payment_id):
        with self.env.registry.cursor() as cr:
            connection = self._connections(cr).browse(connection_id)
            return connection._square_online_get_payment(payment_id)

    def _wait_for_row_lock(self, connection_id):
        deadline = monotonic() + 10
        while monotonic() < deadline:
            with self.env.registry.cursor() as cr:
                cr.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_stat_activity
                        WHERE datname = current_database() AND wait_event_type = 'Lock'
                          AND query LIKE %s
                    )
                """, (f'SELECT id FROM "square_online_connection" WHERE id = {connection_id} FOR NO KEY UPDATE',))
                if cr.fetchone()[0]:
                    return
            sleep(0.025)
        self.fail("The second request did not contend for the OAuth credential row.")

    @patch(square_boundary)
    def test_concurrent_recovery_renews_expired_access_once(self, external):
        connection_id = self._create_committed_connection()
        self._expire_access(connection_id)
        renew_started = Event()
        release_renewal = Event()

        def respond(method, url, *, headers, json, **kwargs):
            if url.endswith("/oauth2/token"):
                self.assertEqual(method, "POST")
                self.assertEqual(json["refresh_token"], "square-concurrent-refresh-test")
                renew_started.set()
                self.assertTrue(release_renewal.wait(15), "Timed out waiting for the second request")
                return self._response(self._token("shared-renewed-token-test"))
            self.assertEqual(method, "GET")
            self.assertEqual(headers["Authorization"], "Bearer shared-renewed-token-test")
            return self._response({"payment": {"id": url.rsplit("/", 1)[1]}})

        external.side_effect = respond
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self._read_payment, connection_id, "payment-first")
            try:
                self.assertTrue(renew_started.wait(10))
                second = pool.submit(self._read_payment, connection_id, "payment-second")
                self._wait_for_row_lock(connection_id)
            finally:
                release_renewal.set()
            self.assertEqual(first.result(timeout=15), {"id": "payment-first"})
            self.assertEqual(second.result(timeout=15), {"id": "payment-second"})
        self.assertEqual([call.args[0] for call in external.call_args_list], ["POST", "GET", "GET"])

    @patch(square_boundary)
    def test_old_request_cannot_invalidate_replaced_access(self, external):
        connection_id = self._create_committed_connection()
        old_read_started = Event()
        release_old_response = Event()

        def respond(method, url, *, headers, **kwargs):
            if url.endswith("/payment-before-renewal"):
                self.assertEqual(headers["Authorization"], "Bearer original-access-test")
                old_read_started.set()
                self.assertTrue(release_old_response.wait(15), "Timed out waiting for renewal")
                return self._response({"errors": [{"code": "ACCESS_TOKEN_REVOKED"}]}, 401)
            if method == "POST":
                self.assertTrue(url.endswith("/oauth2/token"))
                return self._response(self._token("replaced-access-test"))
            self.assertEqual(headers["Authorization"], "Bearer replaced-access-test")
            return self._response({"payment": {"id": url.rsplit("/", 1)[1]}})

        external.side_effect = respond
        with ThreadPoolExecutor(max_workers=1) as pool:
            old = pool.submit(self._read_payment, connection_id, "payment-before-renewal")
            try:
                self.assertTrue(old_read_started.wait(10))
                self._expire_access(connection_id)
                self.assertEqual(
                    self._read_payment(connection_id, "payment-after-renewal"), {"id": "payment-after-renewal"},
                )
            finally:
                release_old_response.set()
            with self.assertRaises(SquareAPIError):
                old.result(timeout=15)
        self.assertEqual(
            self._read_payment(connection_id, "another-payment"), {"id": "another-payment"},
        )
        with self.env.registry.cursor() as cr:
            self.assertEqual(self._connections(cr).browse(connection_id).oauth_health, "authorized")
        self.assertEqual([call.args[0] for call in external.call_args_list], ["GET", "POST", "GET", "GET"])

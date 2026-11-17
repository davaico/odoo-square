"""Run with an installed test database in an ordinary Odoo shell:

    from odoo.addons.payment_square_online.tests.test_refund_concurrency import run
    run(env.registry)

Odoo's in-process test runner holds a Python registry lock that blocks independent workers.
"""

import json
import os
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import timedelta
from threading import Event
from time import monotonic, sleep
from unittest import TestCase, TestLoader, TextTestRunner
from unittest.mock import patch
from uuid import uuid4

from cryptography.fernet import Fernet
from requests import Response

from odoo import SUPERUSER_ID, api, fields
from odoo.exceptions import UserError, ValidationError
from odoo.service.model import retrying


class TestSquareRefundConcurrency(TestCase):
    def test_overlapping_wizards_cannot_reserve_more_than_the_capture(self):
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            for second_amount in (30, 50):
                with self.subTest(second_amount=second_amount):
                    self._assert_overlapping_refunds(second_amount)

    def test_external_refund_ingestion_serializes_with_an_overlapping_wizard(self):
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            source_id, wizard_ids, actor_id = self._prepare_refund_wizards(30)
            with self.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                source = env['payment.transaction'].browse(source_id)
                payment_ref = source.provider_reference
                refund_ref = f'{payment_ref}-external'
                env['payment.square.webhook.event'].create({
                    'connection_id': source.square_connection_id.id, 'event_id': refund_ref,
                    'event_type': 'refund.created', 'merchant_id': source.square_merchant_id,
                    'environment': 'sandbox', 'resource_id': refund_ref, 'resource_reference': payment_ref,
                })

            def square(method, url, **kwargs):
                self.assertEqual((method, url), ('GET', f'https://connect.squareupsandbox.com/v2/refunds/{refund_ref}'))
                response = Response()
                response.status_code = 200
                response._content = json.dumps({'refund': {
                    'id': refund_ref, 'status': 'PENDING', 'location_id': 'RACELOCATION',
                    'payment_id': payment_ref, 'amount_money': {'amount': 3000, 'currency': 'USD'},
                }}).encode()
                return response

            def process_webhook():
                with self.registry.cursor() as cr:
                    env = api.Environment(cr, SUPERUSER_ID, {})
                    env.ref('payment_square_online.ir_cron_square_online_webhooks').method_direct_trigger()

            def submit_wizard():
                with self.registry.cursor() as cr:
                    env = api.Environment(cr, actor_id, {})
                    wizard = env['payment.refund.wizard'].browse(wizard_ids)
                    return retrying(wizard.action_refund, env)

            with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api_mock:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    with self.registry.cursor() as gate_cr:
                        gate_cr.execute(
                            'SELECT pg_backend_pid() FROM payment_transaction WHERE id = %s FOR NO KEY UPDATE',
                            [source_id],
                        )
                        gate_pid = gate_cr.fetchone()[0]
                        ingestion = pool.submit(process_webhook)
                        self._wait_for_waiting_reservations(gate_pid, count=1)
                        submission = pool.submit(submit_wizard)
                        self._wait_for_waiting_reservations(gate_pid)

                    ingestion.result(timeout=10)
                    with self.assertRaisesRegex(ValidationError, 'remaining refundable'):
                        submission.result(timeout=10)
                api_mock.assert_called_once()

            with self.registry.cursor() as cr:
                source = api.Environment(cr, SUPERUSER_ID, {})['payment.transaction'].browse(source_id)
                child = source.child_transaction_ids
                self.assertEqual(len(child), 1)
                self.assertEqual((child.provider_reference, child.state, child.amount), (refund_ref, 'pending', -30))
                self.assertFalse(child.square_refund_requested_by_id)
                self.assertFalse(child.payment_id)
                self.assertEqual(source.payment_id.amount_available_for_refund, 20)

    def _assert_overlapping_refunds(self, second_amount):
        source_id, wizard_ids, actor_id = self._prepare_refund_wizards(30, second_amount)
        entered_square = Event()
        release_square = Event()

        def square_refund(method, url, **kwargs):
            self.assertEqual((method, url), ('POST', 'https://connect.squareupsandbox.com/v2/refunds'))
            request = kwargs['json']
            entered_square.set()
            self.assertTrue(release_square.wait(15), 'Timed out waiting to release the Square response')
            response = Response()
            response.status_code = 200
            response._content = json.dumps({'refund': {
                'id': request['idempotency_key'], 'status': 'PENDING', 'location_id': 'RACELOCATION',
                'payment_id': request['payment_id'], 'amount_money': request['amount_money'],
                'reason': request['reason'],
            }}).encode()
            return response

        def submit_wizard(wizard_id):
            with self.registry.cursor() as cr:
                env = api.Environment(cr, actor_id, {})
                wizard = env['payment.refund.wizard'].browse(wizard_id)
                return retrying(wizard.action_refund, env)

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_refund) as square:
            with ThreadPoolExecutor(max_workers=2) as pool:
                try:
                    # Force both reservation transactions to have a pre-refund snapshot and wait on
                    # the same source. A real PostgreSQL row lock and independent sessions do this.
                    with self.registry.cursor() as gate_cr:
                        gate_cr.execute(
                            'SELECT pg_backend_pid() FROM payment_transaction WHERE id = %s FOR NO KEY UPDATE',
                            [source_id],
                        )
                        gate_pid = gate_cr.fetchone()[0]
                        attempts = [pool.submit(submit_wizard, wizard_id) for wizard_id in wizard_ids]
                        self._wait_for_waiting_reservations(gate_pid)

                    self.assertTrue(entered_square.wait(10), 'Neither request reached Square')
                    rejected, awaiting_square = wait(attempts, timeout=10, return_when=FIRST_COMPLETED)
                    self.assertEqual(len(rejected), 1, 'Exactly one concurrent request must be rejected before Square replies')
                    rejected, = rejected
                    awaiting_square, = awaiting_square
                    with self.assertRaisesRegex((UserError, ValidationError), 'no amount available|remaining refundable'):
                        rejected.result()
                    # Before releasing the external response, a third connection already observes
                    # the winning reservation and the reduced balance.
                    with self.registry.cursor() as reader_cr:
                        source = api.Environment(reader_cr, SUPERUSER_ID, {})['payment.transaction'].browse(source_id)
                        refund = source.child_transaction_ids
                        self.assertEqual(len(refund), 1)
                        self.assertEqual(refund.state, 'pending')
                        self.assertFalse(refund.provider_reference)
                        self.assertEqual(source.payment_id.amount_available_for_refund, 50 + refund.amount)
                        refund_id = refund.id
                finally:
                    release_square.set()
                self.assertEqual(awaiting_square.result(timeout=10)['res_id'], refund_id)
            square.assert_called_once()

        with self.registry.cursor() as cr:
            source = api.Environment(cr, SUPERUSER_ID, {})['payment.transaction'].browse(source_id)
            self.assertEqual(source.child_transaction_ids.ids, [refund_id])
            self.assertEqual(source.child_transaction_ids.state, 'pending')
            self.assertTrue(source.child_transaction_ids.provider_reference)
            self.assertFalse(source.child_transaction_ids.payment_id)

    def _wait_for_waiting_reservations(self, gate_pid, count=2):
        end = monotonic() + 10
        with self.registry.cursor() as cr:
            while monotonic() < end:
                cr.execute('SELECT pg_stat_clear_snapshot()')
                cr.execute("""
                    WITH RECURSIVE waiting(pid) AS (
                        SELECT %s::integer
                        UNION
                        SELECT activity.pid
                        FROM pg_stat_activity AS activity
                        JOIN waiting ON waiting.pid = ANY(pg_blocking_pids(activity.pid))
                        WHERE activity.datname = current_database()
                    )
                    SELECT COUNT(*) - 1 FROM waiting
                """, [gate_pid])
                if cr.fetchone()[0] >= count:
                    return
                sleep(0.02)
        self.fail(f'{count} independent PostgreSQL operations did not wait on the same captured payment')

    def _prepare_refund_wizards(self, *amounts):
        unique = uuid4().hex
        with self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            provider = env.ref('payment_square_online.payment_provider_square_online').copy({
                'name': f'Square refund overlap {unique}',
                'square_application_id': 'sandbox-sq0idb-refund-race',
                'square_application_secret': 'sandbox-secret',
            })
            journal = env['account.journal'].search([
                ('type', '=', 'bank'), ('company_id', '=', provider.company_id.id),
            ], limit=1)
            provider.journal_id = journal
            connection = env['payment.square.connection'].search([('provider_id', '=', provider.id)])
            connection.write({
                'merchant_id': unique, 'access_token': connection._seal('sandbox-access'),
                'expires_at': fields.Datetime.now() + timedelta(days=1),
            })
            payment_method = journal.inbound_payment_method_line_ids.filtered(
                lambda line: line.payment_provider_id == provider,
            )
            source = env['payment.transaction'].create({
                'provider_id': provider.id, 'payment_method_id': env.ref('payment.payment_method_card').id,
                'partner_id': env.ref('base.main_partner').id, 'currency_id': env.ref('base.USD').id,
                'amount': 50, 'operation': 'online_direct', 'state': 'done', 'provider_reference': unique,
                'square_connection_id': connection.id, 'square_merchant_id': unique,
                'square_environment': 'sandbox', 'square_application_id': provider.square_application_id,
                'square_location_ref': 'RACELOCATION', 'square_currency_code': 'USD', 'square_amount_minor': 5000,
                'square_journal_id': journal.id, 'square_outstanding_account_id': payment_method.payment_account_id.id,
                'square_reserved_at': fields.Datetime.now(), 'square_dispatched_at': fields.Datetime.now(),
            })
            source._create_payment()
            actor = env.ref('base.user_admin')
            wizards = env['payment.refund.wizard'].with_user(actor).create([
                {'payment_id': source.payment_id.id, 'amount_to_refund': amount} for amount in amounts
            ])
            self.addCleanup(self._remove_fixture, source.id, wizards.ids, provider.id)
            return source.id, wizards.ids, actor.id

    def _remove_fixture(self, source_id, wizard_ids, provider_id):
        with self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env['payment.refund.wizard'].browse(wizard_ids).exists().unlink()
            source = env['payment.transaction'].browse(source_id).exists()
            source.child_transaction_ids.unlink()
            if source.payment_id:
                source.payment_id.action_draft()
                source.payment_id.unlink()
            source.unlink()
            env['account.payment.method.line'].search([('payment_provider_id', '=', provider_id)]).unlink()
            env['payment.provider'].browse(provider_id).exists().unlink()


def run(registry):
    TestSquareRefundConcurrency.registry = registry
    suite = TestLoader().loadTestsFromTestCase(TestSquareRefundConcurrency)
    if not TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        raise SystemExit(1)

import json
import os
from datetime import timedelta
from functools import partial
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

from cryptography.fernet import Fernet
from requests import Response

from odoo import SUPERUSER_ID, api, fields
from odoo.exceptions import AccessError, ValidationError
from odoo.service.model import retrying
from odoo.tests import Form, HttpCase, TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestSquareConnection(HttpCase):
    def test_installs_as_disabled_standalone_provider(self):
        provider = self.env.ref('payment_square_online.payment_provider_square_online')
        self.assertEqual(provider.code, 'square_online')
        self.assertEqual(provider.state, 'disabled')
        self.assertFalse(provider.is_published)
        self.assertTrue(self.env['account.payment.method'].search_count([
            ('code', '=', 'square_online'), ('payment_type', '=', 'inbound'),
        ]))
        module = self.env['ir.module.module'].search([('name', '=', 'payment_square_online')])
        self.assertNotIn('odoo_square', module.dependencies_id.mapped('name'))

    def test_configuration_form_keeps_secret_write_only(self):
        provider = self.env.ref('payment_square_online.payment_provider_square_online')
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            with Form(provider) as form:
                form.square_application_id = 'sandbox-sq0idb-app'
                form.square_application_secret = 'sandbox-secret'
        self.assertEqual(provider.square_environment, 'sandbox')
        self.assertEqual(provider.square_application_id, 'sandbox-sq0idb-app')
        self.assertFalse(provider.square_application_secret)
        self.assertTrue(provider.square_has_application_secret)
        self.assertEqual(provider.square_connection_status, 'authorize')
        with self.assertRaises(ValidationError):
            with Form(provider) as form:
                form.state = 'test'
        self.assertFalse(Form(self.env['payment.provider']).square_connection_status)

    def test_square_online_webhook_events_are_private(self):
        admin = self.env.ref('base.user_admin')
        model = self.env['payment.square.webhook.event'].with_user(admin)
        for operation in ('read', 'write', 'create', 'unlink'):
            with self.subTest(operation=operation), self.assertRaises(AccessError):
                model.check_access(operation)

    def test_authorizes_sandbox_and_selects_owned_location_and_journal(self):
        provider = self.env.ref('payment_square_online.payment_provider_square_online')
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            self.authenticate('admin', 'admin')
            self.env['ir.config_parameter'].set_param('web.base.url', 'https://store.example.test')
            self._configure_application(provider, 'sandbox')
            authorization_url = self._authorize(provider)
            self.assertEqual(authorization_url.netloc, 'connect.squareupsandbox.com')
            query = parse_qs(authorization_url.query)
            self.assertEqual(query['client_id'], ['sandbox-sq0idb-app'])
            self.assertEqual(set(query['scope'][0].split()), {
                'MERCHANT_PROFILE_READ', 'ORDERS_READ', 'ORDERS_WRITE', 'PAYMENTS_READ', 'PAYMENTS_WRITE',
            })
            self.assertEqual(query['redirect_uri'], ['https://store.example.test/payment/square/online/oauth/callback'])
            with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_responses) as square:
                response = self._callback(query['state'][0], code='test-code')
            self.assertEqual(response.status_code, 303)
            self.assertNotIn('test-code', response.headers['Location'])
            self.assertNotIn('square-access-token', response.text)
            self.assertEqual([call.args[:2] for call in square.call_args_list], [
                ('POST', 'https://connect.squareupsandbox.com/oauth2/token'),
                ('GET', 'https://connect.squareupsandbox.com/v2/locations'),
            ])
            self.assertEqual(square.call_args_list[0].kwargs['json']['client_secret'], 'sandbox-secret')
            self.assertEqual(provider.square_merchant_id, 'SQUAREMERCHANT')
            self.assertEqual(provider.square_connection_status, 'select_location')
            location = self.env['payment.square.location'].search([('provider_id', '=', provider.id)])
            self.assertEqual(location.name, 'Tokyo (JP, JPY)')
            journal = self.env['account.journal'].search([
                ('company_id', '=', provider.company_id.id), ('type', '=', 'bank'),
            ], limit=1)
            with Form(provider) as form:
                form.square_location_id = location
                form.journal_id = journal
                form.state = 'test'
            self.assertEqual(provider.square_location_id.square_id, 'SQUARETOKYO')
            self.assertEqual(provider.square_location_id.currency_code, 'JPY')
            self.assertEqual(provider.journal_id, journal)
            self.assertTrue(journal.inbound_payment_method_line_ids.filtered(
                lambda line: line.payment_provider_id == provider,
            ))
            self.assertEqual(provider.state, 'test')
            self.assertEqual(provider.square_connection_status, 'ready')
            with self.assertRaises(AccessError):
                self.env['payment.square.connection'].with_user(self.env.ref('base.user_admin')).search_read(
                    [], ['access_token', 'refresh_token'],
                )
            journal.active = False
            provider.invalidate_recordset(['square_connection_status'])
            self.assertEqual(provider.square_connection_status, 'select_journal')

    def test_denied_callback_consumes_real_state_only_once(self):
        provider = self.env.ref('payment_square_online.payment_provider_square_online')
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            self.authenticate('admin', 'admin')
            self.env['ir.config_parameter'].set_param('web.base.url', 'https://store.example.test')
            self._configure_application(provider, 'sandbox')
            state = parse_qs(self._authorize(provider).query)['state'][0]
            with patch('odoo.addons.payment_square_online.square_api.request') as square:
                self.assertEqual(self._callback('0' * 43, code='fake-code').status_code, 400)
                self.assertFalse(provider.square_authorization_result)
                response = self._callback(state, error='access_denied', error_description='untrusted-detail')
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers['Cache-Control'], 'no-store')
                self.assertNotIn('untrusted-detail', response.text)
                self.assertEqual(provider.square_authorization_result, 'denied')
                self.assertEqual(self._callback(state, code='replay-code').status_code, 400)
                square.assert_not_called()
            self.assertFalse(provider.square_merchant_id)
            self.assertEqual(provider.square_connection_status, 'authorize')

    def test_oauth_state_rejects_different_admin_browser_session(self):
        provider = self.env.ref('payment_square_online.payment_provider_square_online')
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            self.authenticate('admin', 'admin')
            self.env['ir.config_parameter'].set_param('web.base.url', 'https://store.example.test')
            self._configure_application(provider, 'sandbox')
            state = parse_qs(self._authorize(provider).query)['state'][0]
            self.authenticate('admin', 'admin')
            with patch('odoo.addons.payment_square_online.square_api.request') as square:
                self.assertEqual(self._callback(state, code='wrong-session').status_code, 400)
                square.assert_not_called()
            self.assertFalse(provider.square_merchant_id)

    def test_employee_and_other_company_admin_cannot_configure_provider(self):
        provider = self.env.ref('payment_square_online.payment_provider_square_online')
        employee = new_test_user(self.env, login='square-employee', groups='base.group_user')
        # A branch administrator inherits the native parent-company provider rule.
        branch = self.env['res.company'].create({'name': 'Square branch', 'parent_id': provider.company_id.id})
        branch_admin = new_test_user(
            self.env, login='square-branch-admin', groups='base.group_system',
            company_id=branch.id, company_ids=[(6, 0, branch.ids)],
        )
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            self.authenticate('admin', 'admin')
            self.env['ir.config_parameter'].set_param('web.base.url', 'https://store.example.test')
            self._configure_application(provider, 'sandbox')
            state = parse_qs(self._authorize(provider).query)['state'][0]
            for user, expected_callback_status in [(employee, 403), (branch_admin, 400)]:
                self.authenticate(user.login, 'password')
                with patch('odoo.addons.payment_square_online.square_api.request') as square:
                    self.assertEqual(self._callback(state, code='unauthorized').status_code, expected_callback_status)
                    self.assertEqual(self._authorize_response(provider)['error']['data']['name'], 'odoo.exceptions.AccessError')
                    square.assert_not_called()
                with self.assertRaises(AccessError):
                    provider.with_user(user).read(['square_application_secret'])
        self.assertFalse(provider.square_merchant_id)

    def test_reconnect_rejects_different_merchant_and_first_connect_rejects_foreign_location(self):
        provider = self.env.ref('payment_square_online.payment_provider_square_online')
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            self.authenticate('admin', 'admin')
            self.env['ir.config_parameter'].set_param('web.base.url', 'https://store.example.test')
            self._configure_application(provider, 'sandbox')
            state = parse_qs(self._authorize(provider).query)['state'][0]
            with patch('odoo.addons.payment_square_online.square_api.request', side_effect=partial(
                self._square_responses, location_merchant='OTHERACCOUNT',
            )) as square:
                self.assertEqual(self._callback(state, code='foreign-location').status_code, 303)
                self.assertEqual(square.call_count, 2)
            self.assertEqual(provider.square_authorization_result, 'failed')
            self.assertFalse(provider.square_merchant_id)
            self.assertFalse(self.env['payment.square.location'].search([('provider_id', '=', provider.id)]))
            state = parse_qs(self._authorize(provider).query)['state'][0]
            with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_responses):
                self.assertEqual(self._callback(state, code='original-merchant').status_code, 303)
            self.assertEqual(provider.square_merchant_id, 'SQUAREMERCHANT')
            state = parse_qs(self._authorize(provider).query)['state'][0]
            with patch('odoo.addons.payment_square_online.square_api.request', side_effect=partial(
                self._square_responses, merchant='OTHERACCOUNT',
            )) as square:
                self.assertEqual(self._callback(state, code='mismatched-merchant').status_code, 303)
                self.assertEqual(self._callback(state, code='replay').status_code, 400)
                self.assertEqual(square.call_count, 1)
            self.assertEqual(provider.square_merchant_id, 'SQUAREMERCHANT')
            self.assertEqual(provider.square_authorization_result, 'merchant_mismatch')

    def test_environments_use_separate_grants_and_reject_foreign_location(self):
        sandbox = self.env.ref('payment_square_online.payment_provider_square_online')
        production = sandbox.copy({'name': 'Square Production'})
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            self.authenticate('admin', 'admin')
            self.env['ir.config_parameter'].set_param('web.base.url', 'http://localhost:8069')
            self._configure_application(sandbox, 'sandbox')
            sandbox_url = self._authorize(sandbox)
            sandbox_query = parse_qs(sandbox_url.query)
            self.assertEqual(sandbox_query['redirect_uri'], ['http://localhost:8069/payment/square/online/oauth/callback'])
            self._configure_application(production, 'production')
            self.assertEqual(self._authorize_response(production)['error']['data']['name'], 'odoo.exceptions.UserError')
            self.env['ir.config_parameter'].set_param('web.base.url', 'https://store.example.test')
            production_url = self._authorize(production)
            self.assertEqual(production_url.netloc, 'connect.squareup.com')
            production_query = parse_qs(production_url.query)
            self.assertEqual(production_query['client_id'], ['sq0idp-app'])
            self.assertNotEqual(sandbox_query['state'], production_query['state'])
            with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_responses) as square:
                self.assertEqual(self._callback(sandbox_query['state'][0], code='sandbox-code').status_code, 303)
                self.assertEqual(square.call_args_list[0].kwargs['json']['client_secret'], 'sandbox-secret')
            with patch('odoo.addons.payment_square_online.square_api.request', side_effect=partial(
                self._square_responses, merchant='PRODMERCHANT', location_id='PRODSQUARETOKYO',
            )) as square:
                self.assertEqual(self._callback(production_query['state'][0], code='production-code').status_code, 303)
                self.assertEqual([call.args[:2] for call in square.call_args_list], [
                    ('POST', 'https://connect.squareup.com/oauth2/token'),
                    ('GET', 'https://connect.squareup.com/v2/locations'),
                ])
                self.assertEqual(square.call_args_list[0].kwargs['json']['client_secret'], 'production-secret')
            self.assertEqual(sandbox.square_merchant_id, 'SQUAREMERCHANT')
            self.assertEqual(production.square_merchant_id, 'PRODMERCHANT')
            production_location = self.env['payment.square.location'].search([('provider_id', '=', production.id)])
            with self.assertRaisesRegex(ValidationError, 'from this Square connection'), self.cr.savepoint():
                sandbox.square_location_id = production_location
            with self.assertRaisesRegex(ValidationError, 'Duplicate the provider'), self.cr.savepoint():
                sandbox.square_environment = 'production'
            with self.assertRaisesRegex(ValidationError, 'Use Test Mode'), self.cr.savepoint():
                sandbox.state = 'enabled'
            with self.assertRaisesRegex(ValidationError, 'Use Test Mode'), self.cr.savepoint():
                production.state = 'test'

    def _configure_application(self, provider, environment):
        app_id = 'sandbox-sq0idb-app' if environment == 'sandbox' else 'sq0idp-app'
        with Form(provider) as form:
            form.square_environment = environment
            form.square_application_id = app_id
            form.square_application_secret = f'{environment}-secret'

    def _authorize(self, provider):
        response = self._authorize_response(provider)
        self.assertNotIn('error', response, response)
        return urlsplit(response['result']['url'])

    def _authorize_response(self, provider):
        return self.url_open('/web/dataset/call_button', data=json.dumps({
            'jsonrpc': '2.0',
            'method': 'call',
            'params': {
                'model': 'payment.provider', 'method': 'action_square_authorize',
                'args': [[provider.id]], 'kwargs': {},
            },
        }), headers={'Content-Type': 'application/json'}).json()

    def _callback(self, state, **query):
        params = urlencode({'state': state, **query})
        response = self.url_open(f'/payment/square/online/oauth/callback?{params}', allow_redirects=False)
        self.env.invalidate_all()
        return response

    @staticmethod
    def _square_responses(method, url, *, merchant='SQUAREMERCHANT', location_merchant=None,
                          location_id='SQUARETOKYO', **kwargs):
        if method == 'POST' and url.endswith('/oauth2/token'):
            payload = {
                'access_token': 'square-access-token', 'refresh_token': 'square-refresh-token',
                'merchant_id': merchant, 'token_type': 'bearer',
                'expires_at': '2099-03-17T10:00:00Z',
            }
        elif method == 'GET' and url.endswith('/v2/locations'):
            payload = {'locations': [{
                'id': location_id, 'name': 'Tokyo', 'merchant_id': location_merchant or merchant,
                'status': 'ACTIVE', 'country': 'JP', 'currency': 'JPY',
                'capabilities': ['CREDIT_CARD_PROCESSING'],
            }]}
        else:
            raise AssertionError(f'Unexpected Square request {method} {url}')
        response = Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        return response


@tagged('post_install', '-at_install')
class TestSquareAuthorizationRace(TransactionCase):
    def test_recovery_uses_the_grant_renewed_by_a_concurrent_scheduled_action(self):
        merchant_id = f'SQUARE-{uuid4().hex}'
        with patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()}):
            with self.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                provider = env.ref('payment_square_online.payment_provider_square_online').copy({
                    'square_application_id': 'sandbox-sq0idb-race', 'square_application_secret': 'sandbox-secret',
                })
                connection = env['payment.square.connection'].search([('provider_id', '=', provider.id)])
                connection.write({
                    'merchant_id': merchant_id, 'access_token': connection._seal('old-access-token'),
                    'refresh_token': connection._seal('square-refresh-token'),
                    'expires_at': fields.Datetime.now() + timedelta(days=5),
                })
                transaction = env['payment.transaction'].create({
                    'provider_id': provider.id, 'payment_method_id': env.ref('payment.payment_method_card').id,
                    'partner_id': env.ref('base.main_partner').id, 'currency_id': env.ref('base.JPY').id,
                    'amount': 1700, 'operation': 'online_direct', 'state': 'pending',
                    'square_connection_id': connection.id, 'square_merchant_id': merchant_id,
                    'square_environment': 'sandbox', 'square_application_id': 'sandbox-sq0idb-race',
                    'square_location_ref': 'SQUARETOKYO', 'square_currency_code': 'JPY', 'square_amount_minor': 1700,
                    'square_order_reference': f'odoo-web-{uuid4().hex[:23]}', 'square_order_id': 'SQUAREORDER',
                    'square_reserved_at': fields.Datetime.now(), 'square_dispatched_at': fields.Datetime.now(),
                })
                provider_id, transaction_id = provider.id, transaction.id
            self.addCleanup(self._remove_connection_fixture, provider_id, transaction_id)

            with self.registry.cursor() as stale_cr:
                stale_env = api.Environment(stale_cr, SUPERUSER_ID, {})
                tx = stale_env['payment.transaction'].browse(transaction_id)
                self.assertEqual(tx.state, 'pending')  # Take the recovery snapshot before the cron commits.
                calls = []

                def square(method, url, **kwargs):
                    self.assertTrue(url.startswith('https://connect.squareupsandbox.com/'))
                    calls.append((method, url.rsplit('/', 1)[-1]))
                    if method == 'POST' and url.endswith('/oauth2/token'):
                        self.assertEqual(kwargs['json']['grant_type'], 'refresh_token')
                        return TestSquareConnection._square_responses(method, url, merchant=merchant_id)
                    self.assertEqual(method, 'GET')
                    self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
                    response = Response()
                    response.status_code = 200
                    response._content = json.dumps({'payments': [{
                        'id': 'SQUAREPAYMENT', 'status': 'PENDING', 'source_type': 'CARD',
                        'location_id': 'SQUARETOKYO', 'order_id': 'SQUAREORDER',
                        'reference_id': tx.square_order_reference, 'amount_money': {'amount': 1700, 'currency': 'JPY'},
                    }]}).encode()
                    return response

                with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square):
                    with self.registry.cursor() as winning_cr:
                        winning_env = api.Environment(winning_cr, SUPERUSER_ID, {})
                        cron = winning_env.ref('payment_square_online.ir_cron_square_online_renew')
                        cron.method_direct_trigger()
                        cron.method_direct_trigger()
                    retrying(tx.action_square_refresh_status, stale_env)
                self.assertEqual(calls, [('POST', 'token'), ('GET', 'payments')])
                self.assertEqual(tx.provider_reference, 'SQUAREPAYMENT')
                self.assertEqual(tx.state, 'pending')
                self.assertFalse(tx.square_recovery_message)

    def _remove_connection_fixture(self, provider_id, transaction_id):
        with self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env['payment.transaction'].browse(transaction_id).unlink()
            env['payment.provider'].browse(provider_id).unlink()

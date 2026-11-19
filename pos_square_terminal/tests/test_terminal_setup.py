# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json
import os
from functools import partial
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit

from cryptography.fernet import Fernet
from requests import Response

from odoo import Command, fields
from odoo.exceptions import AccessError, UserError
from odoo.tests import Form, HttpCase, tagged


@tagged('post_install', '-at_install')
class TestSquareTerminalSetup(HttpCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        company = cls.env.company
        if not company.chart_template:
            cls.env['account.chart.template'].try_loading('generic_coa', company)
        cls.bank_journal = cls.env['account.journal'].search([
            ('company_id', '=', company.id), ('type', '=', 'bank'),
        ], limit=1)

    def setUp(self):
        super().setUp()
        self.admin = self.env.ref('base.user_admin')
        self.authenticate(self.admin.login, 'admin')
        self.env['ir.config_parameter'].set_param('web.base.url', 'https://register.example.test')
        key = patch.dict(os.environ, {'ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY': Fernet.generate_key().decode()})
        key.start()
        self.addCleanup(key.stop)

    def test_administrator_pairs_a_named_payment_method(self):
        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=self._square_response):
            connection = self._authorize_connection()
            terminal = self._prepare_terminal(connection)
            self._button(terminal, 'action_create_pairing_code')
            self.assertEqual(terminal.pairing_code, 'ABCDEF')
            self.assertEqual(terminal.readiness, 'awaiting_pairing')
            self._button(terminal, 'action_check_readiness')
            self.assertEqual(terminal.device_id, 'SQUARETERMINAL')
            self.assertEqual(terminal.readiness, 'ready')
            self.assertFalse(terminal.pairing_code)
            method = self._create_payment_method(terminal)
            self.assertEqual(method.name, 'Square Counter')
            self.assertFalse(method.square_terminal_unavailable_reason)

            eur_journal = self.env['account.journal'].create({
                'name': 'Setup EUR Sales', 'type': 'sale', 'code': 'SQEUR',
                'currency_id': self.env.ref('base.EUR').id,
            })
            counter, euro_register = self.env['pos.config'].create([
                {'name': 'Square setup register', 'payment_method_ids': [Command.set(method.ids)]},
                {
                    'name': 'Square EUR register', 'payment_method_ids': [Command.set(method.ids)],
                    'journal_id': eur_journal.id, 'use_pricelist': False,
                },
            ])
            counter.open_ui()
            euro_register.open_ui()
            self.assertFalse(self._register_payment_methods(counter)[method.id]['square_terminal_unavailable_reason'])
            self.assertIn(
                'Square location currency (USD)',
                self._register_payment_methods(euro_register)[method.id]['square_terminal_unavailable_reason'],
            )

    def test_incompatible_terminal_leaves_cash_payments_available(self):
        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=partial(
            self._square_response, application_version='6.61',
        )) as square:
            connection = self._authorize_connection()
            terminal = self._prepare_terminal(connection)
            self._button(terminal, 'action_create_pairing_code')
            self._button(terminal, 'action_check_readiness')
            self.assertEqual(terminal.readiness, 'incompatible')
            self.assertFalse(terminal.device_id)
            self.assertIn('6.62', terminal.unavailable_reason)
            method = self._create_payment_method(terminal)
            cash_journal = self.env['account.journal'].search([
                ('company_id', '=', connection.company_id.id), ('type', '=', 'cash'),
                ('pos_payment_method_ids', '=', False),
            ], limit=1)
            if not cash_journal:
                cash_journal = self.env['account.journal'].create({
                    'name': 'Setup Cash', 'type': 'cash', 'code': 'SQCT',
                    'company_id': connection.company_id.id,
                })
            cash = self.env['pos.payment.method'].create({'name': 'Setup Cash', 'journal_id': cash_journal.id})
            config = self.env['pos.config'].create({
                'name': 'Square setup register', 'payment_method_ids': [Command.set((method | cash).ids)],
            })
            self.env['product.product'].create({
                'name': 'Square setup product', 'available_in_pos': True, 'list_price': 10,
                'taxes_id': [Command.clear()],
            })
            config.open_ui()
            session = config.current_session_id
            session.set_opening_control(0, '')
            methods = self._register_payment_methods(config)
            self.assertIn('6.62', methods[method.id]['square_terminal_unavailable_reason'])
            self.assertFalse(methods[cash.id]['square_terminal_unavailable_reason'])
            square.reset_mock()
            self.start_tour(f'/pos/ui?config_id={config.id}', 'SquareTerminalIncompatibleDevice', login=self.admin.login)
            square.assert_not_called()
            self.env.invalidate_all()
            self.assertEqual(session.order_ids.payment_ids.payment_method_id, cash)
            self.assertEqual(session.order_ids.payment_ids.amount, 10)
            self.assertEqual(session.order_ids.state, 'paid')

    def test_square_setup_rejects_forged_provider_fields(self):
        connections = self.env['pos.square.connection'].with_user(self.admin)
        with self.assertRaises(AccessError), self.cr.savepoint():
            connections.with_context(default_merchant_id='FORGED').create({
                'name': 'Forged connection', 'application_id': 'sq0idp-app',
            })
        with self.assertRaises(AccessError), self.cr.savepoint():
            self.env['pos.square.terminal'].with_user(self.admin).with_context(default_readiness='ready').create({
                'name': 'Forged device',
            })
        credential = self.env['pos.square.credential'].with_user(self.admin)
        with self.assertRaises(AccessError):
            credential.search_read([], ['access_token'])

    def test_expired_grant_renews_for_the_original_location_request(self):
        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=self._square_response) as square:
            connection = self._authorize_connection()
            credential = connection._credential()
            credential.expires_at = fields.Datetime.subtract(fields.Datetime.now(), minutes=1)
            saved_grant = (credential.access_token, credential.refresh_token)

            square.reset_mock()
            square.side_effect = [self._http_response({'errors': [{'code': 'SERVICE_UNAVAILABLE'}]}, 503)]
            with self.assertRaisesRegex(UserError, 'could not renew'):
                connection.action_refresh_locations()
            self.assertEqual(square.call_count, 1)
            self.assertEqual((credential.access_token, credential.refresh_token), saved_grant)
            self.assertFalse(credential.reauthorization_required)

            square.reset_mock()
            square.side_effect = [self._http_response(self._renewed_tokens()), self._http_response({'locations': []})]
            self._button(connection, 'action_refresh_locations')
            renewal, locations = square.call_args_list
            self._assert_refresh_request(renewal, connection)
            self.assertEqual(locations.args, ('GET', 'https://connect.squareup.com/v2/locations'))
            self.assertEqual(locations.kwargs['headers']['Authorization'], 'Bearer renewed-square-access')
            self.assertEqual(connection.merchant_id, 'SQUAREMERCHANT')
            self.assertEqual(connection.authorization_result, 'connected')
            self.assertEqual(credential._unseal(credential.refresh_token), 'rotated-square-refresh')
            self.assertGreater(credential.refresh_after, fields.Datetime.now())

    def test_scheduled_renewal_retries_transient_failure_and_rotates_grant(self):
        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=self._square_response) as square:
            connection = self._authorize_connection()
            credential = connection._credential()
            saved_grant = (credential.access_token, credential.refresh_token)
            cron = self.env.ref('pos_square_terminal.ir_cron_square_terminal_renew_authorizations')
            now = fields.Datetime.now()
            credential.refresh_after = fields.Datetime.subtract(now, minutes=1)
            square.reset_mock()
            square.side_effect = [self._http_response({'errors': [{'code': 'SERVICE_UNAVAILABLE'}]}, 503)]
            cron.method_direct_trigger()
            self.env.invalidate_all()
            self._assert_refresh_request(square.call_args, connection)
            self.assertEqual((credential.access_token, credential.refresh_token), saved_grant)
            self.assertEqual(connection.authorization_result, 'connected')
            self.assertFalse(credential.reauthorization_required)
            self.assertGreaterEqual(credential.refresh_after, fields.Datetime.add(now, hours=1))
            cron.method_direct_trigger()
            self.assertEqual(square.call_count, 1)

            credential.refresh_after = now
            square.side_effect = [self._http_response(self._renewed_tokens())]
            cron.method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(square.call_count, 2)
            self._assert_refresh_request(square.call_args, connection)
            self.assertEqual(credential._unseal(credential.access_token), 'renewed-square-access')
            self.assertEqual(credential._unseal(credential.refresh_token), 'rotated-square-refresh')
            self.assertNotEqual(credential.refresh_token, 'rotated-square-refresh')
            self.assertGreater(credential.refresh_after, fields.Datetime.add(now, days=5))

    def test_scheduled_renewal_rejects_bad_grant_and_different_merchant(self):
        cron = self.env.ref('pos_square_terminal.ir_cron_square_terminal_renew_authorizations')
        for response in (
            self._http_response({'error': 'invalid_grant'}, 400),
            self._http_response(self._renewed_tokens(merchant_id='DIFFERENT-MERCHANT')),
        ):
            with self.subTest(status=response.status_code), patch(
                'odoo.addons.pos_square_terminal.square_api.request', side_effect=self._square_response,
            ) as square:
                connection = self._authorize_connection()
                credential = connection._credential()
                saved_grant = (credential.access_token, credential.refresh_token)
                credential.refresh_after = fields.Datetime.subtract(fields.Datetime.now(), minutes=1)
                square.reset_mock()
                square.side_effect = [response]
                cron.method_direct_trigger()
                self.env.invalidate_all()
                self._assert_refresh_request(square.call_args, connection)
                self.assertTrue(credential.reauthorization_required)
                self.assertEqual(connection.authorization_result, 'revoked')
                self.assertEqual(connection.merchant_id, 'SQUAREMERCHANT')
                self.assertEqual((credential.access_token, credential.refresh_token), saved_grant)
                cron.method_direct_trigger()
                self.assertEqual(square.call_count, 1)

    def _assert_refresh_request(self, request, connection):
        self.assertEqual(request.args, ('POST', 'https://connect.squareup.com/oauth2/token'))
        self.assertEqual(request.kwargs['json'], {
            'grant_type': 'refresh_token', 'client_id': connection.application_id,
            'client_secret': 'square-application-secret', 'refresh_token': 'square-refresh-token',
        })

    @staticmethod
    def _renewed_tokens(merchant_id='SQUAREMERCHANT'):
        return {
            'access_token': 'renewed-square-access', 'refresh_token': 'rotated-square-refresh',
            'merchant_id': merchant_id, 'token_type': 'bearer', 'expires_at': '2099-04-17T10:00:00Z',
        }

    def _register_payment_methods(self, config):
        return {
            record['id']: record
            for record in config.current_session_id.load_data(['pos.payment.method'])['pos.payment.method']
        }

    def _prepare_terminal(self, connection):
        location = self.env['pos.square.location'].search([('connection_id', '=', connection.id)])
        self.assertEqual(location.square_id, 'SQUARELOC')
        self.assertEqual(location.currency_code, 'USD')
        with Form(self.env['pos.square.terminal'].with_user(self.admin)) as terminal_form:
            terminal_form.name = 'Counter'
            terminal_form.connection_id = connection
            terminal_form.location_id = location
        return terminal_form.record

    def _create_payment_method(self, terminal):
        self.assertTrue(self.bank_journal)
        with Form(self.env['pos.payment.method'].with_user(self.admin)) as payment_form:
            payment_form.name = 'Square Counter'
            payment_form.journal_id = self.bank_journal
            payment_form.payment_method_type = 'terminal'
            payment_form.use_payment_terminal = 'square_terminal'
            payment_form.square_terminal_id = terminal
        return payment_form.record

    def _authorize_connection(self):
        with Form(self.env['pos.square.connection'].with_user(self.admin)) as connection_form:
            connection_form.name = 'Store Square account'
            connection_form.environment = 'production'
            connection_form.application_id = 'sq0idp-application'
            connection_form.new_application_secret = 'square-application-secret'
        connection = connection_form.record
        self.assertFalse(connection.new_application_secret)
        url = urlsplit(self._button(connection, 'action_authorize')['url'])
        self.assertEqual(url.netloc, 'connect.squareup.com')
        query = parse_qs(url.query)
        self.assertEqual(query['redirect_uri'], ['https://register.example.test/pos/square/terminal/oauth/callback'])
        self.assertIn('DEVICE_CREDENTIAL_MANAGEMENT', query['scope'][0].split())
        params = urlencode({'state': query['state'][0], 'code': 'test-auth-code'})
        response = self.url_open(f'/pos/square/terminal/oauth/callback?{params}', allow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertNotIn('test-auth-code', response.headers['Location'])
        self.assertEqual(self.url_open(f'/pos/square/terminal/oauth/callback?{params}', allow_redirects=False).status_code, 400)
        self.env.invalidate_all()
        self.assertEqual(connection.merchant_id, 'SQUAREMERCHANT')
        self.assertEqual(connection.authorization_result, 'connected')
        return connection

    def _button(self, record, method_name):
        response = self.url_open('/web/dataset/call_button', data=json.dumps({
            'jsonrpc': '2.0',
            'method': 'call',
            'params': {
                'model': record._name, 'method': method_name, 'args': [record.ids], 'kwargs': {},
            },
        }), headers={'Content-Type': 'application/json'}).json()
        self.assertNotIn('error', response, response)
        self.env.invalidate_all()
        return response['result']

    def _square_response(self, method, url, *, application_version='6.77', **kwargs):
        location_id = 'SQUARELOC'
        pairing = {'id': 'PAIRING', 'location_id': location_id, 'product_type': 'TERMINAL_API'}
        base_url = 'https://connect.squareup.com'
        self.assertTrue(url.startswith(base_url))
        if (method, url) == ('POST', base_url + '/oauth2/token'):
            self.assertEqual(kwargs['json']['client_secret'], 'square-application-secret')
            payload = {
                'access_token': 'square-access-token', 'refresh_token': 'square-refresh-token',
                'merchant_id': 'SQUAREMERCHANT', 'token_type': 'bearer', 'expires_at': '2099-03-17T10:00:00Z',
            }
        else:
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
            if (method, url) == ('GET', base_url + '/v2/locations'):
                payload = {'locations': [{
                    'id': location_id, 'merchant_id': 'SQUAREMERCHANT', 'name': 'Main Street',
                    'country': 'US', 'currency': 'USD', 'status': 'ACTIVE',
                    'capabilities': ['CREDIT_CARD_PROCESSING'],
                }]}
            elif (method, url) == ('POST', base_url + '/v2/devices/codes'):
                self.assertEqual(kwargs['json']['device_code'], {
                    'name': 'Counter', 'location_id': location_id, 'product_type': 'TERMINAL_API',
                })
                payload = {'device_code': {
                    **pairing, 'status': 'UNPAIRED', 'code': 'ABCDEF', 'pair_by': '2099-03-17T10:00:00Z',
                }}
            elif (method, url) == ('GET', base_url + '/v2/devices/codes/PAIRING'):
                payload = {'device_code': {**pairing, 'status': 'PAIRED', 'device_id': 'SQUARETERMINAL'}}
            elif (method, url) == ('GET', base_url + '/v2/devices/device%3ASQUARETERMINAL'):
                payload = {'device': {
                    'id': 'device:SQUARETERMINAL',
                    'attributes': {'type': 'TERMINAL', 'merchant_token': 'SQUAREMERCHANT'},
                    'status': {'category': 'AVAILABLE'},
                    'components': [{'type': 'APPLICATION', 'application_details': {
                        'application_type': 'TERMINAL_API', 'version': application_version,
                        'session_location': location_id, 'device_code_id': 'PAIRING',
                    }}],
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {url}')
        return self._http_response(payload)

    @staticmethod
    def _http_response(payload, status=200):
        response = Response()
        response.status_code = status
        response._content = json.dumps(payload).encode()
        return response

import hmac
import json
import os
from base64 import b64encode
from datetime import timedelta
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit

from cryptography.fernet import Fernet
from lxml import html
from requests import Response, Timeout

from odoo import Command, fields
from odoo.tests import HttpCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestSquareCheckout(HttpCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env.ref('website.default_website')
        cls.company = cls.website.company_id
        cls.provider = cls.env.ref('payment_square_online.payment_provider_square_online')
        cls.journal = cls.env['account.journal'].search([
            ('type', '=', 'bank'), ('company_id', '=', cls.company.id),
        ], limit=1)
        cls.currency = cls.env.ref('base.USD')
        cls.pricelist = cls.env['product.pricelist'].create({
            'name': 'Square US Online Store', 'currency_id': cls.currency.id,
            'website_id': cls.website.id, 'company_id': cls.company.id,
        })
        cls.customer = new_test_user(cls.env, login='square_customer', password='square_customer',
                                     groups='base.group_portal')
        cls.customer.partner_id.write({
            'name': 'Jamie Sample', 'email': 'square@example.test', 'phone': '2125550100',
            'street': '1 Main St', 'city': 'New York', 'zip': '10001',
            'country_id': cls.env.ref('base.us').id, 'state_id': cls.env.ref('base.state_us_27').id,
            'property_product_pricelist': cls.pricelist.id,
        })
        cls.tax = cls.env['account.tax'].create({
            'name': 'Square test sales tax', 'amount': 10, 'amount_type': 'percent',
            'type_tax_use': 'sale', 'company_id': cls.company.id,
        })
        cls.product = cls.env['product.product'].create({
            'name': 'Square test item', 'type': 'consu', 'list_price': 40,
            'website_published': True, 'taxes_id': [Command.set(cls.tax.ids)],
            **({'is_storable': True} if 'is_storable' in cls.env['product.product']._fields else {}),
        })
        shipping_product = cls.env['product.product'].create({
            'name': 'Square test shipping', 'type': 'service', 'taxes_id': [Command.clear()],
        })
        cls.carrier = cls.env['delivery.carrier'].create({
            'name': 'Square test standard delivery', 'delivery_type': 'fixed',
            'product_id': shipping_product.id, 'fixed_price': 6, 'website_published': True,
        })
        cls.env['ir.config_parameter'].set_param('sale.automatic_invoice', 'False')

    def setUp(self):
        super().setUp()
        key = patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()})
        key.start()
        self.addCleanup(key.stop)
        self.provider.write({
            'square_application_id': 'sandbox-sq0idb-checkout-test',
            'square_application_secret': 'sandbox-secret', 'website_id': self.website.id,
            'journal_id': self.journal.id,
        })
        connection = self.env['payment.square.connection'].search([('provider_id', '=', self.provider.id)])
        connection.write({
            'merchant_id': 'TESTMERCHANT', 'access_token': connection._seal('test-access-token'),
            'expires_at': fields.Datetime.now() + timedelta(days=1),
        })
        location = self.env['payment.square.location'].create({
            'provider_id': self.provider.id, 'square_id': 'TESTNYC',
            'name': 'New York', 'country_code': 'US', 'currency_code': 'USD', 'is_available': True,
        })
        self.provider.write({'square_location_id': location.id, 'state': 'test', 'is_published': True})
        self.authenticate(self.customer.login, self.customer.login)

    def _rpc(self, route, **params):
        return self.url_open(route, data=json.dumps({
            'jsonrpc': '2.0', 'method': 'call', 'params': params,
        }), headers={'Content-Type': 'application/json'}).json()

    def _result(self, route, **params):
        response = self._rpc(route, **params)
        self.assertNotIn('error', response, response.get('error', {}).get('data', {}).get('message'))
        self.env.invalidate_all()
        return response['result']

    def _cart(self):
        self._result('/shop/cart/update_json', product_id=self.product.id, add_qty=1)
        order = self.env['sale.order'].search([
            ('partner_id', '=', self.customer.partner_id.id), ('website_id', '=', self.website.id),
            ('state', '=', 'draft'),
        ], limit=1)
        self.assertTrue(order)
        order.set_delivery_line(self.carrier, 6)
        self.assertEqual(order.currency_id, self.currency)
        self.assertEqual(order.amount_total, 50)
        return order

    def _payment_page(self):
        response = self.url_open('/shop/payment')
        self.assertTrue('data-provider-code="square_online"' in response.text,
                        f'Square is not available on {response.url}')
        return response

    def _cart_signature(self):
        page = self._payment_page()
        return html.fromstring(page.content).xpath('//*[@name="o_square_card"]')[0].get(
            'data-square-cart-signature',
        )

    def _prepare(self, order):
        return self._result(
            f'/shop/payment/transaction/{order.id}',
            **self._prepare_params(order, square_cart_signature=self._cart_signature()),
        )

    def _prepare_params(self, order, **overrides):
        return {
            'access_token': order._portal_ensure_token(),
            'provider_id': self.provider.id,
            'payment_method_id': self.env.ref('payment.payment_method_card').id,
            'flow': 'direct',
            'token_id': None, 'tokenization_requested': False,
            'landing_route': '/shop/payment/validate',
            'amount': 50,
            **overrides,
        }

    @staticmethod
    def _square_response(method, url, *, status='COMPLETED', **kwargs):
        if method == 'POST' and url.endswith('/v2/orders'):
            order = kwargs['json']['order']
            data = {'order': {
                'id': 'SQ-ORDER-1', 'location_id': order['location_id'],
                'reference_id': order['reference_id'], 'state': 'OPEN',
                'total_money': {'amount': 5000, 'currency': 'USD'},
            }}
        elif method == 'POST' and url.endswith('/v2/payments'):
            payment = kwargs['json']
            data = {'payment': {
                'id': 'SQ-PAYMENT-1', 'status': status, 'source_type': 'CARD', 'location_id': payment['location_id'],
                'reference_id': payment['reference_id'], 'order_id': payment['order_id'],
                'amount_money': {'amount': 5000, 'currency': 'USD'},
            }}
        else:
            raise AssertionError(f'Unexpected Square API call: {method} {url}')
        response = Response()
        response.status_code = 200
        response._content = json.dumps(data).encode()
        return response

    def test_checkout_pays_the_existing_order_with_tax_and_shipping(self):
        order = self._cart()
        values = self._prepare(order)
        self.assertEqual(values['square_verification']['amount'], '50.00')
        self.assertEqual(values['square_verification']['currencyCode'], 'USD')
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response) as square:
            self._result('/payment/square/online/submit', **values['square_submission'], source_id='cnon:sdk-token')
            again = self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:sdk-token',
            )
            self.assertEqual(again['state'], 'done')
        self.assertEqual(square.call_count, 2)
        payment_request = square.call_args.kwargs['json']
        self.assertEqual(payment_request['amount_money'], {'amount': 5000, 'currency': 'USD'})
        self.assertEqual(payment_request['source_id'], 'cnon:sdk-token')
        self._result('/payment/status/poll')
        self.assertEqual(order.state, 'sale')
        self.assertFalse(order.invoice_ids)
        self.assertEqual(order.transaction_ids.state, 'done')
        self.assertEqual(order.transaction_ids.provider_reference, 'SQ-PAYMENT-1')
        self.assertEqual(order.transaction_ids.payment_id.amount, 50)
        self.assertEqual(order.transaction_ids.payment_id.journal_id, self.journal)
        if 'picking_ids' in order._fields:
            self.assertTrue(order.picking_ids)
            self.assertFalse(order.picking_ids.filtered(lambda picking: picking.state == 'done'))
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            order.with_context(disable_cancel_warning=True).action_cancel()
            self.assertEqual(order.state, 'cancel')
            self.assertEqual(order.transaction_ids.state, 'done')
            self.assertFalse(order.transaction_ids.child_transaction_ids)
            square.assert_not_called()

    def test_checkout_renews_an_expired_connection_before_charging(self):
        connection = self.env['payment.square.connection'].search([('provider_id', '=', self.provider.id)])
        connection.write({
            'refresh_token': connection._seal('test-refresh-token'),
            'expires_at': fields.Datetime.now() - timedelta(minutes=1),
        })
        self.provider.invalidate_recordset(['square_connection_status'])
        order = self._cart()
        values = self._prepare(order)
        requests_sent = []

        def square(method, url, **kwargs):
            self.assertTrue(url.startswith('https://connect.squareupsandbox.com/'))
            requests_sent.append(url.rsplit('/', 1)[-1])
            if url.endswith('/oauth2/token'):
                self.assertEqual(method, 'POST')
                self.assertEqual(kwargs['json'], {
                    'grant_type': 'refresh_token', 'client_id': 'sandbox-sq0idb-checkout-test',
                    'client_secret': 'sandbox-secret', 'refresh_token': 'test-refresh-token',
                })
                return self._oauth_response()
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer renewed-access-token')
            return self._square_response(method, url, **kwargs)

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square):
            result = self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:renewed',
            )
        self.assertEqual(result['state'], 'done')
        self.assertEqual(requests_sent, ['token', 'orders', 'payments'])
        self.assertEqual(self.provider.square_merchant_id, 'TESTMERCHANT')
        self.assertEqual(self.provider.square_connection_status, 'ready')
        self.assertEqual(self._result('/payment/status/poll')['state'], 'done')
        self.assertEqual(order.state, 'sale')
        self.assertEqual(order.transaction_ids.payment_id.amount, 50)

    @staticmethod
    def _oauth_response(merchant='TESTMERCHANT'):
        response = Response()
        response.status_code = 200
        response._content = json.dumps({
            'access_token': 'renewed-access-token', 'refresh_token': 'test-refresh-token',
            'merchant_id': merchant, 'token_type': 'bearer', 'expires_at': '2099-03-17T10:00:00Z',
        }).encode()
        return response

    def test_checkout_rejects_revoked_access_and_shows_reconnect_to_admin(self):
        order = self._cart()
        values = self._prepare(order)

        def square(method, url, **kwargs):
            if method == 'POST' and url.endswith('/v2/payments'):
                return self._square_error_response(401, 'ACCESS_TOKEN_REVOKED')
            return self._square_response(method, url, **kwargs)

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            result = self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:revoked',
            )
            self.assertEqual(result['state'], 'error')
            self.assertNotIn('square-error-detail', result['message'])
            self.assertNotIn('data-provider-code="square_online"', self.url_open('/shop/payment').text)
            self.assertEqual(api.call_count, 2)

        self.assertEqual(self.provider.square_connection_status, 'reconnect')
        self.assertEqual(self.provider.square_merchant_id, 'TESTMERCHANT')
        self.assertIn(order.state, ('draft', 'sent'))
        self.assertFalse(order.transaction_ids.payment_id)

    @staticmethod
    def _square_error_response(status, code):
        response = Response()
        response.status_code = status
        response._content = json.dumps({'errors': [{'code': code, 'detail': 'square-error-detail'}]}).encode()
        return response

    def test_declined_card_leaves_the_order_payable_again(self):
        order = self._cart()
        declined = self._prepare(order)

        def square_decline(method, url, **kwargs):
            if url.endswith('/v2/payments'):
                response = Response()
                response.status_code = 400
                response._content = b'{"errors":[{"code":"CARD_DECLINED","category":"PAYMENT_METHOD_ERROR"}]}'
                return response
            return self._square_response(method, url, **kwargs)

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_decline) as square:
            for _attempt in range(2):
                result = self._result(
                    '/payment/square/online/submit', **declined['square_submission'], source_id='cnon:declined',
                )
                self.assertEqual(result['state'], 'error')
                self.assertIn('declined', result['message'])
            self.assertEqual(square.call_count, 2)
        self.assertEqual(order.state, 'draft')
        self.assertFalse(order.transaction_ids.payment_id)

        prepared = self._prepare(order)
        self.assertNotEqual(prepared['reference'], declined['reference'])
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response):
            self._result('/payment/square/online/submit', **prepared['square_submission'], source_id='cnon:retry')
        self._result('/payment/status/poll')
        self.assertEqual(order.state, 'sale')
        self.assertEqual(len(order.transaction_ids.payment_id), 1)

    def test_lost_payment_response_blocks_square_and_other_providers(self):
        order = self._cart()
        values = self._prepare(order)
        second_tab = self._prepare(order)
        other_provider = self.env['payment.provider'].search([('code', '!=', 'square_online')], limit=1)

        def response_lost(method, url, **kwargs):
            if url.endswith('/v2/payments'):
                raise Timeout('Square response lost')
            return self._square_response(method, url, **kwargs)

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=response_lost) as square:
            result = self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:timeout',
            )
            self.assertEqual(result['state'], 'pending')
            self.assertEqual(self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:timeout',
            )['state'], 'pending')
            self.assertIn('error', self._rpc(
                '/payment/square/online/submit', **second_tab['square_submission'], source_id='cnon:other-tab',
            ))
            self.assertIn('already being processed', self._rpc(
                f'/shop/payment/transaction/{order.id}',
                **self._prepare_params(order, provider_id=other_provider.id, flow='redirect'),
            )['error']['data']['message'])
            self.assertEqual(square.call_count, 2)
        self.assertEqual(self._result('/payment/status/poll')['state'], 'pending')
        self.assertIn(order.state, ('draft', 'sent'))
        self.assertFalse(order.transaction_ids.payment_id)

    def test_refresh_recovers_a_timed_out_payment_from_a_later_square_page(self):
        order = self._cart()
        values = self._prepare(order)
        accepted = {}
        now_visible = False

        def square(method, url, **kwargs):
            if method == 'POST':
                response = self._square_response(method, url, **kwargs)
                if url.endswith('/v2/payments'):
                    accepted.update(response.json()['payment'])
                    raise Timeout('Square accepted the payment before the response was lost')
                return response
            self.assertEqual(method, 'GET')
            self.assertTrue(url.endswith('/v2/payments'))
            self.assertEqual(kwargs['params']['location_id'], 'TESTNYC')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer test-access-token')
            result = {'payments': [{**accepted, 'id': 'UNRELATED', 'reference_id': 'other-payment'}]}
            if now_visible:
                result = ({'payments': [accepted]} if kwargs['params'].get('cursor') == 'next-square-page'
                          else {**result, 'cursor': 'next-square-page'})
            response = Response()
            response.status_code = 200
            response._content = json.dumps(result).encode()
            return response

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            self.assertEqual(self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:timeout',
            )['state'], 'pending')
            tx = order.transaction_ids
            self.assertFalse(tx.provider_reference)
            manager = new_test_user(self.env, login='square_manager', password='square_manager',
                                    groups='account.group_account_manager')
            self.authenticate(manager.login, 'square_manager')

            def refresh():
                return self._result(
                    '/web/dataset/call_button/payment.transaction/action_square_refresh_status',
                    model='payment.transaction', method='action_square_refresh_status', args=[tx.ids], kwargs={},
                )

            refresh()
            self.assertEqual(tx.state, 'pending')
            self.assertFalse(tx.payment_id)
            replacement = self.env['payment.square.location'].create({
                'provider_id': self.provider.id, 'square_id': 'NEW_LOCATION', 'name': 'New location',
                'country_code': 'US', 'currency_code': 'USD', 'is_available': True,
            })
            self.provider.square_location_id = replacement
            now_visible = True
            refresh()

        self.assertEqual([call.args[0] for call in api.call_args_list], ['POST', 'POST', 'GET', 'GET', 'GET'])
        self.assertEqual(order.state, 'sale')
        self.assertEqual(tx.state, 'done')
        self.assertEqual(tx.provider_reference, 'SQ-PAYMENT-1')
        self.assertEqual(tx.square_location_ref, 'TESTNYC')
        self.assertEqual(tx.payment_id.amount, 50)
        self.assertEqual(tx.payment_id.payment_type, 'inbound')
        self.assertEqual(tx.payment_id.journal_id, self.journal)

    def test_reconnect_recovers_original_pending_payment_after_renewal_failure(self):
        order = self._cart()
        values = self._prepare(order)
        accepted = {}

        def initial_checkout(method, url, **kwargs):
            result = self._square_response(method, url, **kwargs)
            if url.endswith('/v2/payments'):
                accepted.update(result.json()['payment'])
                raise Timeout('Square accepted the payment before the response was lost')
            return result

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=initial_checkout):
            self.assertEqual(self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:timeout',
            )['state'], 'pending')

        tx = order.transaction_ids
        connection = self.env['payment.square.connection'].search([('provider_id', '=', self.provider.id)])
        connection.write({
            'refresh_token': connection._seal('test-refresh-token'),
            'expires_at': fields.Datetime.now() - timedelta(minutes=1),
        })
        self.env['ir.config_parameter'].set_param('web.base.url', 'https://store.example.test')
        self.authenticate('admin', 'admin')

        def refresh():
            self._result(
                '/web/dataset/call_button/payment.transaction/action_square_refresh_status',
                model='payment.transaction', method='action_square_refresh_status', args=[tx.ids], kwargs={},
            )

        with patch('odoo.addons.payment_square_online.square_api.request', return_value=self._square_error_response(
            400, 'INVALID_GRANT',
        )) as api:
            refresh()
            self.assertEqual([call.args for call in api.call_args_list], [
                ('POST', 'https://connect.squareupsandbox.com/oauth2/token'),
            ])
        self.assertEqual(tx.state, 'pending')
        self.assertFalse(tx.payment_id)
        self.assertIn('Reconnect the original Square account', tx.square_recovery_message)
        self.assertEqual(self.provider.square_connection_status, 'reconnect')

        def reconnect(code):
            result = self._result(
                '/web/dataset/call_button/payment.provider/action_square_authorize', model='payment.provider',
                method='action_square_authorize', args=[self.provider.ids], kwargs={},
            )
            state = parse_qs(urlsplit(result['url']).query)['state'][0]
            query = urlencode({'state': state, 'code': code})
            callback = self.url_open(f'/payment/square/online/oauth/callback?{query}', allow_redirects=False)
            self.assertEqual(callback.status_code, 303)
            self.env.invalidate_all()

        with patch('odoo.addons.payment_square_online.square_api.request', return_value=self._oauth_response(
            merchant='OTHER_ACCOUNT',
        )) as api:
            reconnect('different-merchant')
            self.assertEqual(api.call_count, 1)
        self.assertEqual(self.provider.square_merchant_id, 'TESTMERCHANT')
        self.assertEqual(self.provider.square_authorization_result, 'merchant_mismatch')
        self.assertEqual(self.provider.square_connection_status, 'reconnect')
        with patch('odoo.addons.payment_square_online.square_api.request') as api:
            refresh()
            api.assert_not_called()

        def reconnected_square(method, url, **kwargs):
            if method == 'POST' and url.endswith('/oauth2/token'):
                self.assertEqual(kwargs['json']['grant_type'], 'authorization_code')
                return self._oauth_response()
            self.assertEqual(method, 'GET')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer renewed-access-token')
            response = Response()
            response.status_code = 200
            if url.endswith('/v2/locations'):
                data = {'locations': [{
                    'id': 'NEW_LOCATION', 'name': 'New location', 'merchant_id': 'TESTMERCHANT',
                    'status': 'ACTIVE', 'country': 'US', 'currency': 'USD',
                    'capabilities': ['CREDIT_CARD_PROCESSING'],
                }]}
            else:
                self.assertTrue(url.endswith('/v2/payments'))
                self.assertEqual(kwargs['params']['location_id'], 'TESTNYC')
                data = {'payments': [accepted]}
            response._content = json.dumps(data).encode()
            return response

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=reconnected_square):
            reconnect('original-merchant')
            self.assertEqual(self.provider.square_authorization_result, 'connected')
            replacement = self.env['payment.square.location'].search([
                ('provider_id', '=', self.provider.id), ('square_id', '=', 'NEW_LOCATION'),
            ])
            self.provider.square_location_id = replacement
            refresh()
        self.assertEqual(self.provider.square_connection_status, 'ready')
        self.assertEqual(tx.state, 'done')
        self.assertEqual(tx.square_location_ref, 'TESTNYC')
        self.assertEqual(tx.provider_reference, accepted['id'])
        self.assertFalse(tx.square_recovery_message)
        self.assertEqual(order.state, 'sale')
        self.assertEqual(tx.payment_id.amount, 50)
        self.assertEqual(tx.payment_id.journal_id, self.journal)

    def test_signed_webhook_records_late_success_without_reopening_the_cancelled_order(self):
        order = self._cart()
        values = self._prepare(order)
        path = f'/payment/square/online/webhook/{self.provider.id}'
        subscription_url = 'https://checkout.example.test' + path
        signature_key = b'test Square subscription signature key'
        self.provider.write({
            'square_webhook_url': subscription_url,
            'square_webhook_signature_key': signature_key.decode(),
        })
        self.assertFalse(self.provider.square_webhook_signature_key)
        accepted = {}

        def square(method, url, **kwargs):
            if method == 'POST':
                response = self._square_response(method, url, **kwargs)
                if url.endswith('/v2/payments'):
                    accepted.update(response.json()['payment'])
                    raise Timeout('Square accepted the payment before the response was lost')
                return response
            self.assertEqual(method, 'GET')
            self.assertTrue(url.endswith('/v2/payments/SQ-PAYMENT-1'))
            response = Response()
            response.status_code = 200
            response._content = json.dumps({'payment': accepted}).encode()
            return response

        def deliver(event_id, status, *, merchant='TESTMERCHANT', sign_url=subscription_url):
            payload = json.dumps({
                'event_id': event_id, 'merchant_id': merchant, 'type': 'payment.updated',
                'data': {'id': 'SQ-PAYMENT-1', 'type': 'payment', 'object': {'payment': {**accepted, 'status': status}}},
            }).encode()
            signature = b64encode(hmac.digest(signature_key, sign_url.encode() + payload, 'sha256')).decode()
            return self.url_open(path, data=payload, headers={
                'Content-Type': 'application/json', 'x-square-hmacsha256-signature': signature,
            })

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            self.assertEqual(self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:late-success',
            )['state'], 'pending')
            tx = order.transaction_ids
            self.authenticate('admin', 'admin')
            self._result('/web/dataset/call_button/sale.order/action_cancel', model='sale.order',
                         method='action_cancel', args=[order.ids], kwargs={'context': {'disable_cancel_warning': True}})
            self.assertEqual(order.state, 'cancel')

            self.assertEqual(deliver('forged', 'COMPLETED', sign_url='https://wrong.example.test' + path).status_code, 403)
            self.assertEqual(deliver('wrong-merchant', 'COMPLETED', merchant='UNRELATED').status_code, 403)
            cron_id = self.env.ref('payment_square_online.ir_cron_square_online_webhooks').id

            def process_inbox():
                self._result('/web/dataset/call_button/ir.cron/method_direct_trigger', model='ir.cron',
                             method='method_direct_trigger', args=[[cron_id]], kwargs={})

            accepted['location_id'] = 'OTHER_LOCATION'
            self.assertEqual(deliver('mismatched-location', 'COMPLETED').status_code, 204)
            process_inbox()
            self.assertEqual(tx.state, 'pending')
            self.assertFalse(tx.payment_id)
            accepted['location_id'] = 'TESTNYC'
            self.assertEqual(deliver('verified', 'COMPLETED').status_code, 204)
            self.assertEqual(deliver('verified', 'COMPLETED').status_code, 204)
            process_inbox()
            first_payment = tx.payment_id
            self.assertEqual(first_payment.amount, 50)
            self.assertEqual(first_payment.payment_type, 'inbound')
            self.assertEqual(tx.state, 'done')
            self.assertEqual(order.state, 'cancel')
            self.assertFalse(order.invoice_ids)

            # Even a different event ID and a stale GET response cannot undo completion.
            accepted['status'] = 'PENDING'
            self.assertEqual(deliver('older-update', 'PENDING').status_code, 204)
            process_inbox()

        read = self._result('/web/dataset/call_kw/payment.transaction/read', model='payment.transaction',
                            method='read', args=[tx.ids, ['state', 'square_paid_cancelled_order']], kwargs={})
        self.assertEqual(read, [{'id': tx.id, 'state': 'done', 'square_paid_cancelled_order': True}])
        self.assertEqual(order.state, 'cancel')
        self.assertEqual(tx.payment_id, first_payment)
        self.assertFalse(tx.child_transaction_ids)
        self.assertEqual(self.env['account.payment'].search_count([('payment_transaction_id', '=', tx.id)]), 1)
        self.assertEqual([call.args[0] for call in api.call_args_list], ['POST', 'POST', 'GET', 'GET', 'GET'])

        self.authenticate(self.customer.login, self.customer.login)
        portal = self.url_open(f'/my/orders/{order.id}')
        self.assertEqual(portal.status_code, 200)
        self.assertIn('Square collected your payment. Please contact us about this order.', portal.text)

    def test_automatic_recovery_posts_the_order_invoice_and_payment_after_checkout_ends(self):
        order = self._cart()
        values = self._prepare(order)
        self.env['ir.config_parameter'].set_param('sale.automatic_invoice', 'True')
        accepted = {}

        def square(method, url, **kwargs):
            if method == 'POST':
                response = self._square_response(method, url, status='APPROVED', **kwargs)
                if url.endswith('/v2/payments'):
                    accepted.update(response.json()['payment'])
                return response
            self.assertEqual(method, 'GET')
            self.assertTrue(url.endswith('/v2/payments/SQ-PAYMENT-1'))
            response = Response()
            response.status_code = 200
            response._content = json.dumps({'payment': {**accepted, 'status': 'COMPLETED'}}).encode()
            return response

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            self.assertEqual(self._result(
                '/payment/square/online/submit', **values['square_submission'], source_id='cnon:approved',
            )['state'], 'pending')
            self.assertIn('Square is processing your payment', self.url_open('/payment/status').text)
            tx = order.transaction_ids
            self.assertEqual(tx.provider_reference, 'SQ-PAYMENT-1')
            self.assertFalse(tx.payment_id)
            self.assertFalse(order.invoice_ids)
            self.authenticate('admin', 'admin')
            cron_id = self.env.ref('payment_square_online.ir_cron_square_online_reconcile').id
            for _ in range(2):
                self._result('/web/dataset/call_button/ir.cron/method_direct_trigger', model='ir.cron',
                             method='method_direct_trigger', args=[[cron_id]], kwargs={})
            self.authenticate(self.customer.login, self.customer.login)
            portal = self.url_open(f'/my/orders/{order.id}')
            self.assertEqual(portal.status_code, 200)
            self.assertTrue(html.fromstring(portal.content).xpath(
                '//*[@name="o_payment_status_alert"][contains(@class, "alert-success")]',
            ))

        self.assertEqual([call.args[0] for call in api.call_args_list], ['POST', 'POST', 'GET'])
        self.assertEqual(order.state, 'sale')
        self.assertEqual(len(order.invoice_ids), 1)
        self.assertEqual(order.invoice_ids.state, 'posted')
        self.assertEqual(order.invoice_ids.amount_residual, 0)
        self.assertEqual(tx.payment_id.amount, 50)
        self.assertEqual(tx.payment_id.payment_type, 'inbound')
        self.assertFalse(tx.child_transaction_ids)

    def test_payment_history_and_refresh_reject_another_customer_or_company(self):
        order = self._cart()
        values = self._prepare(order)
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response):
            self._result('/payment/square/online/submit', **values['square_submission'], source_id='cnon:sdk-token')
        tx = order.transaction_ids
        other_company = self.env['res.company'].create({'name': 'Different Square Company'})
        outsiders = (
            new_test_user(self.env, login='other_square_customer', password='password', groups='base.group_portal'),
            new_test_user(self.env, login='other_square_manager', password='password', groups='account.group_account_manager',
                          company_id=other_company.id, company_ids=[Command.set(other_company.ids)]),
        )
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            for user in outsiders:
                with self.subTest(user=user.login):
                    self.authenticate(user.login, 'password')
                    read = self._rpc('/web/dataset/call_kw/payment.transaction/read', model='payment.transaction',
                                     method='read', args=[tx.ids, ['state', 'provider_reference']], kwargs={})
                    refresh = self._rpc('/web/dataset/call_button/payment.transaction/action_square_refresh_status',
                                        model='payment.transaction', method='action_square_refresh_status',
                                        args=[tx.ids], kwargs={})
                    for response in (read, refresh):
                        self.assertEqual(response['error']['data']['name'], 'odoo.exceptions.AccessError')
            square.assert_not_called()

    def test_an_expired_reservation_before_dispatch_releases_the_order_without_a_charge(self):
        order = self._cart()
        self._prepare(order)
        abandoned = order.transaction_ids
        # A checkout worker reserved the order and stopped before committing dispatch.
        abandoned._square_claim_checkout(order)
        abandoned.square_reserved_at = fields.Datetime.now() - timedelta(minutes=6)

        self.authenticate('admin', 'admin')
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            cron_id = self.env.ref('payment_square_online.ir_cron_square_online_reconcile').id
            self._result('/web/dataset/call_button/ir.cron/method_direct_trigger', model='ir.cron',
                         method='method_direct_trigger', args=[[cron_id]], kwargs={})
            square.assert_not_called()
        self.assertEqual(abandoned.state, 'cancel')
        self.assertFalse(abandoned.payment_id)
        self.assertIn('before it was sent', abandoned.state_message)

        self.authenticate(self.customer.login, self.customer.login, session_extra={'sale_order_id': order.id})
        retry = self._prepare(order)
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response) as square:
            self.assertEqual(self._result(
                '/payment/square/online/submit', **retry['square_submission'], source_id='cnon:new-card',
            )['state'], 'done')
            self._result('/payment/status/poll')
        self.assertEqual(square.call_count, 2)
        self.assertEqual(order.state, 'sale')
        self.assertEqual(len(order.transaction_ids.payment_id), 1)

    def test_checkout_rejects_tampered_attempt_and_stale_cart_without_contacting_square(self):
        order = self._cart()
        cart_signature = self._cart_signature()
        params = self._prepare_params(order, square_cart_signature=cart_signature)
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            for tampering in (
                {'amount': 1}, {'access_token': 'wrong-order-token'}, {'landing_route': '/untrusted', 'flow': 'redirect'},
            ):
                self.assertIn('error', self._rpc(f'/shop/payment/transaction/{order.id}', **(params | tampering)))
            valid = self._prepare(order)
            self.assertIn('error', self._rpc(
                '/payment/square/online/submit', **(valid['square_submission'] | {'submission_token': 'unknown'}),
                source_id='cnon:bad-attempt',
            ))
            self.assertIn('error', self._rpc('/payment/square/online/submit', **valid['square_submission'], source_id='CASH'))
            line = order.order_line.filtered(lambda line: not line.is_delivery)
            line.name += ' (updated)'
            self.assertIn('cart has changed', self._rpc(
                f'/shop/payment/transaction/{order.id}', **params,
            )['error']['data']['message'])
            self.assertIn('Checkout has changed', self._rpc(
                '/payment/square/online/submit', **valid['square_submission'], source_id='cnon:stale',
            )['error']['data']['message'])
            square.assert_not_called()
        self.assertEqual(order.state, 'draft')
        self.assertFalse(order.transaction_ids.payment_id)

    def test_original_journal_and_native_invoice_policy_survive_a_configuration_change(self):
        order = self._cart()
        self.env['ir.config_parameter'].set_param('sale.automatic_invoice', 'True')
        self.env['ir.config_parameter'].set_param('sale.async_emails', 'True')
        values = self._prepare(order)
        original_journal = self.journal
        new_journal = original_journal.copy({'name': 'Another bank', 'code': 'SQNEW'})
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response):
            self._result('/payment/square/online/submit', **values['square_submission'], source_id='cnon:invoice')
        self.provider.journal_id = new_journal
        self._result('/payment/status/poll')
        self.assertEqual(order.state, 'sale')
        self.assertEqual(len(order.invoice_ids), 1)
        self.assertEqual(order.invoice_ids.state, 'posted')
        self.assertEqual(order.invoice_ids.amount_residual, 0)
        self.assertEqual(order.transaction_ids.payment_id.journal_id, original_journal)
        self.assertEqual(order.transaction_ids.payment_id.amount, 50)

    def fetch_proxy(self, url):
        if url != 'https://sandbox.web.squarecdn.com/v1/square.js':
            return super().fetch_proxy(url)
        # Simulate the SDK, including a challenge that must remain usable while tokenization waits.
        return self.make_fetch_proxy_response("""
            window.Square = {payments: (app, location) => {
                if (app !== 'sandbox-sq0idb-checkout-test' || location !== 'TESTNYC') {
                    throw new Error('Wrong Square application or location');
                }
                return {card: async () => ({
                    attach: async (selector) => {
                        document.querySelector(selector).innerHTML =
                            '<iframe title="Simulated Secure Square Card Form"></iframe>';
                    },
                    tokenize: (details) => {
                        if (details.intent !== 'CHARGE' || details.amount !== '50.00'
                            || details.currencyCode !== 'USD' || details.sellerKeyedIn !== false
                            || details.customerInitiated !== true || details.billingContact.postalCode !== '10001') {
                            throw new Error('Incomplete verification details');
                        }
                        return new Promise(resolve => {
                            const challenge = document.createElement('div');
                            challenge.id = 'simulated-square-challenge';
                            Object.assign(challenge.style, {
                                position: 'fixed', top: '100px', left: '20px', zIndex: '6000',
                            });
                            for (const [action, token] of [['cancel', null], ['verify', 'cnon:browser-verified']]) {
                                const button = document.createElement('button');
                                button.textContent = action;
                                button.id = 'simulated-square-' + action;
                                button.addEventListener('click', () => {
                                    challenge.remove();
                                    resolve(token ? {status: 'OK', token} : {status: 'Cancel'});
                                });
                                challenge.append(button);
                            }
                            document.body.append(challenge);
                        });
                    },
                })};
            }};
        """)

    def test_mobile_browser_can_cancel_and_complete_square_authentication(self):
        order = self._cart()
        self.browser_size = '390x844'
        self.touch_enabled = True
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response) as square:
            self.browser_js('/shop', """
                (async () => {
                    const waitFor = async (condition) => {
                        for (let trial = 0; trial < 200; trial++) {
                            const value = condition();
                            if (value) {
                                return value;
                            }
                            await new Promise(resolve => setTimeout(resolve, 50));
                        }
                        throw new Error(`Expected checkout element did not appear: ${frame.contentDocument.body.innerText}`);
                    };
                    const frame = document.createElement('iframe');
                    frame.src = '/shop/payment';
                    Object.assign(frame.style, {
                        position: 'fixed', inset: '0', width: '100%', height: '100%', zIndex: '20000',
                    });
                    document.body.append(frame);
                    const find = (selector) => frame.contentDocument.querySelector(selector);
                    await waitFor(() => find('iframe[title="Simulated Secure Square Card Form"]'));
                    const payButton = await waitFor(() => find('button[name="o_payment_submit_button"]:not(:disabled)'));
                    payButton.click();
                    const cancel = await waitFor(() => find('#simulated-square-cancel'));
                    const bounds = cancel.getBoundingClientRect();
                    if (frame.contentDocument.elementFromPoint(bounds.left + 1, bounds.top + 1) !== cancel) {
                        throw new Error('The verification controls are blocked');
                    }
                    cancel.click();
                    const getErrorDialog = () => [...frame.contentDocument.querySelectorAll('[role="dialog"]')]
                        .find(dialog => dialog.textContent.includes('verify your card'));
                    const errorDialog = await waitFor(getErrorDialog);
                    errorDialog.querySelector('.modal-footer button').click();
                    await waitFor(() => !getErrorDialog());
                    payButton.click();
                    const verify = await waitFor(() => find('#simulated-square-verify'));
                    verify.click();
                    await waitFor(() => frame.contentWindow.location.pathname === '/shop/confirmation');
                    console.log('test successful');
                })().catch((error) => console.error(error));
            """, login=self.customer.login, timeout=35)
        self.env.invalidate_all()
        self.assertEqual(square.call_count, 2)
        self.assertEqual(square.call_args.kwargs['json']['source_id'], 'cnon:browser-verified')
        self.assertEqual(order.state, 'sale')
        self.assertEqual(order.transaction_ids.payment_id.amount, 50)

    def test_staff_cannot_forge_a_square_result_or_remove_the_reservation_using_rpc(self):
        order = self._cart()
        values = self._prepare(order)
        tx_id = values['square_submission']['transaction_id']
        operator = new_test_user(
            self.env, login='square_operator', password='square_operator',
            groups='account.group_account_invoice,sales_team.group_sale_salesman_all_leads',
        )
        self.authenticate(operator.login, operator.login)

        def write(model, record, updates):
            return self._rpc(
                f'/web/dataset/call_kw/{model}/write', model=model, method='write',
                args=[[record], updates], kwargs={},
            )

        self.assertIn('Square payment records can only be updated', write(
            'payment.transaction', tx_id, {'state': 'done', 'provider_reference': 'fake-square-payment'},
        )['error']['data']['message'])
        self.assertIn('Square payment reservations cannot be edited', write(
            'sale.order', order.id, {'square_online_transaction_id': False},
        )['error']['data']['message'])
        self.assertIn('Square payment reservations cannot be edited', write(
            'sale.order', order.id, {'transaction_ids': [Command.clear()]},
        )['error']['data']['message'])
        self.env.invalidate_all()
        self.assertEqual(order.transaction_ids.state, 'draft')
        self.assertFalse(order.transaction_ids.provider_reference)

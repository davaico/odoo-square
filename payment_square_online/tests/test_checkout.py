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


class SquareCheckoutCase(HttpCase):
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
    def _square_response(method, url, *, status='COMPLETED', currency='USD', **kwargs):
        if method == 'POST' and url.endswith('/v2/orders'):
            order = kwargs['json']['order']
            data = {'order': {
                'id': 'SQ-ORDER-1', 'location_id': order['location_id'],
                'reference_id': order['reference_id'], 'state': 'OPEN',
                'total_money': {'amount': 5000, 'currency': currency},
            }}
        elif method == 'POST' and url.endswith('/v2/payments'):
            payment = kwargs['json']
            data = {'payment': {
                'id': 'SQ-PAYMENT-1', 'status': status, 'source_type': 'CARD', 'location_id': payment['location_id'],
                'reference_id': payment['reference_id'], 'order_id': payment['order_id'],
                'amount_money': {'amount': 5000, 'currency': currency},
            }}
        else:
            raise AssertionError(f'Unexpected Square API call: {method} {url}')
        response = Response()
        response.status_code = 200
        response._content = json.dumps(data).encode()
        return response


@tagged('post_install', '-at_install')
class TestSquareCheckout(SquareCheckoutCase):
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

    def _refund_button(self, model, method, record_id, **kwargs):
        return self._result('/web/dataset/call_button', model=model, method=method, args=[[record_id]], kwargs=kwargs)

    def _refund_wizard(self, payment, amount=None):
        action = self._refund_button('account.payment', 'action_refund_wizard', payment.id)
        self.assertEqual(action['res_model'], 'payment.refund.wizard')
        return self._result(
            '/web/dataset/call_kw', model=action['res_model'], method='create',
            args=[{'amount_to_refund': amount} if amount is not None else {}],
            kwargs={'context': {'active_id': payment.id}},
        )

    def _captured_order(self):
        order = self._cart()
        prepared = self._prepare(order)
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response):
            self._result('/payment/square/online/submit', **prepared['square_submission'], source_id='cnon:refund-source')
        self._result('/payment/status/poll')
        return order

    @staticmethod
    def _refund_response(body, status=200):
        response = Response()
        response.status_code = status
        response._content = json.dumps(body).encode()
        return response

    def _square_refund_webhook(self, refund, event_id, event_type='refund.updated', *, merchant='TESTMERCHANT'):
        from odoo.addons.payment_square_online.const import SQUARE_WEBHOOK_PATH

        path = f'{SQUARE_WEBHOOK_PATH}/{self.provider.id}'
        subscription_url = f'https://square-test.example.test{path}'
        key = 'test-refund-webhook-key'
        self.provider.write({'square_webhook_url': subscription_url, 'square_webhook_signature_key': key})
        payload = json.dumps({
            'event_id': event_id, 'merchant_id': merchant, 'type': event_type,
            'data': {'id': refund['id'], 'type': 'refund', 'object': {'refund': refund}},
        }).encode()
        signature = b64encode(hmac.digest(key.encode(), subscription_url.encode() + payload, 'sha256')).decode()
        return self.url_open(path, data=payload, headers={
            'Content-Type': 'application/json', 'x-square-hmacsha256-signature': signature,
        })

    def test_square_dashboard_full_refund_webhook_posts_one_outbound_payment(self):
        order = self._captured_order()
        tx = order.transaction_ids
        self.authenticate('admin', 'admin')
        cron = self.env.ref('payment_square_online.ir_cron_square_online_webhooks')
        external = {
            'id': 'SQ-DASHBOARD-REFUND', 'payment_id': 'SQ-PAYMENT-1', 'status': 'COMPLETED',
            'amount_money': {'amount': 5000, 'currency': 'USD'}, 'location_id': 'TESTNYC',
            'reason': 'Square Dashboard', 'order_id': 'SQ-RETURN-ORDER',
        }

        def square(method, url, **kwargs):
            self.assertEqual(method, 'GET')
            self.assertTrue(url.endswith('/v2/refunds/SQ-DASHBOARD-REFUND'))
            return self._refund_response({'refund': external})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            self.assertEqual(self._square_refund_webhook(external, 'refund-complete', merchant='OTHER').status_code, 403)
            self.assertEqual(self._square_refund_webhook(external, 'refund-complete').status_code, 204)
            self.assertEqual(self._square_refund_webhook(external, 'refund-complete').status_code, 204)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            child = tx.child_transaction_ids
            self.assertEqual(len(child), 1)
            self.assertEqual(child.state, 'done')
            outbound = child.payment_id

            external['status'] = 'PENDING'
            self.assertEqual(self._square_refund_webhook(external, 'refund-older', 'refund.created').status_code, 204)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)

        self.assertEqual([call.args[0] for call in api.call_args_list], ['GET', 'GET'])
        self.assertEqual(tx.child_transaction_ids, child)
        self.assertEqual(child.provider_reference, 'SQ-DASHBOARD-REFUND')
        self.assertEqual(child.source_transaction_id, tx)
        self.assertEqual(child.amount, -50)
        self.assertEqual(child.currency_id, self.currency)
        self.assertFalse(child.square_refund_requested_by_id)
        self.assertEqual(child.state, 'done')
        self.assertEqual(child.payment_id, outbound)
        self.assertEqual(outbound.payment_type, 'outbound')
        self.assertEqual(outbound.amount, 50)
        self.assertEqual(outbound.currency_id, self.currency)
        self.assertEqual(outbound.company_id, self.company)
        self.assertEqual(outbound.source_payment_id, tx.payment_id)
        self.assertEqual(tx.payment_id.amount_available_for_refund, 0)
        self.assertEqual(self.env['account.payment'].search_count([('source_payment_id', '=', tx.payment_id.id)]), 1)
        self.assertFalse(order.invoice_ids)
        if 'picking_ids' in order._fields:
            self.assertFalse(order.picking_ids.filtered(lambda picking: picking.picking_type_code == 'incoming'))

    def test_signed_refund_webhook_is_retried_after_seven_days(self):
        tx = self._captured_order().transaction_ids
        self.authenticate('admin', 'admin')
        cron = self.env.ref('payment_square_online.ir_cron_square_online_webhooks')
        external = {
            'id': 'SQ-OLDER-REFUND', 'payment_id': tx.provider_reference, 'status': 'COMPLETED',
            'amount_money': {'amount': 1250, 'currency': 'USD'}, 'location_id': 'TESTNYC',
        }
        self.assertEqual(self._square_refund_webhook(external, 'long-delayed-refund').status_code, 204)
        event = self.env['payment.square.webhook.event'].search([('event_id', '=', 'long-delayed-refund')])
        self.assertTrue(event)
        eight_days_ago = fields.Datetime.now() - timedelta(days=8)
        self.env.cr.execute('UPDATE payment_square_webhook_event SET create_date = %s WHERE id = %s',
                            (eight_days_ago, event.id))
        response = self._square_error_response(503, 'SERVICE_UNAVAILABLE')

        def square(method, url, **kwargs):
            self.assertEqual(method, 'GET')
            self.assertTrue(url.endswith('/v2/refunds/SQ-OLDER-REFUND'))
            return response

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square):
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            self.assertTrue(event.exists())
            self.assertFalse(event.processed_at)
            self.assertFalse(tx.child_transaction_ids)

            response = self._refund_response({'refund': external})
            event.next_check_at = fields.Datetime.now() - timedelta(seconds=1)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            self.assertTrue(event.processed_at)
            self.assertEqual(tx.child_transaction_ids.payment_id.amount, 12.5)
            self.assertEqual(tx.child_transaction_ids.payment_id.payment_type, 'outbound')

            event.processed_at = eight_days_ago
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            self.assertFalse(event.exists())
            self.assertEqual(len(tx.child_transaction_ids), 1)

    def test_square_refund_discovery_preserves_equal_amount_local_reservation(self):
        order = self._captured_order()
        charge = order.transaction_ids
        self.authenticate('admin', 'admin')
        wizard = self._refund_wizard(charge.payment_id, 20)
        external = {
            'id': 'SQ-EXTERNAL-PARTIAL', 'payment_id': 'SQ-PAYMENT-1', 'status': 'COMPLETED',
            'location_id': 'TESTNYC', 'amount_money': {'amount': 2000, 'currency': 'USD'},
        }
        local = {**external, 'id': 'SQ-LOCAL-PARTIAL', 'status': 'PENDING'}

        def square(method, url, **kwargs):
            if method == 'POST' and url.endswith('/v2/refunds'):
                local['reason'] = kwargs['json']['reason']
                raise Timeout('Square accepted the local refund before losing the response')
            self.assertEqual(method, 'GET')
            if url.endswith('/v2/payments/SQ-PAYMENT-1'):
                return self._refund_response({'payment': {
                    'id': 'SQ-PAYMENT-1', 'status': 'COMPLETED', 'source_type': 'CARD',
                    'location_id': 'TESTNYC', 'reference_id': charge.square_order_reference,
                    'order_id': 'SQ-ORDER-1', 'amount_money': {'amount': 5000, 'currency': 'USD'},
                    'refund_ids': [external['id']], 'refunded_money': {'amount': 2000, 'currency': 'USD'},
                }})
            for refund in (external, local):
                if url.endswith(f"/v2/refunds/{refund['id']}"):
                    return self._refund_response({'refund': refund})
            self.fail(f'Unexpected Square API call: {method} {url}')

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            local_id = self._refund_button('payment.refund.wizard', 'action_refund', wizard)['res_id']
            local_tx = charge.child_transaction_ids
            self.assertEqual(local_tx.id, local_id)
            self.assertFalse(local_tx.provider_reference)
            self.assertEqual(charge.payment_id.amount_available_for_refund, 30)

            self._refund_button('payment.transaction', 'action_square_refresh_status', charge.id)
            self.assertEqual(len(charge.child_transaction_ids), 2)
            external_tx = charge.child_transaction_ids - local_tx
            self.assertEqual(external_tx.provider_reference, 'SQ-EXTERNAL-PARTIAL')
            self.assertEqual(external_tx.square_refund_origin, 'square')
            self.assertFalse(external_tx.square_refund_requested_by_id)
            self.assertEqual((external_tx.state, external_tx.payment_id.amount), ('done', 20))
            self.assertEqual(local_tx.state, 'pending')
            self.assertFalse(local_tx.provider_reference)
            self.assertEqual(charge.payment_id.amount_available_for_refund, 10)

            cron = self.env.ref('payment_square_online.ir_cron_square_online_webhooks')
            self.assertEqual(self._square_refund_webhook(local, 'local-link').status_code, 204)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            self.assertEqual(local_tx.provider_reference, 'SQ-LOCAL-PARTIAL')
            self.assertEqual(local_tx.state, 'pending')
            self.assertEqual(charge.payment_id.amount_available_for_refund, 10)

            local['status'] = 'FAILED'
            self.assertEqual(self._square_refund_webhook(local, 'local-failure').status_code, 204)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)

        self.assertEqual([call.args[0] for call in api.call_args_list], ['POST', 'GET', 'GET', 'GET', 'GET'])
        self.assertEqual(len(charge.child_transaction_ids), 2)
        self.assertEqual(local_tx.state, 'error')
        self.assertEqual(local_tx.square_refund_origin, 'odoo')
        self.assertEqual(local_tx.square_refund_requested_by_id, self.env.ref('base.user_admin'))
        self.assertFalse(local_tx.payment_id)
        self.assertEqual(charge.payment_id.amount_available_for_refund, 30)
        self.assertEqual(self.env['account.payment'].search_count([('source_payment_id', '=', charge.payment_id.id)]), 1)

    def test_scheduled_square_refund_discovery_checks_settled_payments_without_webhooks(self):
        charge = self._captured_order().transaction_ids
        self.authenticate('admin', 'admin')
        new_location = self.env['payment.square.location'].create({
            'provider_id': self.provider.id, 'square_id': 'NEWLOCATION', 'name': 'New store',
            'country_code': 'US', 'currency_code': 'USD',
        })
        self.provider.square_location_id = new_location
        external = {
            'id': 'SQ-MISSING-WEBHOOK', 'payment_id': 'SQ-PAYMENT-1', 'status': 'PENDING',
            'location_id': 'TESTNYC', 'amount_money': {'amount': 1250, 'currency': 'USD'},
        }
        unrelated = {**external, 'id': 'POS-REFUND', 'payment_id': 'POS-PAYMENT'}
        wrong_location = {**external, 'id': 'UNRELATED-LOCATION', 'location_id': 'NEWLOCATION'}
        wrong_currency = {**external, 'id': 'WRONG-CURRENCY', 'amount_money': {'amount': 1250, 'currency': 'EUR'}}

        def square(method, url, **kwargs):
            self.assertEqual(method, 'GET')
            if url.endswith('/v2/refunds'):
                if kwargs['params'].get('cursor') == 'next-refund-page':
                    return self._refund_response({'refunds': [external]})
                return self._refund_response({
                    'refunds': [unrelated, wrong_location, wrong_currency], 'cursor': 'next-refund-page',
                })
            self.assertTrue(url.endswith('/v2/refunds/SQ-MISSING-WEBHOOK'))
            return self._refund_response({'refund': {**external, 'status': 'COMPLETED'}})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            cron = self.env.ref('payment_square_online.ir_cron_square_online_discover_refunds')
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            child = charge.child_transaction_ids
            self.assertEqual(len(child), 1)
            self.assertEqual(child.provider_reference, 'SQ-MISSING-WEBHOOK')
            self.assertEqual(child.state, 'pending')
            self.assertEqual(child.square_location_ref, 'TESTNYC')
            self.assertEqual(child.amount, -12.5)
            self.assertFalse(child.payment_id)
            self.assertEqual(charge.payment_id.amount_available_for_refund, 37.5)

            status_cron = self.env.ref('payment_square_online.ir_cron_square_online_reconcile')
            self._refund_button('ir.cron', 'method_direct_trigger', status_cron.id)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)

        self.assertEqual([call.args[0] for call in api.call_args_list], ['GET', 'GET', 'GET'])
        self.assertEqual(charge.child_transaction_ids, child)
        self.assertEqual(child.state, 'done')
        self.assertEqual(child.payment_id.amount, 12.5)
        self.assertEqual(child.payment_id.source_payment_id, charge.payment_id)
        self.assertEqual(charge.payment_id.amount_available_for_refund, 37.5)
        self.assertEqual(self.env['account.payment'].search_count([('source_payment_id', '=', charge.payment_id.id)]), 1)

    def test_payment_recovery_retries_a_refund_older_than_the_global_scan_overlap(self):
        earlier_charge = self._captured_order().transaction_ids
        earlier_charge.square_dispatched_at = fields.Datetime.now() - timedelta(days=3)
        order = self._cart()
        submission = self._prepare(order)['square_submission']
        refund_time = fields.Datetime.now() - timedelta(days=2)
        refund = {
            'id': 'SQ-OLDER-DASHBOARD-REFUND', 'payment_id': 'SQ-LATE-PAYMENT', 'status': 'COMPLETED',
            'location_id': 'TESTNYC', 'amount_money': {'amount': 1250, 'currency': 'USD'},
            'created_at': refund_time.isoformat() + 'Z',
        }
        accepted = {}
        payment_visible = False
        refund_available = False
        scan_begins = []

        def square(method, url, **kwargs):
            if method == 'POST':
                result = self._square_response(method, url, **kwargs)
                if url.endswith('/v2/payments'):
                    accepted.update(result.json()['payment'], id='SQ-LATE-PAYMENT', refund_ids=[refund['id']],
                                    refunded_money=refund['amount_money'])
                    raise Timeout('Square accepted the payment before losing the response')
                return result
            self.assertEqual(method, 'GET')
            if url.endswith('/v2/refunds'):
                begins = kwargs['params']['begin_time']
                scan_begins.append(begins)
                return self._refund_response({'refunds': [refund] if begins < refund['created_at'] else []})
            if url.endswith('/v2/payments'):
                return self._refund_response({'payments': [accepted] if payment_visible else []})
            if url.endswith('/v2/payments/SQ-LATE-PAYMENT'):
                return self._refund_response({'payment': accepted})
            self.assertTrue(url.endswith('/v2/refunds/SQ-OLDER-DASHBOARD-REFUND'))
            if not refund_available:
                return self._square_error_response(503, 'SERVICE_UNAVAILABLE')
            return self._refund_response({'refund': refund})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            self.assertEqual(self._result(
                '/payment/square/online/submit', **submission, source_id='cnon:late-source',
            )['state'], 'pending')
            charge = order.transaction_ids
            charge.square_dispatched_at = refund_time - timedelta(hours=1)
            self.authenticate('admin', 'admin')
            discovery = self.env.ref('payment_square_online.ir_cron_square_online_discover_refunds')
            recovery = self.env.ref('payment_square_online.ir_cron_square_online_reconcile')
            self._refund_button('ir.cron', 'method_direct_trigger', discovery.id)
            self.assertFalse(charge.provider_reference)
            self.assertFalse(charge.child_transaction_ids)
            self.assertLess(scan_begins[0], refund['created_at'])

            earlier_charge.square_connection_id.refund_scan_next_check = False
            self._refund_button('ir.cron', 'method_direct_trigger', discovery.id)
            self.assertGreater(scan_begins[1], refund['created_at'])

            payment_visible = True
            self._refund_button('ir.cron', 'method_direct_trigger', recovery.id)
            self.assertEqual(order.state, 'sale')
            self.assertEqual(charge.payment_id.amount, 50)
            self.assertFalse(charge.child_transaction_ids)
            self.assertEqual(charge.payment_id.amount_available_for_refund, 0)

            refund_available = True
            charge.square_recovery_next_check = False
            self._refund_button('ir.cron', 'method_direct_trigger', recovery.id)
            charge.square_recovery_next_check = False
            self._refund_button('ir.cron', 'method_direct_trigger', recovery.id)

        child = charge.child_transaction_ids
        self.assertEqual(child.provider_reference, refund['id'])
        self.assertEqual((child.state, child.payment_id.payment_type, child.payment_id.amount),
                         ('done', 'outbound', 12.5))
        self.assertEqual(child.payment_id.source_payment_id, charge.payment_id)
        self.assertEqual(charge.payment_id.amount_available_for_refund, 37.5)
        self.assertFalse(order.invoice_ids)
        self.assertEqual([call.args[0] for call in api.call_args_list],
                         ['POST', 'POST', 'GET', 'GET', 'GET', 'GET', 'GET', 'GET'])

    def test_refund_discovery_retains_a_capture_waiting_for_native_accounting(self):
        earlier_charge = self._captured_order().transaction_ids
        earlier_charge.square_dispatched_at = fields.Datetime.now() - timedelta(days=3)
        order = self._cart()
        submission = self._prepare(order)['square_submission']
        refund_time = fields.Datetime.now() - timedelta(days=2)
        refund = {
            'id': 'SQ-UNPOSTED-REFUND', 'payment_id': 'SQ-UNPOSTED-PAYMENT', 'status': 'COMPLETED',
            'location_id': 'TESTNYC', 'amount_money': {'amount': 1250, 'currency': 'USD'},
            'created_at': refund_time.isoformat() + 'Z',
        }
        accepted = {}
        scan_begins = []

        def square(method, url, **kwargs):
            if method == 'POST':
                response = self._square_response(method, url, **kwargs)
                if url.endswith('/v2/payments'):
                    accepted.update(response.json()['payment'], id='SQ-UNPOSTED-PAYMENT')
                    return self._refund_response({'payment': accepted})
                return response
            self.assertEqual(method, 'GET')
            if url.endswith('/v2/refunds'):
                begin = kwargs['params']['begin_time']
                scan_begins.append(begin)
                return self._refund_response({'refunds': [refund] if begin < refund['created_at'] else []})
            if url.endswith('/v2/payments/SQ-UNPOSTED-PAYMENT'):
                return self._refund_response({'payment': {
                    **accepted, 'refund_ids': [refund['id']], 'refunded_money': refund['amount_money'],
                }})
            self.assertTrue(url.endswith('/v2/refunds/SQ-UNPOSTED-REFUND'))
            return self._refund_response({'refund': refund})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            result = self._result('/payment/square/online/submit', **submission, source_id='cnon:unposted')
            self.assertEqual(result['state'], 'done')
            charge = order.transaction_ids
            charge.square_dispatched_at = refund_time - timedelta(hours=1)
            # The browser closed before status polling; native accounting has not run yet.
            self.assertFalse(charge.payment_id)
            self.assertFalse(charge.is_post_processed)
            self.authenticate('admin', 'admin')
            discovery = self.env.ref('payment_square_online.ir_cron_square_online_discover_refunds')
            recovery = self.env.ref('payment_square_online.ir_cron_square_online_reconcile')
            self._refund_button('ir.cron', 'method_direct_trigger', discovery.id)
            self.assertFalse(charge.child_transaction_ids)
            self.assertTrue(charge.square_linked_refunds_pending)
            self.assertLess(scan_begins[0], refund['created_at'])

            charge.square_connection_id.refund_scan_next_check = False
            self._refund_button('ir.cron', 'method_direct_trigger', discovery.id)
            self.assertGreater(scan_begins[1], refund['created_at'])
            self._refund_button('ir.cron', 'method_direct_trigger', recovery.id)
            charge.square_recovery_next_check = False
            self._refund_button('ir.cron', 'method_direct_trigger', recovery.id)

        self.assertEqual(order.state, 'sale')
        self.assertEqual(charge.payment_id.amount, 50)
        child = charge.child_transaction_ids
        self.assertEqual(child.provider_reference, refund['id'])
        self.assertEqual((child.state, child.payment_id.payment_type, child.payment_id.amount),
                         ('done', 'outbound', 12.5))
        self.assertEqual(child.payment_id.source_payment_id, charge.payment_id)
        self.assertEqual(charge.payment_id.amount_available_for_refund, 37.5)
        self.assertFalse(order.invoice_ids)
        self.assertEqual([call.args[0] for call in api.call_args_list],
                         ['POST', 'POST', 'GET', 'GET', 'GET', 'GET'])

    def test_square_amount_rejection_recovers_the_dashboard_refund_that_won_the_race(self):
        charge = self._captured_order().transaction_ids
        self.authenticate('admin', 'admin')
        wizard = self._refund_wizard(charge.payment_id, 40)
        second_wizard = self._refund_wizard(charge.payment_id, 40)
        visible_at_square = False
        external = {
            'id': 'SQ-DASHBOARD-FIRST', 'payment_id': 'SQ-PAYMENT-1', 'status': 'COMPLETED',
            'location_id': 'TESTNYC', 'amount_money': {'amount': 3000, 'currency': 'USD'},
        }

        def square(method, url, **kwargs):
            if method == 'POST' and url.endswith('/v2/refunds'):
                self.assertEqual(kwargs['json']['amount_money'], {'amount': 4000, 'currency': 'USD'})
                return self._square_error_response(400, 'REFUND_AMOUNT_INVALID')
            self.assertEqual(method, 'GET')
            if url.endswith('/v2/payments/SQ-PAYMENT-1'):
                return self._refund_response({'payment': {
                    'id': 'SQ-PAYMENT-1', 'status': 'COMPLETED', 'source_type': 'CARD', 'location_id': 'TESTNYC',
                    'reference_id': charge.square_order_reference, 'order_id': 'SQ-ORDER-1',
                    'amount_money': {'amount': 5000, 'currency': 'USD'},
                    'refund_ids': [external['id']] if visible_at_square else [],
                    'refunded_money': {'amount': 3000 if visible_at_square else 0, 'currency': 'USD'},
                }})
            self.assertTrue(url.endswith('/v2/refunds/SQ-DASHBOARD-FIRST'))
            return self._refund_response({'refund': external})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            local_id = self._refund_button('payment.refund.wizard', 'action_refund', wizard)['res_id']
            self.assertEqual(self._refund_button('payment.refund.wizard', 'action_refund', wizard)['res_id'], local_id)
            self.assertEqual(charge.payment_id.amount_available_for_refund, 0)
            blocked = self._rpc('/web/dataset/call_button', model='payment.refund.wizard', method='action_refund',
                                args=[[second_wizard]], kwargs={})
            self.assertIn('reported a lower refundable balance', blocked['error']['data']['message'])
            self.assertEqual(len(charge.child_transaction_ids), 1)
            visible_at_square = True
            self._refund_button('payment.transaction', 'action_square_refresh_status', charge.id)

        self.assertEqual([call.args[0] for call in api.call_args_list], ['POST', 'GET', 'GET', 'GET'])
        self.assertEqual(len(charge.child_transaction_ids), 2)
        local = charge.child_transaction_ids.filtered(lambda child: child.id == local_id)
        imported = charge.child_transaction_ids - local
        self.assertEqual(local.state, 'error')
        self.assertFalse(local.payment_id)
        self.assertEqual((imported.state, imported.payment_id.amount), ('done', 30))
        self.assertEqual(charge.payment_id.amount_available_for_refund, 20)
        self.assertEqual(self.env['account.payment'].search_count([('source_payment_id', '=', charge.payment_id.id)]), 1)

    def test_accounting_manager_issues_full_refund_in_native_payment_wizard(self):
        self.env['ir.config_parameter'].set_param('sale.automatic_invoice', 'True')
        order = self._captured_order()
        charge = order.transaction_ids
        original_payment = charge.payment_id
        original_credit_notes = self.env['account.move'].search([('move_type', '=', 'out_refund')])
        self.assertEqual(order.invoice_ids.state, 'posted')
        self.assertEqual(order.invoice_ids.amount_residual, 0)
        if 'picking_ids' in order._fields:
            self.assertTrue(order.picking_ids)
            self.assertFalse(order.picking_ids.filtered(lambda picking: picking.state == 'done'))
            pickings = {picking.id: picking.state for picking in self.env['stock.picking'].search([])}
        manager = new_test_user(self.env, login='square_refund_manager', password='square_refund_manager',
                                groups='account.group_account_manager', company_id=self.company.id)
        self.authenticate(manager.login, manager.login)
        wizard = self._refund_wizard(original_payment, 50)
        replacement_journal = self.env['account.journal'].create({
            'name': 'Replacement Square bank', 'code': 'SQNEW', 'type': 'bank', 'company_id': self.company.id,
        })
        replacement_location = self.env['payment.square.location'].create({
            'provider_id': self.provider.id, 'square_id': 'OTHER_LOCATION', 'name': 'Replacement location',
            'country_code': 'US', 'currency_code': 'USD',
        })
        self.provider.write({'journal_id': replacement_journal.id, 'square_location_id': replacement_location.id})

        def square_refund(method, url, **kwargs):
            self.assertEqual((method, url), ('POST', 'https://connect.squareupsandbox.com/v2/refunds'))
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer test-access-token')
            payment_refund = kwargs['json']
            self.assertEqual(payment_refund['payment_id'], 'SQ-PAYMENT-1')
            self.assertEqual(payment_refund['amount_money'], {'amount': 5000, 'currency': 'USD'})
            self.assertNotIn('location_id', payment_refund)
            self.assertTrue(payment_refund['idempotency_key'])
            self.assertTrue(payment_refund['reason'])
            return self._refund_response({'refund': {
                'id': 'SQ-REFUND-1', 'status': 'COMPLETED', 'location_id': 'TESTNYC',
                'payment_id': 'SQ-PAYMENT-1', 'amount_money': {'amount': 5000, 'currency': 'USD'},
                'reason': payment_refund['reason'],
            }})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_refund) as square:
            action = self._refund_button('payment.refund.wizard', 'action_refund', wizard)
            repeated = self._refund_button('payment.refund.wizard', 'action_refund', wizard)
            self.assertEqual(repeated['res_id'], action['res_id'])
            self.assertEqual(square.call_count, 1)

        refund = charge.child_transaction_ids
        self.assertEqual(refund.id, action['res_id'])
        self.assertEqual(refund.operation, 'refund')
        self.assertEqual(refund.state, 'done')
        self.assertEqual(refund.amount, -50)
        self.assertEqual(refund.provider_reference, 'SQ-REFUND-1')
        self.assertEqual(refund.square_refund_requested_by_id, manager)
        payment = refund.payment_id
        self.assertEqual(payment.payment_type, 'outbound')
        self.assertEqual(payment.source_payment_id, original_payment)
        self.assertEqual(payment.move_id.state, 'posted')
        self.assertEqual(payment.journal_id, original_payment.journal_id)
        self.assertEqual(payment.currency_id, self.currency)
        self.assertEqual(payment.amount, 50)
        self.assertEqual(original_payment.amount_available_for_refund, 0)
        self.assertEqual(order.state, 'sale')
        self.assertEqual(self.env['account.move'].search([('move_type', '=', 'out_refund')]), original_credit_notes)
        if 'picking_ids' in order._fields:
            self.assertEqual({picking.id: picking.state for picking in self.env['stock.picking'].search([])}, pickings)
        self.assertEqual(order.invoice_ids.amount_residual, 0)

    def test_refund_rejection_releases_the_balance_without_retrying_the_same_wizard(self):
        charge = self._captured_order().transaction_ids
        self.authenticate('admin', 'admin')
        wizard = self._refund_wizard(charge.payment_id, 50)
        rejection = self._refund_response({'errors': [{'code': 'PAYMENT_NOT_REFUNDABLE'}]}, status=400)
        with patch('odoo.addons.payment_square_online.square_api.request', return_value=rejection) as square:
            first = self._refund_button('payment.refund.wizard', 'action_refund', wizard)
            repeated = self._refund_button('payment.refund.wizard', 'action_refund', wizard)
            square.assert_called_once()
        self.assertEqual(first['res_id'], repeated['res_id'])
        refund = charge.child_transaction_ids
        self.assertEqual(refund.state, 'error')
        self.assertIn('Square rejected the refund', refund.state_message)
        self.assertEqual(refund.square_refund_requested_by_id, self.env.ref('base.user_admin'))
        self.assertEqual(charge.payment_id.amount_available_for_refund, 50)
        self.assertFalse(refund.payment_id)

    def test_accounting_manager_issues_successive_partial_refunds_and_the_remainder(self):
        self.env['ir.config_parameter'].set_param('sale.automatic_invoice', 'True')
        order = self._captured_order()
        charge = order.transaction_ids
        payment = charge.payment_id
        credit_notes = self.env['account.move'].search([('move_type', '=', 'out_refund')])
        if 'picking_ids' in order._fields:
            self.assertTrue(order.picking_ids)
            pickings = self.env['stock.picking'].search([]).read(['state'])
            moves = self.env['stock.move'].search([]).read(['state', 'quantity', 'origin_returned_move_id'])
        manager = new_test_user(self.env, login='square_partial_manager', password='square_partial_manager',
                                groups='account.group_account_manager', company_id=self.company.id)
        self.authenticate(manager.login, manager.login)

        def square_refund(method, url, **kwargs):
            self.assertEqual((method, url), ('POST', 'https://connect.squareupsandbox.com/v2/refunds'))
            request = kwargs['json']
            self.assertEqual(request['payment_id'], 'SQ-PAYMENT-1')
            return self._refund_response({'refund': {
                'id': request['idempotency_key'], 'status': 'COMPLETED', 'location_id': 'TESTNYC',
                'payment_id': 'SQ-PAYMENT-1', 'amount_money': request['amount_money'],
                'reason': request['reason'],
            }})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_refund) as square:
            refund_ids = []
            for amount, available, refunded in ((10.01, 50, 0), (10.01, 39.99, 10.01), (None, 29.98, 20.02)):
                wizard = self._refund_wizard(payment, amount)
                summary = self._result(
                    '/web/dataset/call_kw', model='payment.refund.wizard', method='read', args=[[wizard], [
                        'support_refund', 'amount_available_for_refund', 'refunded_amount', 'has_pending_refund',
                    ]], kwargs={},
                )
                self.assertEqual(summary, [{
                    'id': wizard, 'support_refund': 'partial', 'amount_available_for_refund': available,
                    'refunded_amount': refunded, 'has_pending_refund': False,
                }])
                action = self._refund_button('payment.refund.wizard', 'action_refund', wizard)
                self.assertEqual(self._refund_button('payment.refund.wizard', 'action_refund', wizard), action)
                refund_ids.append(action['res_id'])

        self.assertEqual([call.kwargs['json']['amount_money'] for call in square.call_args_list], [
            {'amount': 1001, 'currency': 'USD'}, {'amount': 1001, 'currency': 'USD'},
            {'amount': 2998, 'currency': 'USD'},
        ])
        refunds = self.env['payment.transaction'].browse(refund_ids)
        self.assertEqual(len(refunds), 3)
        self.assertEqual(refunds.mapped('state'), ['done', 'done', 'done'])
        self.assertEqual(refunds.mapped('amount'), [-10.01, -10.01, -29.98])
        self.assertEqual(len(set(refunds.mapped('provider_reference'))), 3)
        self.assertEqual(refunds.square_refund_requested_by_id, manager)
        self.assertEqual(refunds.source_transaction_id, charge)
        outbound = self.env['account.payment'].search([('source_payment_id', '=', payment.id)])
        self.assertEqual(outbound, refunds.payment_id.sorted('id', reverse=True))
        self.assertEqual(outbound.mapped('payment_type'), ['outbound', 'outbound', 'outbound'])
        self.assertEqual(outbound.move_id.mapped('state'), ['posted', 'posted', 'posted'])
        self.assertCountEqual(outbound.mapped('amount'), [10.01, 10.01, 29.98])
        self.assertEqual(sum(outbound.mapped('amount')), 50)
        self.assertEqual(payment.amount_available_for_refund, 0)
        self.assertEqual(order.invoice_ids.amount_residual, 0)
        self.assertEqual(self.env['account.move'].search([('move_type', '=', 'out_refund')]), credit_notes)
        if 'picking_ids' in order._fields:
            self.assertEqual(self.env['stock.picking'].search([]).read(['state']), pickings)
            self.assertEqual(self.env['stock.move'].search([]).read(['state', 'quantity', 'origin_returned_move_id']), moves)

    def test_refund_rejects_invalid_amounts_before_monetary_rounding(self):
        charge = self._captured_order().transaction_ids
        self.authenticate('admin', 'admin')
        payment = charge.payment_id
        wizard = self._refund_wizard(payment, 10)
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            for amount in (0, -1, 50.01, 1.001, 50.001):
                with self.subTest(amount=amount):
                    for model, method, args, kwargs in (
                        ('payment.refund.wizard', 'create',
                         [{'payment_id': payment.id, 'amount_to_refund': amount}], {}),
                        ('payment.refund.wizard', 'write', [[wizard], {'amount_to_refund': amount}], {}),
                        ('payment.transaction', 'action_refund', [charge.ids], {'amount_to_refund': amount}),
                    ):
                        response = self._rpc('/web/dataset/call_kw', model=model, method=method,
                                             args=args, kwargs=kwargs)
                        self.assertEqual(response.get('error', {}).get('data', {}).get('name'),
                                         'odoo.exceptions.ValidationError', response)
            response = self._rpc('/web/dataset/call_kw', model='payment.refund.wizard', method='create',
                                 args=[{}], kwargs={'context': {
                                     'active_id': payment.id, 'default_amount_to_refund': 1.001,
                                 }})
            self.assertEqual(response.get('error', {}).get('data', {}).get('name'),
                             'odoo.exceptions.ValidationError', response)
            response = self._rpc('/web/dataset/call_button', model='payment.transaction', method='action_refund',
                                 args=[charge.ids], kwargs={'amount_to_refund': 10})
            self.assertIn('Refund wizard', response['error']['data']['message'])
            square.assert_not_called()
        self.assertFalse(charge.child_transaction_ids)
        self.assertEqual(payment.amount_available_for_refund, 50)

    def test_refund_response_loss_recovers_by_marker_and_known_id_without_another_post(self):
        charge = self._captured_order().transaction_ids
        self.authenticate('admin', 'admin')
        wizard = self._refund_wizard(charge.payment_id, 50)
        other_wizard = self._refund_wizard(charge.payment_id, 50)
        accepted = {}

        def square_refund(method, url, **kwargs):
            if method == 'POST':
                self.assertTrue(url.endswith('/v2/refunds'))
                accepted.update({
                    'id': 'SQ-REFUND-1', 'status': 'PENDING', 'location_id': 'TESTNYC',
                    'payment_id': 'SQ-PAYMENT-1', 'amount_money': {'amount': 5000, 'currency': 'USD'},
                    'reason': kwargs['json']['reason'],
                })
                raise Timeout('Square accepted the refund before the response was lost')
            self.assertEqual(method, 'GET')
            if url.endswith('/v2/refunds'):
                self.assertEqual(kwargs['params']['location_id'], 'TESTNYC')
                if kwargs['params'].get('cursor') == 'next-page':
                    return self._refund_response({'refunds': [accepted]})
                unrelated = {**accepted, 'reason': 'Square Dashboard', 'id': 'SQ-UNRELATED', 'status': 'COMPLETED'}
                return self._refund_response({'refunds': [unrelated], 'cursor': 'next-page'})
            self.assertTrue(url.endswith('/v2/refunds/SQ-REFUND-1'))
            return self._refund_response({'refund': {**accepted, 'status': 'COMPLETED'}})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_refund) as square:
            action = self._refund_button('payment.refund.wizard', 'action_refund', wizard)
            self.assertEqual(self._refund_button('payment.refund.wizard', 'action_refund', wizard), action)
            refund = charge.child_transaction_ids
            self.assertEqual(refund.state, 'pending')
            self.assertFalse(refund.provider_reference)
            self.assertFalse(refund.payment_id)
            self.assertEqual(charge.payment_id.amount_available_for_refund, 0)
            wizard_fields = self._result('/web/dataset/call_kw', model='payment.refund.wizard', method='read',
                                         args=[[other_wizard], ['has_pending_refund', 'refunded_amount']], kwargs={})
            self.assertEqual(wizard_fields, [{'id': other_wizard, 'has_pending_refund': True, 'refunded_amount': 0}])
            blocked = self._rpc('/web/dataset/call_button', model='payment.refund.wizard', method='action_refund',
                                args=[[other_wizard]], kwargs={})
            self.assertIn('no amount available', blocked['error']['data']['message'])
            self._refund_button('payment.transaction', 'action_square_refresh_status', refund.id)
            self.assertEqual(refund.provider_reference, 'SQ-REFUND-1')
            self.assertEqual(refund.state, 'pending')
            cron = self.env.ref('payment_square_online.ir_cron_square_online_reconcile')
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
            self._refund_button('ir.cron', 'method_direct_trigger', cron.id)
        self.assertEqual([call.args[0] for call in square.call_args_list], ['POST', 'GET', 'GET', 'GET'])
        self.assertEqual(refund.state, 'done')
        self.assertEqual(refund.payment_id.payment_type, 'outbound')
        self.assertEqual(refund.payment_id.source_payment_id, charge.payment_id)
        self.assertEqual(self.env['account.payment'].search_count([('source_payment_id', '=', charge.payment_id.id)]), 1)

    def test_pending_partial_refund_reserves_only_its_amount_until_square_reports_failure(self):
        charge = self._captured_order().transaction_ids
        self.authenticate('admin', 'admin')
        payment = charge.payment_id
        stale_wizard = self._refund_wizard(payment, 40)
        pending_wizard = self._refund_wizard(payment, 18.51)
        accepted = {}
        found = False

        def square_refund(method, url, **kwargs):
            self.assertEqual(url, 'https://connect.squareupsandbox.com/v2/refunds')
            if method == 'GET':
                return self._refund_response({'refunds': [accepted] if found else []})
            self.assertEqual(method, 'POST')
            request = kwargs['json']
            data = {
                'id': request['idempotency_key'], 'status': 'COMPLETED', 'location_id': 'TESTNYC',
                'payment_id': 'SQ-PAYMENT-1', 'amount_money': request['amount_money'],
                'reason': request['reason'],
            }
            if request['amount_money']['amount'] == 1851:
                accepted.update({**data, 'status': 'FAILED'})
                raise Timeout('Square accepted the refund before the response was lost')
            return self._refund_response({'refund': data})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_refund) as square:
            pending = self._refund_button('payment.refund.wizard', 'action_refund', pending_wizard)
            refund = self.env['payment.transaction'].browse(pending['res_id'])
            self._refund_button('payment.transaction', 'action_square_refresh_status', refund.id)
            self.assertEqual(self._refund_button('payment.refund.wizard', 'action_refund', pending_wizard), pending)
            self.assertEqual(refund.state, 'pending')
            self.assertEqual(payment.amount_available_for_refund, 31.49)
            blocked = self._rpc('/web/dataset/call_button', model='payment.refund.wizard', method='action_refund',
                                args=[[stale_wizard]], kwargs={})
            self.assertIn('remaining refundable balance', blocked['error']['data']['message'])

            second_wizard = self._refund_wizard(payment, 10.25)
            summary = self._result(
                '/web/dataset/call_kw', model='payment.refund.wizard', method='read', args=[[second_wizard], [
                    'amount_available_for_refund', 'refunded_amount', 'has_pending_refund',
                ]], kwargs={},
            )
            self.assertEqual(summary, [{
                'id': second_wizard, 'amount_available_for_refund': 31.49,
                'refunded_amount': 0, 'has_pending_refund': True,
            }])
            self._refund_button('payment.refund.wizard', 'action_refund', second_wizard)
            self.assertEqual(payment.amount_available_for_refund, 21.24)

            found = True
            self._refund_button('payment.transaction', 'action_square_refresh_status', refund.id)
            self.assertEqual(refund.state, 'error')
            self.assertEqual(refund.provider_reference, accepted['id'])
            self.assertFalse(refund.payment_id)
            self.assertEqual(payment.amount_available_for_refund, 39.75)
            final_wizard = self._refund_wizard(payment, 39.75)
            self._refund_button('payment.refund.wizard', 'action_refund', final_wizard)

        self.assertEqual([call.args[0] for call in square.call_args_list], ['POST', 'GET', 'POST', 'GET', 'POST'])
        self.assertEqual([call.kwargs['json']['amount_money']['amount']
                          for call in square.call_args_list if call.args[0] == 'POST'], [1851, 1025, 3975])
        outbound = self.env['account.payment'].search([('source_payment_id', '=', payment.id)])
        self.assertCountEqual(outbound.mapped('amount'), [10.25, 39.75])
        self.assertEqual(payment.amount_available_for_refund, 0)

    def test_square_refund_actions_deny_unauthorized_users_and_companies(self):
        charge = self._captured_order().transaction_ids
        other_company = self.env['res.company'].create({'name': 'Other refund company'})
        accountant = new_test_user(self.env, login='square_refund_clerk', password='square_refund_clerk',
                                   groups='account.group_account_invoice', company_id=self.company.id)
        manager = new_test_user(self.env, login='square_refund_other', password='square_refund_other',
                                groups='account.group_account_manager', company_id=other_company.id)
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            for user in (accountant, self.customer, manager):
                with self.subTest(user=user.login):
                    self.authenticate(user.login, user.login)
                    for model, method, target in (
                        ('payment.transaction', 'action_refund', charge),
                        ('account.payment', 'action_refund_wizard', charge.payment_id),
                    ):
                        result = self._rpc('/web/dataset/call_button', model=model, method=method,
                                           args=[target.ids], kwargs={})
                        self.assertEqual(result['error']['data']['name'], 'odoo.exceptions.AccessError')
            square.assert_not_called()
        self.assertFalse(charge.child_transaction_ids)

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

    def test_definitive_square_rejections_release_checkout_for_a_safe_retry(self):
        order = self._cart()
        for code, message in (
            ('AMOUNT_TOO_LOW', 'payment amount'),
            ('VERIFY_BUYER_FAILED', 'authentication'),
            ('PAYMENT_AMOUNT_MISMATCH', 'payment amount'),
            ('INVALID_LOCATION', 'unavailable'),
        ):
            with self.subTest(code=code):
                prepared = self._prepare(order)

                def rejected(method, url, **kwargs):
                    if url.endswith('/v2/payments'):
                        return self._square_error_response(400, code)
                    return self._square_response(method, url, **kwargs)

                with patch('odoo.addons.payment_square_online.square_api.request', side_effect=rejected) as square:
                    for _attempt in range(2):
                        result = self._result('/payment/square/online/submit',
                                              **prepared['square_submission'], source_id='cnon:rejected')
                        self.assertEqual(result['state'], 'error')
                        self.assertIn(message, result['message'])
                    self.assertEqual(square.call_count, 2)
                self.assertEqual(order.state, 'draft')
                self.assertFalse(order.transaction_ids.payment_id)

        prepared = self._prepare(order)
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=self._square_response):
            self._result('/payment/square/online/submit', **prepared['square_submission'], source_id='cnon:retry')
        self._result('/payment/status/poll')
        self.assertEqual(order.state, 'sale')
        self.assertEqual(len(order.transaction_ids.payment_id), 1)

    def test_ambiguous_square_error_does_not_release_the_payment_reservation(self):
        order = self._cart()
        prepared = self._prepare(order)

        def ambiguous(method, url, **kwargs):
            if url.endswith('/v2/payments'):
                return self._refund_response({'errors': [
                    {'code': 'CARD_DECLINED'}, {'code': 'CARD_TOKEN_USED'},
                ]}, status=400)
            return self._square_response(method, url, **kwargs)

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=ambiguous) as square:
            result = self._result('/payment/square/online/submit',
                                  **prepared['square_submission'], source_id='cnon:ambiguous')
            self.assertEqual(result['state'], 'pending')
            self.assertEqual(self._result('/payment/square/online/submit',
                                         **prepared['square_submission'], source_id='cnon:ambiguous')['state'], 'pending')
            self.assertIn('error', self._rpc(f'/shop/payment/transaction/{order.id}', **self._prepare_params(order)))
            self.assertEqual(square.call_count, 2)
        self.assertEqual(order.state, 'draft')
        self.assertFalse(order.transaction_ids.payment_id)

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

    def test_late_payment_does_not_confirm_or_invoice_an_order_changed_by_staff(self):
        self.env['ir.config_parameter'].set_param('sale.automatic_invoice', 'True')
        order = self._cart()
        prepared = self._prepare(order)
        accepted = {}

        def square(method, url, **kwargs):
            if method == 'POST':
                response = self._square_response(method, url, **kwargs)
                if url.endswith('/v2/payments'):
                    accepted.update(response.json()['payment'])
                    raise Timeout('Square accepted the original total before the response was lost')
                return response
            self.assertEqual(method, 'GET')
            self.assertTrue(url.endswith('/v2/payments'))
            return self._refund_response({'payments': [accepted]})

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api:
            self.assertEqual(self._result('/payment/square/online/submit',
                                         **prepared['square_submission'], source_id='cnon:changed')['state'], 'pending')
            self.authenticate('admin', 'admin')
            line = order.order_line.filtered(lambda line: line.product_id == self.product)
            self._result('/web/dataset/call_kw', model='sale.order.line', method='write',
                         args=[line.ids, {'price_unit': 20}], kwargs={})
            self.assertEqual(order.amount_total, 28)
            tx = order.transaction_ids
            self._refund_button('payment.transaction', 'action_square_refresh_status', tx.id)

        self.assertEqual(tx.state, 'done')
        self.assertEqual(tx.payment_id.amount, 50)
        self.assertEqual(tx.payment_id.move_id.state, 'posted')
        self.assertEqual(order.state, 'draft')
        self.assertFalse(order.invoice_ids)
        self.assertTrue(tx.square_paid_changed_order)
        self.assertFalse(tx.child_transaction_ids)
        if 'picking_ids' in order._fields:
            self.assertFalse(order.picking_ids)
        self.assertEqual([call.args[0] for call in api.call_args_list], ['POST', 'POST', 'GET'])

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

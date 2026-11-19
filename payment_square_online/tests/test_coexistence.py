import hmac
import json
from base64 import b64encode
from datetime import timedelta
from functools import partial
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import uuid4

from requests import Response, Timeout

from odoo import Command, fields
from odoo.tests import tagged

from .test_checkout import SquareCheckoutCase


@tagged('post_install', '-at_install', '-standard', 'square_coexistence')
class TestSquareCoexistence(SquareCheckoutCase):
    def setUp(self):
        super().setUp()
        self.warehouse = self.env['stock.warehouse'].search([
            ('company_id', '=', self.company.id),
        ], limit=1)
        self.pos_config = self.env['square.config'].create({
            'square_application_id': 'different-pos-application',
            'square_access_token': 'pos-access-token',
            'square_environment': 'sandbox',
            'square_webhook_signature_key': 'pos-webhook-key',
            'payment_journal_id': self.journal.id,
            'location_mapping_ids': [Command.create({
                'square_location_id': 'TESTNYC', 'square_location_name': 'New York',
                'warehouse_id': self.warehouse.id,
            })],
        })

    def _pos_event(self, event_type, event_id, resource):
        kind, action = event_type.split('.')
        key = f'{kind}_{action}' if kind == 'order' else kind
        route = '/square/webhook'
        return self._signed_event(
            route, 'https://' + urlsplit(self.base_url()).netloc + route,
            'pos-webhook-key', event_type, event_id, key, resource,
        )

    def _signed_event(self, route, signed_url, signing_key, event_type, event_id, key, resource):
        payload = json.dumps({
            'event_id': event_id, 'merchant_id': 'TESTMERCHANT', 'type': event_type,
            'data': {
                'id': resource.get('id') or resource['order_id'],
                'type': event_type.split('.')[0], 'object': {key: resource},
            },
        }).encode()
        signature = b64encode(hmac.digest(signing_key.encode(), signed_url.encode() + payload, 'sha256')).decode()
        response = self.url_open(route, data=payload, headers={
            'Content-Type': 'application/json', 'x-square-hmacsha256-signature': signature,
        })
        self.env.invalidate_all()
        return response

    @staticmethod
    def _response(payload):
        response = Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        return response

    def _pos_lookup(self, resources):
        def lookup(url, **kwargs):
            self.assertEqual(urlsplit(url).netloc, 'connect.squareupsandbox.com')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer pos-access-token')
            path = urlsplit(url).path
            if path == '/v2/locations/TESTNYC':
                return self._response({'location': {'id': 'TESTNYC', 'merchant_id': 'TESTMERCHANT'}})
            if path not in resources:
                raise AssertionError(f'Unexpected Square request {path}')
            result = resources[path]
            if isinstance(result, Exception):
                raise result
            return self._response(result)

        return patch('odoo.addons.odoo_square.models.square_api_client.requests.get', side_effect=lookup)

    def test_pos_order_first_excludes_a_website_order_after_checkout_loses_its_response(self):
        cart = self._cart()
        submission = self._prepare(cart)['square_submission']
        square_order = {
            'id': 'EARLY-WEB-ORDER', 'location_id': 'TESTNYC', 'state': 'OPEN',
            'line_items': [{'uid': 'web-line', 'name': self.product.name, 'quantity': '1',
                            'base_price_money': {'amount': 5000, 'currency': 'USD'}}],
            'total_money': {'amount': 5000, 'currency': 'USD'},
        }

        def accept_order_and_lose_response(method, url, **kwargs):
            self.assertEqual((method, urlsplit(url).path), ('POST', '/v2/orders'))
            square_order['reference_id'] = kwargs['json']['order']['reference_id']
            raise Timeout('Square accepted the order before the connection closed')

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=accept_order_and_lose_response):
            self._result('/payment/square/online/submit', **submission, source_id='cnon:sdk-token')
        self.assertFalse(cart.transaction_ids.square_order_id)
        self.assertFalse(cart.transaction_ids.provider_reference)
        notice = {'order_id': square_order['id'], 'location_id': 'TESTNYC', 'state': 'OPEN'}
        sale_count = self.env['sale.order'].search_count([])
        with self._pos_lookup({'/v2/orders/EARLY-WEB-ORDER': {'order': square_order}}):
            response = self._pos_event('order.created', 'order-arrives-first', notice)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.env['sale.order'].search_count([]), sale_count)
        self.assertFalse(cart.square_order_id)
        self.assertEqual(cart.state, 'draft')

    def test_return_order_and_refund_reach_pos_before_website_recovers_payment(self):
        cart = self._cart()
        submission = self._prepare(cart)['square_submission']
        before = {
            model: self.env[model].search_count([])
            for model in ('sale.order', 'account.payment', 'account.move', 'square.refund')
        }
        quant = self.env['stock.quant']
        location = self.warehouse.lot_stock_id
        quant._update_available_quantity(self.product, location, 8)
        path = f'/payment/square/online/webhook/{self.provider.id}'
        signed_url = 'https://square-test.example.test' + path
        self.provider.write({'square_webhook_url': signed_url, 'square_webhook_signature_key': 'online-key'})

        refund = {
            'id': 'WEB-REFUND', 'payment_id': 'SQ-PAYMENT-1', 'status': 'COMPLETED',
            'amount_money': {'amount': 2500, 'currency': 'USD'}, 'location_id': 'TESTNYC',
            'order_id': 'WEB-RETURN',
        }
        return_order = {
            'id': 'WEB-RETURN', 'location_id': 'TESTNYC', 'state': 'COMPLETED',
            'returns': [{'source_order_id': 'SQ-ORDER-1'}],
        }
        square_resources = {
            '/v2/refunds/WEB-REFUND': {'refund': refund},
            '/v2/orders/WEB-RETURN': {'order': return_order},
        }

        def square(method, url, **kwargs):
            endpoint = urlsplit(url).path
            if method == 'GET':
                return self._response(square_resources[endpoint])
            response = self._square_response(method, url, **kwargs)
            if endpoint == '/v2/orders':
                square_resources['/v2/orders/SQ-ORDER-1'] = response.json()
                return response
            square_resources['/v2/payments/SQ-PAYMENT-1'] = response.json()
            raise Timeout('Square accepted the payment before the connection closed')

        with (
            patch('odoo.addons.payment_square_online.square_api.request', side_effect=square) as api,
            self._pos_lookup(square_resources),
        ):
            self.assertEqual(self._result(
                '/payment/square/online/submit', **submission, source_id='cnon:sdk-token',
            )['state'], 'pending')
            tx = cart.transaction_ids
            self.assertFalse(tx.provider_reference)
            self.assertFalse(tx.child_transaction_ids)
            payment = square_resources['/v2/payments/SQ-PAYMENT-1']['payment']
            return_notice = {'order_id': 'WEB-RETURN', 'location_id': 'TESTNYC', 'state': 'COMPLETED'}
            # The order arrives before either addon has seen a financial notification.
            for event, event_id, data in (
                ('order.created', 'early-return', return_notice),
                ('payment.updated', 'early-payment', payment),
                ('refund.created', 'early-refund', {'id': 'WEB-REFUND', 'payment_id': 'SQ-PAYMENT-1'}),
            ):
                response = self._pos_event(event, event_id, data)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['status'], 'ignored', response.text)
            self.assertFalse(tx.provider_reference)
            self.assertFalse(tx.child_transaction_ids)

            self.authenticate('admin', 'admin')
            cron = self.env.ref('payment_square_online.ir_cron_square_online_webhooks')

            def run_inbox():
                self._result('/web/dataset/call_button', model='ir.cron', method='method_direct_trigger',
                             args=[cron.ids], kwargs={})

            def online(event, event_id, resource):
                response = self._signed_event(path, signed_url, 'online-key', event, event_id,
                                              event.split('.')[0], resource)
                self.assertEqual(response.status_code, 204)

            online('refund.created', 'online-refund-first', refund)
            online('payment.updated', 'online-payment', payment)
            online('payment.updated', 'online-payment', payment)
            run_inbox()
            online('refund.updated', 'online-refund', refund)
            run_inbox()
            self.assertEqual(len(tx.child_transaction_ids), 1)

            self.provider.state = 'disabled'
            new_location = self.env['payment.square.location'].create({
                'provider_id': self.provider.id, 'square_id': 'NEW-PROVIDER-LOCATION',
                'name': 'New checkout location', 'country_code': 'US', 'currency_code': 'USD',
            })
            self.provider.square_location_id = new_location
            for event, event_id, data in (
                ('refund.updated', 'late-refund', refund),
                ('order.updated', 'late-return', return_notice),
                ('order.created', 'early-return', return_notice),
                ('payment.updated', 'late-payment', payment),
            ):
                response = self._pos_event(event, event_id, data)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['status'], 'ignored', response.text)

        self.assertEqual(cart.state, 'sale')
        self.assertFalse(cart.square_order_id)
        self.assertEqual(tx.payment_id.amount, 50)
        self.assertEqual(tx.payment_id.payment_type, 'inbound')
        self.assertEqual(tx.child_transaction_ids.provider_reference, 'WEB-REFUND')
        self.assertEqual(tx.child_transaction_ids.payment_id.amount, 25)
        self.assertEqual(tx.child_transaction_ids.payment_id.payment_type, 'outbound')
        self.assertEqual(len(cart.picking_ids), 1)
        self.assertEqual(cart.picking_ids.picking_type_code, 'outgoing')
        self.assertNotIn(cart.picking_ids.state, ('done', 'cancel'))
        self.assertEqual(self.product.with_context(location=location.id).qty_available, 8)
        self.assertFalse(cart.invoice_ids)
        self.assertEqual(self.env['sale.order'].search_count([]), before['sale.order'])
        self.assertEqual(self.env['square.refund'].search_count([]), before['square.refund'])
        self.assertEqual(self.env['account.payment'].search_count([]), before['account.payment'] + 2)
        self.assertEqual(self.env['account.move'].search_count([]), before['account.move'] + 2)
        self.assertEqual([call.args[0] for call in api.call_args_list].count('POST'), 2)

    def _captured_website_order(self, currency='USD'):
        cart = self._cart()
        submission = self._prepare(cart)['square_submission']
        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=partial(self._square_response, currency=currency)):
            self._result('/payment/square/online/submit', **submission, source_id='cnon:sdk-token')
        self._result('/payment/status/poll')
        return cart

    def test_website_and_terminal_ownership_coexist_at_the_same_square_location(self):
        if 'pos.square.order' not in self.env:
            self.skipTest('Square Terminal is not installed')
        cart = self._captured_website_order()
        transaction = cart.transaction_ids
        pos_config = self.env['pos.config'].create({'name': 'Terminal coexistence'})
        session = self.env['pos.session'].create({'config_id': pos_config.id})
        pos_order = self.env['pos.order'].create({
            'uuid': str(uuid4()), 'session_id': session.id, 'state': 'draft',
            'amount_tax': 0, 'amount_total': 50, 'amount_return': 0, 'amount_paid': 0,
        })
        connection = self.env['pos.square.connection'].sudo().create({
            'name': 'Terminal merchant', 'company_id': self.company.id, 'environment': 'sandbox',
            'application_id': 'sandbox-sq0idb-terminal-app', 'merchant_id': 'TESTMERCHANT',
        })
        location = self.env['pos.square.location'].sudo().create({
            'connection_id': connection.id, 'square_id': 'TESTNYC',
            'name': 'New York', 'currency_code': 'USD',
        })
        terminal = {
            'id': 'TERMINAL-ORDER', 'location_id': 'TESTNYC', 'state': 'COMPLETED',
            'reference_id': 'odoo-' + uuid4().hex,
        }
        self.env['pos.square.order'].sudo().create({
            'order_id': pos_order.id, 'order_uuid': pos_order.uuid,
            'company_id': self.company.id, 'connection_id': connection.id, 'environment': 'sandbox',
            'merchant_id': 'TESTMERCHANT', 'location_id': location.id, 'square_location_id': 'TESTNYC',
            'reference': terminal['reference_id'], 'currency_code': 'USD', 'currency_digits': 2,
            'total_minor': 5000, 'create_request': {'order': terminal}, 'square_order_id': terminal['id'],
        })
        website = {
            'id': transaction.square_order_id, 'location_id': 'TESTNYC', 'state': 'COMPLETED',
            'reference_id': transaction.square_order_reference,
        }
        register = {
            'id': 'SQUARE-REGISTER', 'location_id': 'TESTNYC', 'state': 'OPEN',
            'total_money': {'amount': 5000, 'currency': 'USD'},
            'line_items': [{'uid': 'line', 'quantity': '1', 'name': self.product.name,
                            'base_price_money': {'amount': 5000, 'currency': 'USD'}}],
        }
        resources = {f'/v2/orders/{order["id"]}': {'order': order} for order in (terminal, website, register)}
        with self._pos_lookup(resources):
            webhook = {
                'id': transaction.provider_reference, 'order_id': transaction.square_order_id,
                'location_id': 'TESTNYC', 'status': 'COMPLETED',
            }
            response = self._pos_event('payment.updated', 'website-payment', webhook)
            self.assertEqual(response.json(), {'status': 'ignored', 'message': 'Square website operation'}, response.text)
            response = self._pos_event('order.created', 'terminal-order', terminal)
            self.assertEqual(response.json(), {
                'status': 'ignored', 'message': 'Odoo POS Square Terminal operation',
            }, response.text)
            response = self._pos_event('order.created', 'register-order', register)
            self.assertEqual(response.json()['status'], 'success', response.text)
        self.assertEqual(cart.state, 'sale')
        self.assertEqual(transaction.payment_id.amount, 50)
        self.assertEqual(self.env['sale.order'].search([
            ('square_order_id', 'in', [order['id'] for order in (terminal, website, register)]),
        ]).square_order_id, 'SQUARE-REGISTER')

    def _resync_button(self, record, button):
        return self._result('/web/dataset/call_button', model=record._name, method=button,
                            args=[record.ids], kwargs={})

    def test_pos_replay_quarantines_a_legacy_website_queue_entry(self):
        cart = self._captured_website_order()
        notice = {'order_id': 'SQ-ORDER-1', 'location_id': 'TESTNYC', 'state': 'OPEN'}
        api_path = '/v2/orders/SQ-ORDER-1'
        resources = {api_path: Timeout('Square order temporarily unavailable')}

        # Simulate a saved update from before the coexistence guard was installed.
        queue = self.env['square.webhook.queue'].create({
            'webhook_event_id': 'legacy-website-update', 'order_data': json.dumps(notice),
            'event_type': 'order.updated', 'square_order_id': 'SQ-ORDER-1',
            'config_id': self.pos_config.id,
            'next_retry_time': fields.Datetime.now() - timedelta(minutes=1),
        })
        self.authenticate('admin', 'admin')
        def replay():
            result = self._rpc('/web/dataset/call_kw', model=queue._name, method='process_pending_events',
                               args=[], kwargs={})
            self.assertNotIn('error', result)
            self.env.invalidate_all()

        with self._pos_lookup(resources):
            response = self._pos_event('order.updated', 'retryable-website-update', notice)
            self.assertEqual(response.status_code, 503, response.text)
            replay()
            self.assertEqual(queue.state, 'failed')
            self.assertEqual(queue.retry_count, 0)
            self.assertEqual(queue.order_data, '{}')
        self.assertFalse(cart.square_order_id)
        self.assertEqual(self.env['sale.order'].search_count([('square_order_id', '=', 'SQ-ORDER-1')]), 0)

    def test_manual_resync_excludes_website_orders_and_rechecks_previously_selected_rows(self):
        cart = self._captured_website_order()
        website = {'id': 'SQ-ORDER-1', 'location_id': 'TESTNYC', 'state': 'COMPLETED'}
        returned = {
            'id': 'WEB-RETURN', 'location_id': 'TESTNYC', 'state': 'COMPLETED',
            'returns': [{'source_order_id': 'SQ-ORDER-1'}],
        }
        resources = {f'/v2/orders/{order["id"]}': {'order': order} for order in (website, returned)}
        self.authenticate('admin', 'admin')
        wizard_action = self._resync_button(self.pos_config, 'action_open_manual_resync_wizard')
        wizard = self.env[wizard_action['res_model']].browse(wizard_action['res_id'])

        def search(url, **kwargs):
            self.assertEqual(kwargs['json']['location_ids'], ['TESTNYC'])
            self.assertEqual(urlsplit(url).path, '/v2/orders/search')
            return self._response({'orders': [website, returned]})

        with self._pos_lookup(resources), patch('odoo.addons.odoo_square.models.square_api_client.requests.post',
                                                side_effect=search):
            self._resync_button(wizard, 'action_scan_missing_orders')
            self.assertEqual(wizard.square_total, 0)
            self.assertFalse(wizard.line_ids)
            # A browser tab can retain selections from an older scan.
            wizard.line_ids = [
                Command.create({'square_order_id': order['id'], 'selected': True})
                for order in (website, returned)
            ]
            self._resync_button(wizard, 'action_validate_resync')
        self.assertEqual(wizard.state, 'done')
        self.assertEqual(wizard.processed_count, 0)
        self.assertEqual(wizard.error_count, 0)
        self.assertFalse(cart.square_order_id)
        self.assertEqual(self.env['sale.order'].search_count([('square_order_id', 'in', ['SQ-ORDER-1', 'WEB-RETURN'])]), 0)

    def test_pos_sales_refunds_and_exchange_at_the_active_website_location(self):
        self.currency = self.env.ref('base.EUR')
        self.currency.active = True
        self.company.currency_id = self.currency
        self.pricelist.currency_id = self.currency
        self.provider.square_location_id.write({'country_code': 'FR', 'currency_code': 'EUR'})
        web_order = self._captured_website_order(currency='EUR')
        website_payment = web_order.transaction_ids.payment_id
        self.assertEqual(website_payment.amount, 50)

        buyer = self.env['res.partner'].create({'name': 'Square POS buyer', 'email': 'pos-buyer@example.test'})
        tax = self.env['account.tax'].create({
            'name': 'POS test VAT', 'type_tax_use': 'sale', 'amount': 20, 'amount_type': 'percent',
        })
        original, replacement = self.env['product.product'].create([
            {'name': name, 'default_code': sku, 'type': 'consu', 'is_storable': True,
             'invoice_policy': 'delivery', 'taxes_id': [Command.set(tax.ids)]}
            for name, sku in (('Original POS item', 'POS-ORIGINAL'), ('Replacement POS item', 'POS-REPLACEMENT'))
        ])
        location = self.warehouse.lot_stock_id
        for product in (original, replacement):
            self.env['stock.quant']._update_available_quantity(product, location, 10)
        resources = {
            f'/v2/catalog/object/{product.default_code}': {
                'object': {'item_variation_data': {'sku': product.default_code}},
            }
            for product in (original, replacement)
        }

        def money(amount):
            return {'amount': amount, 'currency': 'EUR'}

        def line(product, quantity):
            return {
                'uid': product.default_code, 'name': product.name, 'catalog_object_id': product.default_code,
                'quantity': str(quantity), 'base_price_money': money(1000), 'total_money': money(1200 * quantity),
                'total_tax_money': money(200 * quantity),
            }

        def register_sale(order_id):
            order = {
                'id': order_id, 'location_id': 'TESTNYC', 'state': 'COMPLETED',
                'reference_id': (web_order.transaction_ids.square_order_reference if order_id == 'POS-FULL'
                                 else 'odoo-web-lookalike-pos-reference'),
                'line_items': [line(original, 2)], 'total_money': money(2400),
                'fulfillments': [{'pickup_details': {'recipient': {'email_address': buyer.email}}}],
                'tenders': [{
                    'id': f'{order_id}-PAYMENT', 'type': 'CARD',
                    'card_details': {'status': 'CAPTURED'}, 'amount_money': money(2400),
                }],
            }
            resources[f'/v2/orders/{order_id}'] = {'order': order}
            resources[f'/v2/payments/{order_id}-PAYMENT'] = {
                'payment': {'id': f'{order_id}-PAYMENT', 'order_id': order_id,
                            'location_id': 'TESTNYC', 'status': 'COMPLETED', 'amount_money': money(2400)},
            }
            notice = {key: order[key] for key in ('id', 'location_id', 'state')}
            response = self._pos_event('order.created', f'{order_id}-evt', notice)
            self.assertEqual(response.json()['status'], 'success', response.text)
            sale = self.env['sale.order'].search([('square_order_id', '=', order_id)])
            self.assertEqual(len(sale), 1)
            self.assertEqual(sale.invoice_ids.amount_total, 24)
            self.assertEqual(sale.invoice_ids.reconciled_payment_ids.amount, 24)
            self.assertEqual(sale.picking_ids.state, 'done')
            self.assertEqual(sale.picking_ids.move_ids.quantity, 2)
            return sale

        def refund_sale(sale, refund_id, amount):
            refund = {
                'id': refund_id, 'payment_id': sale.square_payment_id, 'order_id': sale.square_order_id,
                'location_id': 'TESTNYC', 'status': 'COMPLETED', 'amount_money': money(amount),
            }
            refund_path = f'/v2/refunds/{refund_id}'
            payment_path = f'/v2/payments/{sale.square_payment_id}'
            payment_details = resources[payment_path]
            resources[refund_path] = resources[payment_path] = Timeout('Square is temporarily unavailable')
            response = self._pos_event('refund.updated', f'{refund_id}-evt', refund)
            self.assertEqual(response.json()['status'], 'success', response.text)
            replay = self._pos_event('refund.updated', f'{refund_id}-evt', refund)
            self.assertEqual(replay.json()['status'], 'already_processed', replay.text)
            resources[refund_path] = {'refund': refund}
            resources[payment_path] = payment_details
            record = sale.square_refund_ids
            self.assertEqual(len(record), 1)
            self.assertEqual(record.credit_note_id.state, 'posted')
            self.assertEqual(record.credit_note_id.amount_total, amount / 100)
            payment = record.credit_note_id.reconciled_payment_ids
            self.assertEqual(payment.amount, amount / 100)
            self.assertEqual(payment.payment_type, 'outbound')
            self.assertEqual(record.return_picking_ids.state, 'done')
            self.assertEqual(record.return_picking_ids.move_ids.quantity, amount / 1200)

        with self._pos_lookup(resources):
            partial_sale = register_sale('POS-PARTIAL')
            refund_sale(partial_sale, 'POS-PARTIAL-REFUND', 1200)
            self.assertEqual(original.with_context(location=location.id).qty_available, 9)

            full_sale = register_sale('POS-FULL')
            refund_sale(full_sale, 'POS-FULL-REFUND', 2400)
            self.assertEqual(original.with_context(location=location.id).qty_available, 9)

            exchange_sale = register_sale('POS-EXCHANGE-ORIGINAL')
            resources['/v2/orders/POS-EXCHANGE'] = {
                'order': {
                    'id': 'POS-EXCHANGE', 'location_id': 'TESTNYC', 'state': 'COMPLETED',
                    'line_items': [line(replacement, 1)],
                    'returns': [{
                        'source_order_id': exchange_sale.square_order_id,
                        'return_line_items': [{'source_line_item_uid': original.default_code,
                                               'catalog_object_id': original.default_code, 'quantity': '1'}],
                    }],
                },
            }
            payment = {'id': 'POS-EXCHANGE-PAYMENT', 'order_id': 'POS-EXCHANGE', 'status': 'COMPLETED',
                       'location_id': 'TESTNYC', 'amount_money': money(0)}
            response = self._pos_event('payment.updated', 'pos-exchange-payment', payment)
            self.assertEqual(response.json()['status'], 'success', response.text)
            replay = self._pos_event('payment.updated', 'pos-exchange-payment', payment)
            self.assertEqual(replay.json()['status'], 'already_processed', replay.text)

        exchange = self.env['sale.order'].search([('square_order_id', '=', 'POS-EXCHANGE')])
        self.assertEqual(len(exchange), 1)
        self.assertEqual(exchange.invoice_ids.state, 'posted')
        self.assertEqual(exchange.invoice_ids.amount_total, 12)
        self.assertEqual(exchange.invoice_ids.amount_residual, 0)
        self.assertEqual(exchange_sale.invoice_ids.reversal_move_ids.amount_total, 12)
        self.assertEqual(exchange.picking_ids.move_ids.quantity, 1)
        self.assertEqual(original.with_context(location=location.id).qty_available, 8)
        self.assertEqual(replacement.with_context(location=location.id).qty_available, 9)
        self.assertEqual(sorted(
            (payment.payment_type, payment.amount)
            for payment in self.env['account.payment'].search([('partner_id', '=', buyer.id)])
        ), [('inbound', 24), ('inbound', 24), ('inbound', 24), ('outbound', 12), ('outbound', 24)])
        self.assertEqual(web_order.transaction_ids.payment_id, website_payment)
        self.assertFalse(web_order.transaction_ids.child_transaction_ids)

        # Square can deliver the POS transactions to the online application's subscription too.
        online_path = f'/payment/square/online/webhook/{self.provider.id}'
        signed_url = 'https://square-test.example.test' + online_path
        self.provider.write({'square_webhook_url': signed_url, 'square_webhook_signature_key': 'online-key'})
        self.authenticate('admin', 'admin')
        pos_payment = resources['/v2/payments/POS-FULL-PAYMENT']['payment']
        pos_refund = resources['/v2/refunds/POS-FULL-REFUND']['refund']
        for kind, resource in (('payment', pos_payment), ('refund', pos_refund)):
            response = self._signed_event(online_path, signed_url, 'online-key', f'{kind}.updated',
                                          f'unrelated-{kind}', kind, resource)
            self.assertEqual(response.status_code, 204, response.text)
        with patch('odoo.addons.payment_square_online.square_api.request') as online_api:
            self._resync_button(self.env.ref('payment_square_online.ir_cron_square_online_webhooks'),
                                'method_direct_trigger')
            online_api.assert_not_called()
        self.assertEqual(self.env['payment.transaction'].search([('provider_id', '=', self.provider.id)]),
                         web_order.transaction_ids)
        self.assertEqual(web_order.transaction_ids.payment_id, website_payment)
        self.assertFalse(web_order.transaction_ids.child_transaction_ids)
        self.assertFalse(web_order.square_order_id)
        self.assertNotIn(web_order.picking_ids.state, ('done', 'cancel'))

import hmac
import json
import os
from base64 import b64encode
from unittest.mock import patch
from uuid import uuid4
from urllib.parse import urlsplit

from cryptography.fernet import Fernet
from freezegun import freeze_time
from requests import ConnectionError, Response

from odoo import Command, api
from odoo.exceptions import AccessError, UserError
from odoo.tests import tagged
from odoo.addons.point_of_sale.tests.test_frontend import TestPointOfSaleHttpCommon


@tagged('post_install', '-at_install')
class TestSquareTerminalCheckout(TestPointOfSaleHttpCommon):

    def setUp(self):
        super().setUp()
        key = patch.dict(os.environ, {'ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY': Fernet.generate_key().decode()})
        key.start()
        self.addCleanup(key.stop)
        company = self._get_main_company()
        connection = self.env['pos.square.connection'].sudo().create({
            'name': 'Square checkout merchant', 'company_id': company.id,
            'application_id': 'sq0idp-application', 'merchant_id': 'SQUAREMERCHANT',
            'authorization_result': 'connected',
        })
        credential = connection._credential()
        credential.write({
            'access_token': credential._seal('square-access-token'), 'expires_at': '2099-03-17 10:00:00',
        })
        self.currency = self.main_pos_config.currency_id.name
        location = self.env['pos.square.location'].sudo().create({
            'connection_id': connection.id, 'square_id': 'SQUARELOC',
            'name': 'Main Street', 'currency_code': self.currency, 'is_available': True,
        })
        terminal = self.env['pos.square.terminal'].sudo().create({
            'name': 'Counter', 'connection_id': connection.id, 'location_id': location.id,
            'device_id': 'SQUARETERMINAL', 'application_version': '6.77', 'readiness': 'ready',
        })
        self.method = self.env['pos.payment.method'].create({
            'name': 'Square Counter', 'journal_id': self.bank_journal.id,
            'payment_method_type': 'terminal', 'use_payment_terminal': 'square_terminal',
            'square_terminal_id': terminal.id,
        })
        cash_journal = self.env['account.journal'].search([
            ('company_id', '=', company.id), ('type', '=', 'cash'), ('pos_payment_method_ids', '=', False),
        ], limit=1)
        self.cash_method = self.env['pos.payment.method'].create({
            'name': 'Checkout Cash', 'journal_id': cash_journal.id,
        })
        self.main_pos_config.write({
            'payment_method_ids': [Command.link(self.method.id), Command.link(self.cash_method.id)],
            'auto_validate_terminal_payment': True,
        })
        self.desk_pad.list_price = 12.34
        self.main_pos_config.with_user(self.pos_user).open_ui()
        self.pos_session = self.main_pos_config.current_session_id
        self.pos_session.set_opening_control(0, '')

    def test_signed_checkout_notification_recovers_original_payment_without_the_register(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        connection = self.method.sudo().square_terminal_id.connection_id.with_user(self.pos_admin)
        webhook_path = f'/pos/square/terminal/webhook/{connection.id}'
        webhook_url = f'https://register.example.test{webhook_path}'
        secret = 'terminal-webhook-secret'
        connection.write({
            'webhook_url': webhook_url, 'new_webhook_signature_key': secret,
        })
        self.assertTrue(connection.has_webhook_signature_key)
        self.assertFalse(connection.new_webhook_signature_key)
        checkout = {}
        requests = []
        money = {'amount': 1234, 'currency': self.currency}

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            requests.append((method, path))
            if (method, path) == ('POST', '/v2/orders/calculate'):
                payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                payload = {'order': {**self._draft_provider_order(kwargs['json']['order'], 1234), 'id': 'SQUAREORDER'}}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(
                    kwargs['json']['checkout'], id='SQUARECHECKOUT', location_id='SQUARELOC', status='PENDING',
                )
                payload = {'checkout': checkout}
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                payload = {'checkout': {**checkout, 'status': 'COMPLETED', 'payment_ids': ['SQUAREPAYMENT']}}
            elif (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                payload = {'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'COMPLETED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        def notify(event_id, *, merchant='SQUAREMERCHANT', signing_url=webhook_url):
            body = json.dumps({
                'merchant_id': merchant, 'type': 'terminal.checkout.updated', 'event_id': event_id,
                'data': {
                    'type': 'checkout', 'id': checkout['id'],
                    'object': {'checkout': {
                        'id': checkout['id'], 'reference_id': checkout['reference_id'],
                        'status': 'CANCELED', 'note': 'this payload is not provider payment evidence',
                    }},
                },
            }).encode()
            signature = b64encode(hmac.digest(secret.encode(), signing_url.encode() + body, 'sha256')).decode()
            return self.url_open(webhook_path, data=body, headers={
                'Content-Type': 'application/json', 'X-Square-HmacSha256-Signature': signature,
            })

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
            order.square_dispatch_payment(order.uuid, payment.uuid, prepared['reference'])
            self.assertFalse(payment.transaction_id)
            self.assertEqual(notify('checkout-updated', merchant='OTHER').status_code, 403)
            self.assertEqual(notify('checkout-updated', signing_url=webhook_url + '/wrong').status_code, 403)
            self.assertEqual(notify('checkout-updated').status_code, 204)
            self.assertEqual(notify('checkout-updated').status_code, 204)
            self.assertEqual(len(requests), 3)
            cron = self.env.ref('pos_square_terminal.ir_cron_square_terminal_webhooks')
            cron.method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(order.payment_ids, payment)
            self.assertEqual(payment.payment_status, 'done')
            self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')
            self.assertEqual(len(requests), 5)
            self.assertEqual(notify('checkout-older').status_code, 204)
            cron.method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(order.payment_ids, payment)
            self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')

        self.assertEqual(requests.count(('POST', '/v2/orders')), 1)
        self.assertEqual(requests.count(('POST', '/v2/terminals/checkouts')), 1)

    def test_scheduled_check_finds_lost_response_without_a_notification(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        requests = []
        checkout = {}
        money = {'amount': 1234, 'currency': self.currency}

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            requests.append((method, path))
            if (method, path) == ('POST', '/v2/orders/calculate'):
                payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                payload = {'order': {**self._draft_provider_order(kwargs['json']['order'], 1234), 'id': 'SQUAREORDER'}}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(
                    kwargs['json']['checkout'], id='SQUARECHECKOUT', location_id='SQUARELOC',
                    status='COMPLETED', payment_ids=['SQUAREPAYMENT'],
                )
                raise ConnectionError('Square received the checkout before the response was lost')
            elif (method, path) == ('POST', '/v2/terminals/checkouts/search'):
                self.assertEqual(kwargs['json']['query']['filter']['device_id'], 'SQUARETERMINAL')
                payload = {'checkouts': [checkout]}
            elif (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                payload = {'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'COMPLETED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
            order.square_dispatch_payment(order.uuid, payment.uuid, prepared['reference'])
            self.assertFalse(payment.transaction_id)
            # The cashier request has committed its attempt before the scheduled worker reads it.
            self.env.flush_all()
            cron = self.env.ref('pos_square_terminal.ir_cron_square_terminal_reconcile_payments')
            cron.method_direct_trigger()
            cron.method_direct_trigger()

        self.assertEqual(requests, [
            ('POST', '/v2/orders/calculate'), ('POST', '/v2/orders'), ('POST', '/v2/terminals/checkouts'),
            ('POST', '/v2/terminals/checkouts/search'), ('GET', '/v2/payments/SQUAREPAYMENT'),
        ])
        self.env.invalidate_all()
        self.assertEqual(order.payment_ids, payment)
        self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')

    def test_manager_checks_archived_connection_and_preserves_discrepancies(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        original_connection = self.method.sudo().square_terminal_id.connection_id
        original_checkout = {}
        money = {'amount': 1234, 'currency': self.currency}
        amount = money
        revoked = False
        requests = []

        def square_response(method, url, **kwargs):
            parsed = urlsplit(url)
            path = parsed.path
            requests.append((method, path))
            if parsed.netloc == 'connect.squareupsandbox.com':
                self.assertEqual(kwargs['headers']['Authorization'], 'Bearer replacement-token')
                self.assertEqual(method, 'POST')
                if path == '/v2/orders/calculate':
                    payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
                elif path == '/v2/orders':
                    self.assertEqual(kwargs['json']['order']['location_id'], 'NEWLOCATION')
                    payload = {'order': {**self._draft_provider_order(kwargs['json']['order'], 1234), 'id': 'NEWORDER'}}
                else:
                    self.assertEqual(path, '/v2/terminals/checkouts')
                    checkout = kwargs['json']['checkout']
                    self.assertEqual(checkout['device_options']['device_id'], 'NEWDEVICE')
                    payload = {'checkout': {**checkout, 'id': 'NEWCHECKOUT', 'location_id': 'NEWLOCATION',
                                           'status': 'PENDING'}}
            else:
                self.assertEqual(parsed.netloc, 'connect.squareup.com')
                self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
                if (method, path) == ('POST', '/v2/orders/calculate'):
                    payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
                elif (method, path) == ('POST', '/v2/orders'):
                    payload = {'order': {
                        **self._draft_provider_order(kwargs['json']['order'], 1234), 'id': 'SQUAREORDER',
                    }}
                elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                    original_checkout.update(kwargs['json']['checkout'], id='SQUARECHECKOUT',
                                             location_id='SQUARELOC', status='PENDING')
                    payload = {'checkout': original_checkout}
                elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                    payload = {'checkout': {**original_checkout, 'status': 'COMPLETED', 'payment_ids': ['SQUAREPAYMENT']}}
                elif (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                    payload = {'payment': {
                        'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                        'reference_id': original_checkout['reference_id'], 'status': 'COMPLETED',
                        'amount_money': amount, 'total_money': amount, 'source_type': 'CARD',
                    }}
                else:
                    self.fail(f'Unexpected Square request: {method} {path}')
            response = Response()
            response.status_code = 401 if revoked and parsed.netloc == 'connect.squareup.com' else 200
            response._content = json.dumps(
                {'errors': [{'code': 'ACCESS_TOKEN_REVOKED'}]} if response.status_code == 401 else payload,
            ).encode()
            return response

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            with freeze_time('2026-11-19 10:00:00'):
                order.square_prepare_payment(order.uuid, payment.uuid)
                prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
                order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
                order.square_dispatch_payment(order.uuid, payment.uuid, prepared['reference'])
            action = order.with_user(self.pos_admin).action_open_square_reconciliation()
            manager_payments = self.env[action['res_model']].with_user(self.pos_admin)
            attempt = manager_payments.search(action['domain'])
            self.assertEqual(attempt.payment_id.id, payment.id)
            self.assertEqual(attempt.requested_amount, 12.34)
            self.assertEqual(attempt.square_order_id, 'SQUAREORDER')
            self.assertEqual(attempt.square_checkout_id, 'SQUARECHECKOUT')
            with self.assertRaises(AccessError):
                attempt.with_user(self.pos_user).action_check_square_payment()

            amount = {**money, 'amount': 1235}
            with freeze_time('2026-11-19 10:05:00'):
                attempt.action_check_square_payment()
            self.assertIn('1235', attempt.reconciliation_discrepancy)
            self.assertIn('1234', attempt.reconciliation_discrepancy)
            self.assertEqual(str(attempt.last_checked_at), '2026-11-19 10:00:00')
            self.assertFalse(payment.transaction_id)

            amount = money
            with freeze_time('2026-11-19 10:10:00'):
                attempt.action_check_square_payment()
            self.assertEqual(attempt.state, 'verified')
            self.assertEqual(attempt.verified_base, 12.34)
            self.assertEqual(attempt.verified_tip, 0)
            self.assertTrue(attempt.has_verified_amounts)
            self.assertEqual(str(attempt.last_checked_at), '2026-11-19 10:10:00')
            self.assertFalse(attempt.reconciliation_error)
            self.assertTrue(attempt.reconciliation_discrepancy)

            order.action_pos_order_paid()
            self.pos_session.action_pos_session_closing_control()
            self.assertEqual(self.pos_session.state, 'closed')

            replacement = self.env['pos.square.connection'].sudo().create({
                'name': 'Replacement Square account', 'company_id': order.company_id.id, 'environment': 'sandbox',
                'application_id': 'sandbox-application', 'merchant_id': 'NEWMERCHANT',
                'authorization_result': 'connected',
            })
            credential = replacement._credential()
            credential.write({
                'access_token': credential._seal('replacement-token'), 'expires_at': '2099-03-17 10:00:00',
            })
            location = self.env['pos.square.location'].sudo().create({
                'connection_id': replacement.id, 'square_id': 'NEWLOCATION', 'name': 'New location',
                'currency_code': self.currency, 'is_available': True,
            })
            terminal = self.env['pos.square.terminal'].sudo().create({
                'name': 'Replacement register device', 'connection_id': replacement.id, 'location_id': location.id,
                'device_id': 'NEWDEVICE', 'application_version': '6.77', 'readiness': 'ready',
            })
            self.method.square_terminal_id = terminal
            original_connection.active = False
            self.main_pos_config.with_user(self.pos_user).open_ui()
            next_session = self.main_pos_config.current_session_id
            next_session.set_opening_control(0, '')
            next_order = self._cashier_draft(self.method, next_session)
            next_payment = next_order.payment_ids
            next_order.square_prepare_payment(next_order.uuid, next_payment.uuid)
            prepared = next_order.square_prepare_linked_order(next_order.uuid, next_payment.uuid)
            next_order.square_mark_dispatching(next_order.uuid, next_payment.uuid, prepared['reference'])
            next_order.square_dispatch_payment(next_order.uuid, next_payment.uuid, prepared['reference'])

            with freeze_time('2026-11-19 10:15:00'):
                attempt.action_check_square_payment()
            self.assertEqual(str(attempt.last_checked_at), '2026-11-19 10:15:00')
            self.assertEqual(attempt.connection_id.id, original_connection.id)
            self.assertEqual(attempt.square_payment_id, 'SQUAREPAYMENT')
            self.assertEqual(requests[-1], ('GET', '/v2/payments/SQUAREPAYMENT'))

            revoked = True
            with freeze_time('2026-11-19 10:20:00'):
                attempt.action_check_square_payment()
            self.assertEqual(str(attempt.last_checked_at), '2026-11-19 10:15:00')
            self.assertIn(original_connection.name, attempt.reconciliation_error)
            self.assertTrue(attempt.reconciliation_discrepancy)
            self.assertEqual(attempt.state, 'verified')

        self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')
        self.assertEqual(order.payment_ids, payment)
        self.assertEqual(requests.count(('POST', '/v2/terminals/checkouts')), 2)

    def test_first_card_is_verified_on_original_sale(self):
        self.desk_pad.default_code = 'DESK-PAD'
        self.monitor_stand.default_code = 'MONITOR-STAND'
        mutations = []
        calculated_order = {}
        checkout = {}
        pending_reads = 0
        payment_reads = 0
        original_ids = []
        money = {'amount': 1553, 'currency': self.currency}

        def square_response(method, url, **kwargs):
            nonlocal pending_reads, payment_reads
            self.assertEqual(urlsplit(url).netloc, 'connect.squareup.com')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
            path = urlsplit(url).path
            if (method, path) == ('POST', '/v2/catalog/search'):
                search = kwargs['json']
                self.assertEqual(search['object_types'], ['ITEM_VARIATION'])
                sku = search['query']['exact_query']['attribute_value']
                self.assertIn(sku, {'DESK-PAD', 'MONITOR-STAND'})
                return self._square_response({'objects': ([{
                    'type': 'ITEM_VARIATION', 'id': 'SQUAREVARIATION', 'version': 1700000000000,
                    'present_at_all_locations': True,
                    'item_variation_data': {'sku': 'DESK-PAD', 'item_id': 'DESK-ITEM'},
                }] if sku == 'DESK-PAD' else []), 'related_objects': [{'type': 'ITEM', 'id': 'DESK-ITEM'}]})
            if (method, path) == ('POST', '/v2/orders/calculate'):
                order = kwargs['json']['order']
                self.assertEqual(kwargs['json'], {'order': order})
                calculated_order.update(order)
                return self._square_response({'order': {**order, 'total_money': money, 'line_items': [
                    {**line, 'total_money': {'amount': amount, 'currency': self.currency}}
                    for line, amount in zip(order['line_items'], (1234, 319))
                ]}})
            # Read committed POS data as seen between separate cashier RPCs.
            with self.registry.cursor() as cr:
                env = api.Environment(cr, self.env.uid, {})
                order = env['pos.order'].search([('session_id', '=', self.pos_session.id)])
                self.assertEqual(len(order), 1)
                self.assertEqual(order.state, 'draft')
                self.assertFalse(order.partner_id)
                self.assertTrue(order.uuid)
                self.assertEqual(order.payment_ids.payment_method_id.id, self.method.id)
                self.assertEqual(order.payment_ids.amount, 15.53)
                self.assertTrue(order.payment_ids.uuid)
                self.assertNotEqual(order.payment_ids.payment_status, 'done')
                self.assertFalse(order.payment_ids.transaction_id)
                attempt = env['pos.square.payment.attempt'].sudo().search([('order_id', '=', order.id)])
                self.assertEqual(len(attempt), 1)
                link = attempt.square_order_link_id
                self.assertEqual(attempt.payment_uuid, order.payment_ids.uuid)
                self.assertEqual(link.order_uuid, order.uuid)
                self.assertEqual(attempt.merchant_id, 'SQUAREMERCHANT')
                self.assertEqual(attempt.environment, 'production')
                ids = (order.id, order.uuid, order.payment_ids.id, order.payment_ids.uuid)
                if not original_ids:
                    original_ids.append(ids)
                self.assertEqual(ids, original_ids[0])
                if (method, path) == ('POST', '/v2/orders'):
                    self.assertEqual(link.create_request, kwargs['json'])
                    self.assertTrue(attempt.checkout_request['idempotency_key'])
                if (method, path) == ('POST', '/v2/terminals/checkouts'):
                    self.assertEqual(attempt.checkout_request, kwargs['json'])
                    self.assertEqual(link.square_order_id, 'SQUAREORDER')
            if (method, path) == ('POST', '/v2/orders'):
                order = kwargs['json']['order']
                self.assertEqual(order, calculated_order)
                mutations.append(kwargs['json'])
                self.assertEqual(order['location_id'], 'SQUARELOC')
                self.assertTrue(order['reference_id'])
                self.assertNotIn('fulfillments', order)
                self.assertNotIn('customer_id', order)
                self.assertEqual(order['pricing_options'], {'auto_apply_discounts': False, 'auto_apply_taxes': False})
                desk, monitor = order['line_items']
                self.assertEqual(desk['catalog_object_id'], 'SQUAREVARIATION')
                self.assertEqual(desk['catalog_version'], 1700000000000)
                self.assertEqual(desk['base_price_money'], {'amount': 1234, 'currency': self.currency})
                self.assertEqual(monitor['name'], 'Monitor Stand')
                self.assertNotIn('catalog_object_id', monitor)
                payload = {'order': {**order, 'id': 'SQUAREORDER', 'total_money': money, 'line_items': [
                    {**line, 'total_money': {'amount': amount, 'currency': self.currency}}
                    for line, amount in zip((desk, monitor), (1234, 319))
                ]}}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                mutations.append(kwargs['json'])
                checkout.update(kwargs['json']['checkout'])
                self.assertEqual(checkout['order_id'], 'SQUAREORDER')
                self.assertEqual(checkout['amount_money'], money)
                self.assertEqual(checkout['device_options']['device_id'], 'SQUARETERMINAL')
                self.assertTrue(checkout['device_options']['show_itemized_cart'])
                self.assertFalse(checkout['device_options']['tip_settings']['allow_tipping'])
                self.assertNotIn('customer_id', checkout)
                checkout.update(id='SQUARECHECKOUT', location_id='SQUARELOC', status='PENDING')
                payload = {'checkout': checkout}
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                pending_reads += 1
                if pending_reads >= 2:
                    checkout.update(status='COMPLETED', payment_ids=['SQUAREPAYMENT'])
                payload = {'checkout': checkout}
            elif (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                payment_reads += 1
                payload = {'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'terminal_checkout_id': 'SQUARECHECKOUT',
                    'location_id': 'SQUARELOC', 'reference_id': checkout['reference_id'],
                    'status': 'APPROVED' if payment_reads == 1 else 'COMPLETED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour('SquareTerminalCardCheckout')

        self.assertEqual(len(mutations), 2)
        self.assertEqual(len({request['idempotency_key'] for request in mutations}), 2)
        self.assertEqual(payment_reads, 2)
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(len(order), 1)
        self.assertEqual(order.state, 'paid')
        self.assertFalse(order.partner_id)
        self.assertEqual(order.amount_total, 15.53)
        self.assertEqual(order.lines.product_id, self.desk_pad | self.monitor_stand)
        self.assertEqual(len(order.payment_ids), 1)
        self.assertEqual(order.payment_ids.payment_method_id, self.method)
        self.assertEqual(order.payment_ids.payment_status, 'done')
        self.assertEqual(order.payment_ids.transaction_id, 'SQUAREPAYMENT')

        self.assertEqual(
            (order.id, order.uuid, order.payment_ids.id, order.payment_ids.uuid), original_ids[0],
        )

    def test_lost_checkout_response_recovers_original_payment_after_reload(self):
        money = {'amount': 1234, 'currency': self.currency}
        checkout = {}
        mutations = []
        searches = []
        previews = []

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            if (method, path) == ('POST', '/v2/orders/calculate'):
                order = kwargs['json']['order']
                previews.append(order)
                payload = {'order': self._draft_provider_order(order, 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                mutations.append(path)
                self.assertEqual(kwargs['json']['order'], previews[0])
                order = self._draft_provider_order(kwargs['json']['order'], 1234)
                payload = {'order': {**order, 'id': 'SQUAREORDER'}}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                mutations.append(path)
                checkout.update(kwargs['json']['checkout'])
                checkout.update(id='SQUARECHECKOUT', location_id='SQUARELOC', status='CANCELED',
                                cancel_reason='TIMED_OUT')
                raise ConnectionError('Square accepted the checkout before the response was lost')
            elif (method, path) == ('POST', '/v2/terminals/checkouts/search'):
                searches.append(kwargs['json'])
                self.assertEqual(kwargs['json']['query']['filter']['device_id'], 'SQUARETERMINAL')
                payload = (
                    {'checkouts': [checkout]} if kwargs['json'].get('cursor') == 'page-2'
                    else {'checkouts': [{**checkout, 'reference_id': 'another-reference'}], 'cursor': 'page-2'}
                )
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                payload = {'checkout': {**checkout, 'status': 'COMPLETED', 'payment_ids': ['SQUAREPAYMENT']}}
            elif (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                payload = {'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'COMPLETED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            response = Response()
            response.status_code = 200
            response._content = json.dumps(payload).encode()
            return response

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour('SquareTerminalLostCheckoutResponse')

        self.assertEqual(mutations, ['/v2/orders', '/v2/terminals/checkouts'])
        self.assertEqual(len(previews), 1)
        self.assertEqual(len(searches), 2)
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(len(order), 1)
        self.assertEqual(order.state, 'paid')
        self.assertEqual(len(order.payment_ids), 1)
        self.assertEqual(order.payment_ids.transaction_id, 'SQUAREPAYMENT')

    def test_shared_physical_terminal_requires_explicit_send_after_busy(self):
        terminal = self.method.sudo().square_terminal_id.copy({
            'name': 'Counter from the second register', 'device_id': 'device:SQUARETERMINAL', 'readiness': 'ready',
        })
        second_method = self.method.copy({
            'name': 'Shared Square Counter', 'square_terminal_id': terminal.id,
        })
        second_config = self.main_pos_config.copy({
            'name': 'Second Square register', 'payment_method_ids': [Command.set(second_method.ids)],
        })
        second_config.with_user(self.pos_user).open_ui()
        second_session = second_config.current_session_id
        second_session.set_opening_control(0, '')
        order = self._cashier_draft()
        second_order = self._cashier_draft(second_method, second_session)
        payment = order.payment_ids
        second_payment = second_order.payment_ids
        money = {'amount': 1234, 'currency': self.currency}
        attempts = []
        order_ids = []

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            if (method, path) == ('POST', '/v2/orders/calculate'):
                payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                order_id = f'SQUAREORDER-{len(order_ids) + 1}'
                order_ids.append(order_id)
                order = self._draft_provider_order(kwargs['json']['order'], 1234)
                payload = {'order': {**order, 'id': order_id}}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout = {
                    **kwargs['json']['checkout'], 'id': f'SQUARECHECKOUT-{len(attempts) + 1}',
                    'location_id': 'SQUARELOC', 'status': 'PENDING',
                }
                attempts.append(checkout)
                payload = {'checkout': checkout}
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT-1'):
                payload = {'checkout': {**attempts[0], 'status': 'COMPLETED', 'payment_ids': ['SQUAREPAYMENT']}}
            elif (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                payload = {'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': order_ids[0], 'location_id': 'SQUARELOC',
                    'reference_id': attempts[0]['reference_id'], 'status': 'COMPLETED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            response = Response()
            response.status_code = 200
            response._content = json.dumps(payload).encode()
            return response

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
            order.square_dispatch_payment(order.uuid, payment.uuid, prepared['reference'])

            result = second_order.square_prepare_payment(second_order.uuid, second_payment.uuid)
            self.assertEqual(result['state'], 'busy')
            result = second_order.square_check_payment(second_order.uuid, second_payment.uuid)
            self.assertEqual(result['state'], 'unstarted')
            self.assertEqual(len(order_ids), 1)
            self.assertEqual(len(attempts), 1)

            abandoned = self._cashier_draft(second_method, second_session)
            unused_tender = abandoned.payment_ids
            self.assertEqual(abandoned.square_prepare_payment(abandoned.uuid, unused_tender.uuid)['state'], 'busy')
            unused_tender.unlink()

            order.square_check_payment(order.uuid, payment.uuid)
            self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')
            self.assertEqual(len(attempts), 1)

            # An explicit Send from the other register can now reserve the device.
            second_order.square_prepare_payment(second_order.uuid, second_payment.uuid)
            prepared = second_order.square_prepare_linked_order(second_order.uuid, second_payment.uuid)
            second_order.square_mark_dispatching(second_order.uuid, second_payment.uuid, prepared['reference'])
            second_order.square_dispatch_payment(second_order.uuid, second_payment.uuid, prepared['reference'])
            self.assertEqual(len(order_ids), 2)
            self.assertEqual(len(attempts), 2)

    def test_active_decline_reserves_tender_until_confirmed_failure_then_retries_once(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        money = {'amount': 1234, 'currency': self.currency}
        requests = []
        checkouts = []
        first_state = 'IN_PROGRESS'

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            requests.append((method, path))
            if (method, path) == ('POST', '/v2/orders/calculate'):
                payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                order = self._draft_provider_order(kwargs['json']['order'], 1234)
                payload = {'order': {**order, 'id': 'SQUAREORDER'}}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                request = kwargs['json']
                checkout = {
                    **request['checkout'], 'id': f'SQUARECHECKOUT-{len(checkouts) + 1}',
                    'location_id': 'SQUARELOC', 'status': 'PENDING',
                }
                checkouts.append((request['idempotency_key'], checkout))
                payload = {'checkout': checkout}
            elif (method, path) == ('POST', '/v2/terminals/checkouts/search'):
                payload = {'checkouts': []}
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT-1'):
                payload = {'checkout': {
                    **checkouts[0][1], 'status': first_state, 'payment_ids': ['FAILED-TAP'],
                }}
            elif (method, path) == ('GET', '/v2/payments/FAILED-TAP'):
                payload = {'payment': {
                    'id': 'FAILED-TAP', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkouts[0][1]['reference_id'], 'status': 'FAILED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT-2'):
                payload = {'checkout': {
                    **checkouts[1][1], 'status': 'COMPLETED', 'payment_ids': ['CAPTURED-TAP'],
                }}
            elif (method, path) == ('GET', '/v2/payments/CAPTURED-TAP'):
                payload = {'payment': {
                    'id': 'CAPTURED-TAP', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkouts[1][1]['reference_id'], 'status': 'COMPLETED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            response = Response()
            response.status_code = 200
            response._content = json.dumps(payload).encode()
            return response

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
            # A status read that races the first provider request cannot dispatch or abandon it.
            result = order.square_check_payment(order.uuid, payment.uuid)
            self.assertEqual(result['state'], 'dispatching')
            self.assertFalse(checkouts)
            order.square_dispatch_payment(order.uuid, payment.uuid, prepared['reference'])
            result = order.square_check_payment(order.uuid, payment.uuid)
            self.assertEqual(result['state'], 'declined')
            original_attempt = result['reference']
            self.assertEqual(order.square_retry_payment(order.uuid, payment.uuid, original_attempt)['state'], 'declined')
            self.assertEqual(len(checkouts), 1)
            self.assertFalse(payment.transaction_id)
            with self.assertRaises(UserError), self.cr.savepoint():
                payment.unlink()

            first_state = 'COMPLETED'
            self.assertEqual(order.square_check_payment(order.uuid, payment.uuid)['state'], 'failed')
            retry = order.square_retry_payment(order.uuid, payment.uuid, original_attempt)
            duplicate = order.square_retry_payment(order.uuid, payment.uuid, original_attempt)
            self.assertEqual(retry['reference'], duplicate['reference'])
            order.square_mark_dispatching(order.uuid, payment.uuid, retry['reference'])
            order.square_dispatch_payment(order.uuid, payment.uuid, retry['reference'])
            order.square_dispatch_payment(order.uuid, payment.uuid, retry['reference'])
            self.assertEqual(order.square_check_payment(order.uuid, payment.uuid)['state'], 'verified')

        self.assertEqual(requests.count(('POST', '/v2/orders')), 1)
        self.assertEqual(requests.count(('POST', '/v2/orders/calculate')), 1)
        self.assertEqual(len(checkouts), 2)
        self.assertEqual(len({key for key, _checkout in checkouts}), 2)
        self.assertEqual(order.payment_ids, payment)
        self.assertEqual(payment.transaction_id, 'CAPTURED-TAP')

    @staticmethod
    def _square_response(payload):
        response = Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        return response

    def test_cash_stays_local_with_square_configured(self):
        with patch('odoo.addons.pos_square_terminal.square_api.request') as provider:
            self.start_pos_tour('SquareTerminalCashOnly')
            provider.assert_not_called()
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.state, 'paid')
        self.assertEqual(order.payment_ids.payment_method_id, self.cash_method)
        self.assertFalse(self.env['pos.square.order'].sudo().search_count([('order_id', '=', order.id)]))

    def test_fractional_line_preserves_inclusive_tax_and_discount(self):
        tax = self._included_vat()
        self.desk_pad.write({'lst_price': 12, 'taxes_id': [Command.set(tax.ids)]})

        def check_order(order):
            line, = order['line_items']
            self.assertEqual(line['name'], 'Desk Pad')
            self.assertEqual(line['quantity'], '1.5')
            self.assertEqual(line['quantity_unit']['precision'], 1)
            self.assertEqual(line['base_price_money'], self._money(1200))
            square_tax, = order['taxes']
            self.assertEqual(square_tax['name'], 'Included VAT')
            self.assertEqual(square_tax['type'], 'INCLUSIVE')
            self.assertEqual(square_tax['percentage'], '20')
            self.assertEqual(square_tax['scope'], 'LINE_ITEM')
            self.assertEqual(line['applied_taxes'], [{'tax_uid': square_tax['uid']}])
            discount, = order['discounts']
            self.assertEqual(discount['amount_money'], self._money(180))
            self.assertEqual(discount['scope'], 'LINE_ITEM')
            self.assertEqual(line['applied_discounts'], [{'discount_uid': discount['uid']}])

        self._run_priced_checkout('SquareTerminalDiscountedFraction', ((1620, 270, 180),), check_order)
        order = self.pos_session.order_ids
        self.assertEqual(order.amount_total, 16.20)
        self.assertEqual(order.amount_tax, 2.70)
        self.assertEqual(order.lines.qty, 1.5)
        self.assertEqual(order.lines.discount, 10)

    def test_combo_falls_back_to_square_summary_and_keeps_native_receipt(self):
        tax = self._included_vat()
        self.desk_pad.write({'lst_price': 3, 'taxes_id': [Command.set(tax.ids)]})
        self.monitor_stand.lst_price = 3
        self.whiteboard_pen.write({'lst_price': 20, 'to_weight': False})
        choices = self.env['product.combo'].create([{
            'name': product.name,
            'combo_item_ids': [Command.create({'product_id': product.id})],
        } for product in (self.desk_pad, self.monitor_stand)])
        combo = self.env['product.product'].create({
            'name': 'Lunch Combo', 'type': 'combo', 'list_price': 42, 'available_in_pos': True,
            'taxes_id': [Command.clear()], 'combo_ids': [Command.set(choices.ids)],
        })

        def check_order(order):
            with self.registry.cursor() as cr:
                env = api.Environment(cr, self.env.uid, {})
                native_order = env['pos.order'].search([('session_id', '=', self.pos_session.id)])
                receipt_reference = native_order.pos_reference
            self.assertTrue(receipt_reference)
            taxed, untaxed = order['line_items']
            for line in (taxed, untaxed):
                self.assertIn(receipt_reference, line['name'])
                self.assertIn('Lunch Combo', line['name'])
                self.assertEqual(line['quantity'], '1')
                self.assertNotIn('catalog_object_id', line)
            self.assertEqual(taxed['base_price_money'], self._money(2100))
            self.assertEqual(untaxed['base_price_money'], self._money(4100))
            square_tax, = order['taxes']
            self.assertEqual(square_tax['type'], 'INCLUSIVE')
            self.assertEqual(taxed['applied_taxes'], [{'tax_uid': square_tax['uid']}])
            discount, = order['discounts']
            self.assertEqual(discount['amount_money'], self._money(200))
            self.assertEqual(untaxed['applied_discounts'], [{'discount_uid': discount['uid']}])

        self._run_priced_checkout('SquareTerminalComboSummary', ((2100, 350, 0), (3900, 0, 200)), check_order)
        order = self.pos_session.order_ids
        self.assertEqual(order.lines.product_id, combo | self.desk_pad | self.monitor_stand | self.whiteboard_pen)
        parent = order.lines.filtered(lambda line: line.product_id == combo)
        self.assertEqual(parent.combo_line_ids.product_id, self.desk_pad | self.monitor_stand)
        self.assertEqual(order.amount_total, 60.0)
        self.assertEqual(order.amount_tax, 3.50)

    def _included_vat(self):
        return self.env['account.tax'].create({
            'name': 'Included VAT', 'amount_type': 'percent', 'amount': 20,
            'price_include_override': 'tax_included', 'company_id': self._get_main_company().id,
        })

    def _money(self, amount):
        return {'amount': amount, 'currency': self.currency}

    def _run_priced_checkout(self, tour, line_amounts, check_order):
        fields = ('total_money', 'total_tax_money', 'total_discount_money')
        gross = sum(amounts[0] for amounts in line_amounts)
        totals = {field: self._money(sum(amounts)) for field, amounts in zip(fields, zip(*line_amounts))}
        calls = []
        estimated = {}
        checkout = {}

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            calls.append((method, path))
            if path in ('/v2/orders/calculate', '/v2/orders') and method == 'POST':
                order = kwargs['json']['order']
                check_order(order)
                self.assertEqual(len(order['line_items']), len(line_amounts))
                response = {**order, **totals, 'line_items': [
                    {**line, **{field: self._money(amount) for field, amount in zip(fields, amounts)}}
                    for line, amounts in zip(order['line_items'], line_amounts)
                ]}
                if path == '/v2/orders/calculate':
                    estimated.update(order)
                else:
                    self.assertEqual(order, estimated)
                    response['id'] = 'SQUAREORDER'
                return self._square_response({'order': response})
            if (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(kwargs['json']['checkout'])
                self.assertEqual(checkout['order_id'], 'SQUAREORDER')
                self.assertEqual(checkout['amount_money'], self._money(gross))
                checkout.update(id='SQUARECHECKOUT', location_id='SQUARELOC', status='PENDING')
                return self._square_response({'checkout': checkout})
            if (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                checkout.update(status='COMPLETED', payment_ids=['SQUAREPAYMENT'])
                return self._square_response({'checkout': checkout})
            if (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                return self._square_response({'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER',
                    'location_id': 'SQUARELOC', 'reference_id': checkout['reference_id'],
                    'status': 'COMPLETED', 'amount_money': self._money(gross),
                    'total_money': self._money(gross), 'source_type': 'CARD',
                }})
            self.fail(f'Unexpected Square request: {method} {path}')

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour(tour)
        self.assertEqual([path for method, path in calls if method == 'POST'], [
            '/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts',
        ])
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.state, 'paid')
        self.assertEqual(order.payment_ids.transaction_id, 'SQUAREPAYMENT')

    def _cashier_draft(self, method=None, session=None):
        method = method or self.method
        session = session or self.pos_session
        return self.env['pos.order'].with_user(self.pos_user).create({
            'uuid': str(uuid4()), 'session_id': session.id, 'state': 'draft',
            'amount_tax': 0, 'amount_total': 12.34, 'amount_return': 0, 'amount_paid': 12.34,
            'lines': [Command.create({
                'uuid': str(uuid4()), 'product_id': self.desk_pad.id, 'price_unit': 12.34,
                'qty': 1, 'price_subtotal': 12.34, 'price_subtotal_incl': 12.34,
            })],
            'payment_ids': [Command.create({
                'uuid': str(uuid4()), 'payment_method_id': method.id, 'amount': 12.34,
                'payment_status': 'waiting' if method.use_payment_terminal == 'square_terminal' else False,
            })],
        })

    def test_cashier_cannot_forge_or_replace_square_payment(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        with self.assertRaises(AccessError), self.cr.savepoint():
            payment.write({'payment_status': 'done', 'transaction_id': 'fake'})
        with self.assertRaises(UserError), self.cr.savepoint():
            order.action_pos_order_paid()
        cash_order = self._cashier_draft(self.cash_method)
        with self.assertRaises(UserError), self.cr.savepoint():
            cash_order.write({'state': 'paid', 'payment_ids': [Command.link(payment.id)]})
        self.assertEqual(cash_order.state, 'draft')
        self.assertEqual(payment.pos_order_id, order)
        cash_payment = cash_order.payment_ids
        cash_payment.write({'payment_status': 'done', 'transaction_id': 'fake'})
        with self.assertRaises(AccessError), self.cr.savepoint():
            cash_payment.write({'payment_method_id': self.method.id})
        def square_response(method, url, **kwargs):
            self.assertEqual((method, urlsplit(url).path), ('POST', '/v2/orders/calculate'))
            return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1234)})

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
        with self.assertRaises(UserError), self.cr.savepoint():
            payment.unlink()
        with self.assertRaises(UserError), self.cr.savepoint():
            order.lines.write({'qty': 2})
        with self.assertRaises(UserError), self.cr.savepoint():
            order.write({'payment_ids': [Command.create({
                'payment_method_id': self.cash_method.id, 'amount': 12.34,
            })]})
        self.assertEqual(order.state, 'draft')
        self.assertEqual(payment.payment_status, 'waiting')

    def _draft_provider_order(self, order, total):
        line, = order['line_items']
        return {
            **order, 'total_money': self._money(total), 'line_items': [{**line, 'total_money': self._money(1234)}],
        }

    def test_preview_discrepancy_stops_terminal_request(self):
        eur = self.env.ref('base.EUR')
        eur.active = True
        self.main_pos_config.journal_id.currency_id = eur
        self.method.sudo().square_terminal_id.location_id.write({'currency_code': 'EUR', 'country_code': 'FR'})
        self.currency = self.main_pos_config.currency_id.name
        self.assertEqual(self.currency, 'EUR')

        def square_response(method, url, **kwargs):
            self.assertEqual((method, urlsplit(url).path), ('POST', '/v2/orders/calculate'))
            return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1235)})

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response) as provider:
            self.start_pos_tour('SquareTerminalPreviewDiscrepancy')
            provider.assert_called_once()
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.state, 'draft')
        self.assertFalse(order.payment_ids.transaction_id)

    def test_created_order_discrepancy_stops_terminal_request(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        calls = []

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            self.assertEqual(method, 'POST')
            calls.append(path)
            if path == '/v2/orders/calculate':
                return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1234)})
            self.assertEqual(path, '/v2/orders')
            order_response = self._draft_provider_order(kwargs['json']['order'], 1235)
            return self._square_response({'order': {**order_response, 'id': 'SQUAREORDER'}})

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            result = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, result['reference'])
            result = order.square_dispatch_payment(order.uuid, payment.uuid, result['reference'])
            self.assertEqual(result['state'], 'blocked')
        self.assertEqual(calls, ['/v2/orders/calculate', '/v2/orders'])
        self.assertEqual(order.state, 'draft')
        self.assertFalse(payment.transaction_id)

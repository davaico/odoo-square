import hmac
import json
import os
from base64 import b64encode
from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4
from urllib.parse import urlsplit

from cryptography.fernet import Fernet
from freezegun import freeze_time
from requests import ConnectionError, Response

from odoo import Command, api, fields
from odoo.exceptions import AccessError, UserError
from odoo.tests import tagged
from odoo.addons.point_of_sale.tests.test_frontend import TestPointOfSaleHttpCommon


@tagged('post_install', '-at_install')
class TestSquareTerminalCheckout(TestPointOfSaleHttpCommon):

    def setUp(self):
        super().setUp()
        self.desk_pad = self.desk_pad.product_variant_id
        self.monitor_stand = self.monitor_stand.product_variant_id
        self.whiteboard_pen = self.whiteboard_pen.product_variant_id
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
            self._send_terminal_checkout(order, payment, prepared['reference'])
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

    def test_refund_notification_recovers_the_original_sale_before_its_payment_notification(self):
        eur = self.env.ref('base.EUR')
        eur.active = True
        self._get_main_company().currency_id = eur
        self.main_pos_config.available_pricelist_ids.currency_id = eur
        self.currency = 'EUR'
        connection = self.method.sudo().square_terminal_id.connection_id.with_user(self.pos_admin)
        self.method.sudo().square_terminal_id.location_id.write({'currency_code': 'EUR', 'country_code': 'FR'})
        self.desk_pad.list_price = 44
        order = self._cashier_draft(amount=44)
        payment = order.payment_ids
        webhook_path = f'/pos/square/terminal/webhook/{connection.id}'
        webhook_url = f'https://register.example.test{webhook_path}'
        secret = 'terminal-webhook-secret'
        connection.write({'webhook_url': webhook_url, 'new_webhook_signature_key': secret})
        checkout = {}
        requests = []

        def square_response(method, url, **kwargs):
            parsed = urlsplit(url)
            self.assertEqual(parsed.netloc, 'connect.squareup.com')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
            path = parsed.path
            requests.append((method, path))
            if method == 'POST' and path in ('/v2/orders/calculate', '/v2/orders'):
                requested = kwargs['json']['order']
                line, = requested['line_items']
                return self._square_response({'order': {
                    **requested, 'id': 'SQUAREORDER', 'total_money': self._money(4400),
                    'line_items': [{**line, 'total_money': self._money(4400)}],
                }})
            if (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(
                    kwargs['json']['checkout'], id='SQUARECHECKOUT', location_id='SQUARELOC', status='PENDING',
                )
                raise ConnectionError('Square received the checkout before the response was lost')
            if (method, path) == ('POST', '/v2/terminals/checkouts/search'):
                self.assertEqual(kwargs['json']['query']['filter']['device_id'], 'SQUARETERMINAL')
                return self._square_response({'checkouts': [{
                    **checkout, 'status': 'COMPLETED', 'payment_ids': ['SQUAREPAYMENT'],
                }]})
            if (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                return self._square_response({'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'COMPLETED',
                    'amount_money': self._money(4400), 'total_money': self._money(4400), 'source_type': 'CARD',
                    'refunded_money': self._money(1000), 'refund_ids': ['REFUND-10'],
                }})
            if (method, path) == ('GET', '/v2/refunds/REFUND-10'):
                return self._square_response({'refund': {
                    'id': 'REFUND-10', 'payment_id': 'SQUAREPAYMENT', 'location_id': 'SQUARELOC',
                    'order_id': 'REFUND-ORDER', 'status': 'COMPLETED', 'amount_money': self._money(1000),
                }})
            self.fail(f'Unexpected Square request: {method} {path}')

        def notify(event_id, event_type, kind, resource):
            body = json.dumps({
                'merchant_id': 'SQUAREMERCHANT', 'type': event_type, 'event_id': event_id,
                'data': {'type': kind, 'id': resource['id'], 'object': {kind: resource}},
            }).encode()
            signature = b64encode(hmac.digest(secret.encode(), webhook_url.encode() + body, 'sha256')).decode()
            self.assertEqual(self.url_open(webhook_path, data=body, headers={
                'Content-Type': 'application/json', 'X-Square-HmacSha256-Signature': signature,
            }).status_code, 204)

        cron = self.env.ref('pos_square_terminal.ir_cron_square_terminal_webhooks')
        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
            self._send_terminal_checkout(order, payment, prepared['reference'])
            self.assertFalse(payment.transaction_id)

            refund_notice = {
                'id': 'REFUND-10', 'payment_id': 'WRONG-HINT', 'status': 'FAILED', 'amount_money': self._money(9999),
            }
            notify('refund-new', 'refund.updated', 'refund', refund_notice)
            notify('refund-new', 'refund.updated', 'refund', refund_notice)
            cron.method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')
            self.assertEqual(payment.payment_status, 'done')
            self.assertEqual(payment.amount, 44)

            action = order.with_user(self.pos_admin).action_open_square_reconciliation()
            attempt = self.env[action['res_model']].with_user(self.pos_admin).search(action['domain'])
            self.assertEqual(attempt.verified_base, 44)
            self.assertEqual(attempt.currency_id.name, 'EUR')
            self.assertEqual(attempt.refund_ids.mapped(lambda refund: (refund.square_refund_id, refund.amount)), [
                ('REFUND-10', 10),
            ])

            notify('payment-old', 'payment.created', 'payment', {'id': 'SQUAREPAYMENT', 'status': 'APPROVED'})
            notify('refund-old', 'refund.created', 'refund', refund_notice)
            cron.method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(attempt.refund_ids.mapped('square_refund_id'), ['REFUND-10'])
            self.assertEqual(order.payment_ids, payment)
            self.assertEqual(payment.amount, 44)

        self.assertEqual([path for method, path in requests if method == 'POST'], [
            '/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts', '/v2/terminals/checkouts/search',
        ])

    def test_closed_register_recovers_missed_square_refunds_on_the_original_connection(self):
        eur = self.env.ref('base.EUR')
        eur.active = True
        self._get_main_company().currency_id = eur
        self.main_pos_config.available_pricelist_ids.currency_id = eur
        self.currency = 'EUR'
        original_connection = self.method.sudo().square_terminal_id.connection_id
        self.method.sudo().square_terminal_id.location_id.write({'currency_code': 'EUR', 'country_code': 'FR'})
        self.desk_pad.list_price = 44
        order = self._cashier_draft(amount=44)
        payment = order.payment_ids
        checkout = {}
        requests = []
        refund_ids = []
        refunded_minor = 0
        refunds = {
            'MISSED-10': ('COMPLETED', 1000, 'SQUAREPAYMENT', 'EUR'),
            'LATER-5': ('PENDING', 500, 'SQUAREPAYMENT', 'EUR'),
            'WRONG-PAYMENT': ('COMPLETED', 200, 'SOME-OTHER-PAYMENT', 'EUR'),
            'WRONG-CURRENCY': ('COMPLETED', 300, 'SQUAREPAYMENT', 'USD'),
            'OVER-CAPTURE': ('COMPLETED', 3000, 'SQUAREPAYMENT', 'EUR'),
        }

        def square_response(method, url, **kwargs):
            parsed = urlsplit(url)
            self.assertEqual(parsed.netloc, 'connect.squareup.com')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
            path = parsed.path
            requests.append((method, path))
            if method == 'POST' and path in ('/v2/orders/calculate', '/v2/orders'):
                requested = kwargs['json']['order']
                line, = requested['line_items']
                return self._square_response({'order': {
                    **requested, 'id': 'SQUAREORDER', 'total_money': self._money(4400),
                    'line_items': [{**line, 'total_money': self._money(4400)}],
                }})
            if (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(kwargs['json']['checkout'])
                return self._square_response({'checkout': {
                    **checkout, 'id': 'SQUARECHECKOUT', 'location_id': 'SQUARELOC',
                    'status': 'COMPLETED', 'payment_ids': ['SQUAREPAYMENT'],
                }})
            if (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                return self._square_response({'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'COMPLETED',
                    'amount_money': self._money(4400), 'total_money': self._money(4400), 'source_type': 'CARD',
                    'refunded_money': self._money(refunded_minor), 'refund_ids': refund_ids,
                }})
            if method == 'GET' and path.startswith('/v2/refunds/'):
                refund_id = path.rsplit('/', 1)[1]
                status, amount, payment_id, currency = refunds[refund_id]
                return self._square_response({'refund': {
                    'id': refund_id, 'payment_id': payment_id, 'location_id': 'SQUARELOC',
                    'status': status, 'amount_money': {'amount': amount, 'currency': currency},
                }})
            self.fail(f'Unexpected Square request: {method} {path}')

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
            self._send_terminal_checkout(order, payment, prepared['reference'])
            order.action_pos_order_paid()
            self.pos_session.action_pos_session_closing_control()
            self.assertEqual(self.pos_session.state, 'closed')

            replacement = self.env['pos.square.connection'].sudo().create({
                'name': 'Replacement Square account', 'company_id': order.company_id.id, 'environment': 'sandbox',
                'application_id': 'sandbox-application', 'merchant_id': 'NEWMERCHANT',
                'authorization_result': 'connected',
            })
            location = self.env['pos.square.location'].sudo().create({
                'connection_id': replacement.id, 'square_id': 'NEWLOCATION', 'name': 'New location',
                'currency_code': 'EUR', 'is_available': True,
            })
            self.method.square_terminal_id = self.env['pos.square.terminal'].sudo().create({
                'name': 'Replacement register device', 'connection_id': replacement.id, 'location_id': location.id,
                'device_id': 'NEWDEVICE', 'application_version': '6.77', 'readiness': 'ready',
            })
            original_connection.active = False

            action = order.with_user(self.pos_admin).action_open_square_reconciliation()
            attempt = self.env[action['res_model']].with_user(self.pos_admin).search(action['domain'])
            self.assertEqual(attempt.connection_id.id, original_connection.id)
            self.assertEqual(attempt.merchant_id, 'SQUAREMERCHANT')
            self.assertEqual(attempt.verified_base, 44)
            counts = {
                model: self.env[model].search_count([])
                for model in ('pos.order', 'pos.payment', 'account.move', 'account.payment', 'stock.picking', 'stock.move')
            }
            payment_before = payment.read(['amount', 'transaction_id', 'payment_status'])

            refund_ids = ['MISSED-10', 'LATER-5', 'WRONG-PAYMENT', 'WRONG-CURRENCY']
            refunded_minor = 1000
            self.env.flush_all()
            self.env.ref('pos_square_terminal.ir_cron_square_terminal_reconcile_payments').method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(attempt.refund_ids.mapped('square_refund_id'), ['MISSED-10'])
            self.assertEqual(attempt.refunded_amount, 10)
            self.assertTrue(attempt.refund_reconciliation_issue)

            refunds['LATER-5'] = ('COMPLETED', 500, 'SQUAREPAYMENT', 'EUR')
            refund_ids = ['LATER-5', 'LATER-5']
            refunded_minor = 1500
            attempt.action_check_square_payment()
            self.assertEqual(set(attempt.refund_ids.mapped('square_refund_id')), {'MISSED-10', 'LATER-5'})
            self.assertEqual(attempt.refunded_amount, 15)
            self.assertFalse(attempt.refund_reconciliation_issue)

            refund_ids = ['OVER-CAPTURE']
            refunded_minor = 4500
            attempt.action_check_square_payment()
            self.assertEqual(len(attempt.refund_ids), 3)
            self.assertEqual(attempt.refunded_amount, 45)
            self.assertIn('exceed', attempt.refund_reconciliation_issue)
            self.assertEqual(attempt.verified_base, 44)

            self.assertEqual(payment.read(['amount', 'transaction_id', 'payment_status']), payment_before)
            self.assertEqual({
                model: self.env[model].search_count([]) for model in counts
            }, counts)

        self.assertEqual([path for method, path in requests if method == 'POST'], [
            '/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts',
        ])

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
            self._send_terminal_checkout(order, payment, prepared['reference'])
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
                self._send_terminal_checkout(order, payment, prepared['reference'])
            action = order.with_user(self.pos_admin).action_open_square_reconciliation()
            manager_payments = self.env[action['res_model']].with_user(self.pos_admin)
            attempt = manager_payments.search(action['domain'])
            self.assertEqual(attempt.payment_id.id, payment.id)
            self.assertEqual(attempt.requested_amount, 12.34)
            self.assertEqual(attempt.currency_id.name, self.currency)
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
            self._send_terminal_checkout(next_order, next_payment, prepared['reference'])

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
        self.main_pos_config.tip_product_id = False
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
        provider_order = {}

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
                provider_order.update(order, id='SQUAREORDER')
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                mutations.append(path)
                checkout.update(kwargs['json']['checkout'])
                checkout.update(id='SQUARECHECKOUT', location_id='SQUARELOC', status='CANCELED',
                                cancel_reason='TIMED_OUT', payment_ids=['ABANDONEDTAP'])
                raise ConnectionError('Square accepted the checkout before the response was lost')
            elif (method, path) == ('POST', '/v2/terminals/checkouts/search'):
                searches.append(kwargs['json'])
                self.assertEqual(kwargs['json']['query']['filter']['device_id'], 'SQUARETERMINAL')
                payload = (
                    {'checkouts': [checkout]} if kwargs['json'].get('cursor') == 'page-2'
                    else {'checkouts': [{**checkout, 'reference_id': 'another-reference'}], 'cursor': 'page-2'}
                )
            elif (method, path) == ('GET', '/v2/orders/SQUAREORDER'):
                payload = {'order': provider_order}
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                payload = {'checkout': {**checkout, 'status': 'COMPLETED', 'payment_ids': ['SQUAREPAYMENT']}}
            elif (method, path) == ('GET', '/v2/payments/ABANDONEDTAP'):
                payload = {'payment': {
                    'id': 'ABANDONEDTAP', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'FAILED',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
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

    def test_expired_checkout_recovers_authentic_payment_from_original_order(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        provider_order = {}
        checkout = {}
        requests = []
        expired = False
        money = self._money(1234)

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            requests.append((method, path))
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
            if (method, path) == ('POST', '/v2/orders/calculate'):
                return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1234)})
            if (method, path) == ('POST', '/v2/orders'):
                provider_order.update(self._draft_provider_order(kwargs['json']['order'], 1234),
                                      id='SQUAREORDER', version=1)
                return self._square_response({'order': provider_order})
            if (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(kwargs['json']['checkout'], id='SQUARECHECKOUT', location_id='SQUARELOC',
                                status='CANCELED', cancel_reason='TIMED_OUT')
                return self._square_response({'checkout': checkout})
            if (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                if not expired:
                    return self._square_response({'checkout': checkout})
                response = self._square_response({'errors': [{'code': 'NOT_FOUND'}]})
                response.status_code = 404
                return response
            if (method, path) == ('GET', '/v2/orders/SQUAREORDER'):
                return self._square_response({'order': provider_order})
            if (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                return self._square_response({'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'COMPLETED',
                    'terminal_checkout_id': 'SQUARECHECKOUT',
                    'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }})
            self.fail(f'Unexpected Square request: {method} {path}')

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            reference = prepared['reference']
            self._send_terminal_checkout(order, payment, reference)
            self.assertEqual(order.square_check_payment(order.uuid, payment.uuid)['state'], 'timed_out')
            self.assertEqual(order.square_retry_payment(order.uuid, payment.uuid, reference)['state'], 'timed_out')
            with self.assertRaises(UserError), self.cr.savepoint():
                payment.unlink()
            expired = True
            self.assertIn(order.square_check_payment(order.uuid, payment.uuid)['state'], ('timed_out', 'uncertain'))
            provider_order.update(state='COMPLETED', tenders=[{'id': 'SQUAREPAYMENT', 'type': 'CARD'}])
            result = order.square_check_payment(order.uuid, payment.uuid)
            self.assertEqual(result['state'], 'verified')
            self.assertEqual(result['reference'], reference)
            self._send_terminal_checkout(order, payment, reference)

        self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')
        self.assertEqual(requests.count(('POST', '/v2/terminals/checkouts')), 1)
        self.assertEqual(requests.count(('POST', '/v2/orders')), 1)

    def test_ambiguous_dispatch_requires_confirmed_original_order_cancellation_for_cash(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        provider_order = {}
        checkout_requests = []

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            if (method, path) == ('POST', '/v2/orders/calculate'):
                return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1234)})
            if (method, path) == ('POST', '/v2/orders'):
                provider_order.update(self._draft_provider_order(kwargs['json']['order'], 1234),
                                      id='SQUAREORDER', version=1)
                return self._square_response({'order': provider_order})
            if (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout_requests.append(kwargs['json'])
                raise ConnectionError('Lost the response while the Square submission was in flight')
            if (method, path) == ('POST', '/v2/terminals/checkouts/search'):
                return self._square_response({'checkouts': []})
            if (method, path) == ('GET', '/v2/orders/SQUAREORDER'):
                return self._square_response({'order': provider_order})
            if (method, path) == ('GET', '/v2/payments/RELEASED'):
                return self._square_response({'payment': {
                    'id': 'RELEASED', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout_requests[0]['checkout']['reference_id'], 'status': 'CANCELED',
                    'terminal_checkout_id': 'EXPIREDCHECKOUT',
                    'amount_money': self._money(1234), 'total_money': self._money(1234), 'source_type': 'CARD',
                }})
            self.fail(f'Unexpected Square request: {method} {path}')

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            reference = order.square_prepare_linked_order(order.uuid, payment.uuid)['reference']
            self.assertEqual(self._send_terminal_checkout(order, payment, reference)['state'], 'uncertain')
            self.assertEqual(order.square_cancel_payment(order.uuid, payment.uuid, reference)['state'], 'canceling')
            self.assertEqual(order.square_retry_payment(order.uuid, payment.uuid, reference)['state'], 'canceling')
            with self.assertRaises(UserError), self.cr.savepoint():
                payment.unlink()

            original_reference = provider_order['reference_id']
            provider_order.update(state='CANCELED', reference_id='WRONGSALE')
            self.assertEqual(order.square_check_payment(order.uuid, payment.uuid)['state'], 'canceling')
            provider_order.update(reference_id=original_reference, tenders=[{'payment_id': 'RELEASED', 'type': 'CARD'}])
            self.assertEqual(order.square_check_payment(order.uuid, payment.uuid)['state'], 'cancelled')
            payment.unlink()
            order.write({'payment_ids': [Command.create({
                'uuid': str(uuid4()), 'payment_method_id': self.cash_method.id, 'amount': 12.34,
            })]})
            order.action_pos_order_paid()

        self.assertEqual(order.state, 'paid')
        self.assertEqual(len(checkout_requests), 1)
        self.assertEqual(order.payment_ids.payment_method_id, self.cash_method)

    def test_interrupted_browser_before_dispatch_can_cancel_without_a_checkout(self):
        order = self._cashier_draft()
        payment = order.payment_ids
        provider_order = {}

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            if (method, path) == ('POST', '/v2/orders/calculate'):
                return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1234)})
            if (method, path) == ('POST', '/v2/orders'):
                provider_order.update(self._draft_provider_order(kwargs['json']['order'], 1234),
                                      id='SQUAREORDER', version=1)
            elif (method, path) == ('POST', '/v2/terminals/checkouts/search'):
                return self._square_response({'checkouts': []})
            elif (method, path) == ('PUT', '/v2/orders/SQUAREORDER'):
                self.assertEqual(kwargs['json']['order'], {
                    'location_id': 'SQUARELOC', 'version': 1, 'state': 'CANCELED',
                })
                provider_order.update(state='CANCELED', version=2)
            elif (method, path) != ('GET', '/v2/orders/SQUAREORDER'):
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response({'order': provider_order})

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            order.square_prepare_payment(order.uuid, payment.uuid)
            prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
            order.square_mark_dispatching(order.uuid, payment.uuid, prepared['reference'])
            result = order.square_check_payment(order.uuid, payment.uuid)
            self.assertEqual(result['state'], 'ready')
            self.assertEqual(result['reference'], prepared['reference'])
            cancelled = order.square_cancel_payment(order.uuid, payment.uuid, prepared['reference'])
            self.assertEqual(cancelled['state'], 'cancelled')
            payment.unlink()

        self.assertEqual(provider_order['state'], 'CANCELED')
        self.assertFalse(order.payment_ids)

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
            self._send_terminal_checkout(order, payment, prepared['reference'])

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
            self._send_terminal_checkout(second_order, second_payment, prepared['reference'])
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
            # A status read before Send leaves the original reservation ready.
            result = order.square_check_payment(order.uuid, payment.uuid)
            self.assertEqual(result['state'], 'ready')
            self.assertFalse(checkouts)
            self._send_terminal_checkout(order, payment, prepared['reference'])
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
            self._send_terminal_checkout(order, payment, retry['reference'])
            self._send_terminal_checkout(order, payment, retry['reference'])
            self.assertEqual(order.square_check_payment(order.uuid, payment.uuid)['state'], 'verified')

        self.assertEqual(requests.count(('POST', '/v2/orders')), 1)
        self.assertEqual(requests.count(('POST', '/v2/orders/calculate')), 1)
        self.assertEqual(len(checkouts), 2)
        self.assertEqual(len({key for key, _checkout in checkouts}), 2)
        self.assertEqual(order.payment_ids, payment)
        self.assertEqual(payment.transaction_id, 'CAPTURED-TAP')

    def test_split_cards_preserve_first_approval_and_recover_lost_settlement_after_reload(self):
        provider_order = {}
        checkouts = []
        payments = {}
        mutations = []
        settlement = []
        lose_readback = False
        decline_reads = 0

        def square_response(method, url, **kwargs):
            nonlocal lose_readback, decline_reads
            path = urlsplit(url).path
            if method != 'GET':
                mutations.append(path)
            if (method, path) == ('POST', '/v2/orders/calculate'):
                payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                provider_order.update(self._draft_provider_order(kwargs['json']['order'], 1234),
                                      id='SPLITORDER', version=1)
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                request = kwargs['json']
                number = len(checkouts) + 1
                expected_amount = 500 if number == 1 else 734
                checkout = {
                    **request['checkout'], 'id': f'CHECKOUT-{number}', 'location_id': 'SQUARELOC',
                    'status': 'PENDING',
                }
                self.assertEqual(checkout['order_id'], 'SPLITORDER')
                self.assertEqual(checkout['payment_options'], {'autocomplete': False})
                self.assertEqual(checkout['amount_money'], self._money(expected_amount))
                self.assertNotIn(request['idempotency_key'], [key for key, _checkout in checkouts])
                if number > 1:
                    self.assertEqual(payments['TENDER-1']['status'], 'APPROVED')
                checkouts.append((request['idempotency_key'], checkout))
                payments[f'TENDER-{number}'] = {
                    'id': f'TENDER-{number}', 'order_id': 'SPLITORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'FAILED' if number == 2 else 'APPROVED',
                    'amount_money': self._money(expected_amount), 'total_money': self._money(expected_amount),
                    'source_type': 'CARD',
                }
                payload = {'checkout': checkout}
            elif method == 'GET' and path.startswith('/v2/terminals/checkouts/CHECKOUT-'):
                number = int(path.rsplit('-', 1)[1])
                checkout = {**checkouts[number - 1][1], 'status': 'COMPLETED', 'payment_ids': [f'TENDER-{number}']}
                if number == 2:
                    decline_reads += 1
                    if decline_reads == 1:
                        checkout['status'] = 'IN_PROGRESS'
                    else:
                        checkout.update(status='CANCELED', cancel_reason='BUYER_CANCELED')
                payload = {'checkout': checkout}
            elif method == 'GET' and path.startswith('/v2/payments/'):
                payload = {'payment': payments[path.rsplit('/', 1)[1]]}
            elif (method, path) == ('GET', '/v2/orders/SPLITORDER'):
                if lose_readback:
                    lose_readback = False
                    raise ConnectionError('Square is temporarily unavailable after settlement')
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/orders/SPLITORDER/pay'):
                request = kwargs['json']
                self.assertEqual(request['payment_ids'], ['TENDER-1', 'TENDER-3'])
                self.assertEqual(request['order_version'], 1)
                self.assertTrue(request['idempotency_key'])
                native_sale = self.pos_session.order_ids
                self.assertEqual(native_sale.state, 'draft')
                self.assertFalse(native_sale.payment_ids.filtered('transaction_id'))
                self.assertFalse(settlement)
                settlement.append(request)
                tenders = []
                for payment_id in request['payment_ids']:
                    payment = payments[payment_id]
                    payment['status'] = 'COMPLETED'
                    tenders.append({
                        'payment_id': payment_id, 'location_id': 'SQUARELOC', 'type': 'CARD',
                        'amount_money': payment['total_money'],
                    })
                provider_order.update(state='COMPLETED', version=2, tenders=tenders)
                lose_readback = True
                raise ConnectionError('Square settled the order before the response was lost')
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour('SquareTerminalSplitRecovery')

        self.assertEqual(mutations, [
            '/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts',
            '/v2/terminals/checkouts', '/v2/terminals/checkouts', '/v2/orders/SPLITORDER/pay',
        ])
        self.assertEqual(payments['TENDER-2']['status'], 'FAILED')
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(len(order), 1)
        self.assertEqual(order.state, 'paid')
        self.assertEqual(order.lines.product_id, self.desk_pad)
        self.assertEqual(order.amount_total, 12.34)
        self.assertEqual({(line.amount, line.payment_status, line.transaction_id) for line in order.payment_ids}, {
            (5.00, 'done', 'TENDER-1'), (7.34, 'done', 'TENDER-3'),
        })

    def test_split_device_tips_survive_lost_settlement_and_reload(self):
        eur = self.env.ref('base.EUR')
        eur.active = True
        self.main_pos_config.journal_id.currency_id = eur
        self.method.sudo().square_terminal_id.location_id.write({'currency_code': 'EUR', 'country_code': 'FR'})
        self.currency = self.main_pos_config.currency_id.name
        self.assertEqual(self.currency, 'EUR')
        self.desk_pad.list_price = 100
        tip_product = self._configure_device_tip(odoo_tipping=True)
        provider_order = {}
        checkouts = {}
        payments = {}
        mutations = []
        lose_readback = False

        def square_response(method, url, **kwargs):
            nonlocal lose_readback
            path = urlsplit(url).path
            if method != 'GET':
                mutations.append(path)
            if (method, path) == ('POST', '/v2/orders/calculate'):
                order = kwargs['json']['order']
                self.assertEqual(len(order['line_items']), 1)
                payload = {'order': self._draft_provider_order(order, 10000, line_total=10000)}
            elif (method, path) == ('POST', '/v2/orders'):
                provider_order.update(self._draft_provider_order(kwargs['json']['order'], 10000, line_total=10000),
                                      id='TIPPEDORDER', version=1)
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                number = len(checkouts) + 1
                base, tip, gross = ((4000, 300, 4300), (6000, 200, 6200))[number - 1]
                checkout = kwargs['json']['checkout']
                self.assertEqual(checkout['amount_money'], self._money(base))
                self.assertEqual(checkout['order_id'], 'TIPPEDORDER')
                self.assertEqual(checkout['payment_options'], {'autocomplete': False})
                self.assertTrue(checkout['device_options']['tip_settings']['allow_tipping'])
                checkout_id, payment_id = f'CHECKOUT-{number}', f'TIPPED-CARD-{number}'
                checkouts[checkout_id] = {
                    **checkout, 'id': checkout_id, 'location_id': 'SQUARELOC',
                    'status': 'COMPLETED', 'payment_ids': [payment_id],
                }
                payments[payment_id] = {
                    'id': payment_id, 'order_id': 'TIPPEDORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'terminal_checkout_id': checkout_id,
                    'status': 'APPROVED', 'amount_money': self._money(base),
                    'tip_money': self._money(tip), 'total_money': self._money(gross), 'source_type': 'CARD',
                }
                payload = {'checkout': {**checkouts[checkout_id], 'status': 'PENDING'}}
            elif method == 'GET' and path.startswith('/v2/terminals/checkouts/'):
                payload = {'checkout': checkouts[path.rsplit('/', 1)[1]]}
            elif method == 'GET' and path.startswith('/v2/payments/'):
                payload = {'payment': payments[path.rsplit('/', 1)[1]]}
            elif (method, path) == ('GET', '/v2/orders/TIPPEDORDER'):
                if lose_readback:
                    lose_readback = False
                    raise ConnectionError('Square settlement read unavailable')
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/orders/TIPPEDORDER/pay'):
                request = kwargs['json']
                self.assertEqual(request['payment_ids'], ['TIPPED-CARD-1', 'TIPPED-CARD-2'])
                self.assertEqual(request['order_version'], 1)
                self.assertTrue(request['idempotency_key'])
                tenders = []
                for payment in payments.values():
                    payment['status'] = 'COMPLETED'
                    tenders.append({
                        'payment_id': payment['id'], 'location_id': 'SQUARELOC', 'type': 'CARD',
                        'amount_money': payment['total_money'], 'tip_money': payment['tip_money'],
                    })
                provider_order.update(
                    state='COMPLETED', version=2, total_money=self._money(10500),
                    total_tip_money=self._money(500), tenders=tenders,
                )
                lose_readback = True
                raise ConnectionError('Square settled the original sale before losing the response')
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour('SquareTerminalSplitDeviceTips')
            self.env.invalidate_all()
            order = self.pos_session.order_ids
            original_tip_line = order.lines.filtered(lambda line: line.product_id == tip_product)
            attempts = self.env['pos.square.payment.attempt'].with_user(self.pos_admin).search([
                ('order_id', '=', order.id),
            ])
            for attempt in attempts:
                attempt.action_check_square_payment()
                self.assertFalse(attempt.reconciliation_error)
            # Reconciliation can see later conflicting evidence, which must preserve
            # the original captured payment and POS accounting for the manager.
            first = attempts.filtered(lambda line: line.square_payment_id == 'TIPPED-CARD-1')
            payments['TIPPED-CARD-1'].update(tip_money=self._money(400), total_money=self._money(4400))
            first.action_check_square_payment()
            self.assertIn('total or tip differs', first.reconciliation_discrepancy)

        self.assertEqual(mutations, [
            '/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts',
            '/v2/terminals/checkouts', '/v2/orders/TIPPEDORDER/pay',
        ])
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(len(order), 1)
        self.assertEqual(order.state, 'paid')
        self.assertEqual((order.amount_total, order.amount_paid, order.tip_amount), (105, 105, 5))
        tip_line = order.lines.filtered(lambda line: line.product_id == tip_product)
        self.assertEqual(len(tip_line), 1)
        self.assertEqual(tip_line, original_tip_line)
        self.assertEqual((tip_line.qty, tip_line.price_subtotal_incl), (1, 5))
        self.assertEqual({(line.amount, line.transaction_id) for line in order.payment_ids}, {
            (43, 'TIPPED-CARD-1'), (62, 'TIPPED-CARD-2'),
        })
        attempts = self.env['pos.square.payment.attempt'].sudo().search([('order_id', '=', order.id)])
        self.assertEqual({(line.square_payment_id, line.verified_base, line.verified_tip) for line in attempts}, {
            ('TIPPED-CARD-1', 40, 3), ('TIPPED-CARD-2', 60, 2),
        })
        with self.assertRaisesRegex(UserError, 'original goods are reserved'):
            tip_line.with_user(self.pos_user).write({'price_unit': 12})
        with self.assertRaisesRegex(UserError, 'original tender is reserved'):
            order.payment_ids[:1].with_user(self.pos_user).write({'amount': 12})

    def test_device_tip_missing_product_stops_before_dispatch(self):
        self.main_pos_config.write({'square_terminal_tipping': True, 'tip_product_id': False})
        with patch('odoo.addons.pos_square_terminal.square_api.request') as provider:
            self.start_pos_tour('SquareTerminalDeviceTipMissingProduct')
        provider.assert_not_called()
        self.env.invalidate_all()
        self.assertEqual(self.pos_session.order_ids.state, 'draft')
        self.assertFalse(self.pos_session.order_ids.payment_ids.transaction_id)

    def test_device_tip_zero_leaves_receipt_unchanged(self):
        self._configure_device_tip()
        self._run_priced_checkout('SquareTerminalDeviceTipZero', ((1234, 0, 0),), lambda order: None, device_tip_minor=0)
        order = self.pos_session.order_ids
        self.assertEqual(order.lines.product_id, self.desk_pad)
        self.assertEqual((order.amount_total, order.amount_paid, order.tip_amount), (12.34, 12.34, 0))

    def test_device_tip_on_one_card_updates_native_receipt(self):
        tip_product = self._configure_device_tip()
        self._run_priced_checkout(
            'SquareTerminalSingleDeviceTip', ((1234, 0, 0),), lambda order: None, device_tip_minor=330,
        )
        order = self.pos_session.order_ids
        tip_line = order.lines.filtered(lambda line: line.product_id == tip_product)
        self.assertEqual((len(tip_line), tip_line.price_subtotal_incl), (1, 3.30))
        self.assertEqual((order.amount_total, order.amount_paid, order.tip_amount), (15.64, 15.64, 3.30))

    def _configure_device_tip(self, *, odoo_tipping=False):
        tip_product = self.env['product.product'].create({
            'name': 'Terminal Gratuity', 'type': 'service', 'taxes_id': [Command.clear()],
        })
        settings = self.env['res.config.settings'].create({
            'pos_config_id': self.main_pos_config.id,
            'pos_square_terminal_tipping': True,
            'pos_iface_tipproduct': odoo_tipping,
            'pos_tip_product_id': tip_product.id,
        })
        self.assertEqual(settings.pos_tip_product_id, tip_product)
        return tip_product

    def test_abandon_split_releases_each_approval_before_replacement_cash(self):
        self._assert_abandon_split('SquareTerminalAbandonSplit', 400)

    def test_abandon_fully_authorized_split_after_interrupted_settlement(self):
        self._assert_abandon_split('SquareTerminalAbandonInterruptedSplit', 934)

    def test_capture_racing_abandonment_stays_on_original_split(self):
        self._assert_abandon_split('SquareTerminalCaptureDuringSplitAbandonment', 934, captured=True)

    def _assert_abandon_split(self, tour, second_amount, *, captured=False):
        provider_order = {}
        checkouts = {}
        payments = {}
        mutations = []
        release_checks = []
        lose_release_read = False
        fully_authorized = second_amount == 934

        def square_response(method, url, **kwargs):
            nonlocal lose_release_read
            path = urlsplit(url).path
            if method != 'GET':
                mutations.append(path)
            if (method, path) == ('POST', '/v2/orders/calculate'):
                payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                provider_order.update(self._draft_provider_order(kwargs['json']['order'], 1234),
                                      id='SPLITORDER', version=1)
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                number = len(checkouts) + 1
                request = kwargs['json']['checkout']
                self.assertEqual(request['order_id'], 'SPLITORDER')
                self.assertEqual(request['amount_money'], self._money(300 if number == 1 else second_amount))
                self.assertFalse(request['payment_options']['autocomplete'])
                checkout = {
                    **request, 'id': f'CHECKOUT-{number}', 'location_id': 'SQUARELOC',
                    'status': 'COMPLETED', 'payment_ids': [f'TENDER-{number}'],
                }
                checkouts[checkout['id']] = checkout
                payments[f'TENDER-{number}'] = {
                    'id': f'TENDER-{number}', 'order_id': 'SPLITORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'APPROVED',
                    'amount_money': request['amount_money'], 'total_money': request['amount_money'],
                    'source_type': 'CARD',
                }
                payload = {'checkout': checkout}
            elif method == 'GET' and path.startswith('/v2/terminals/checkouts/'):
                payload = {'checkout': checkouts[path.rsplit('/', 1)[1]]}
            elif method == 'GET' and path.startswith('/v2/payments/'):
                payment_id = path.rsplit('/', 1)[1]
                if payment_id == 'TENDER-2' and lose_release_read:
                    lose_release_read = False
                    raise ConnectionError('Square accepted the release but the first readback was lost')
                payment = payments[payment_id]
                if payment['status'] == 'CANCELED':
                    release_checks.append(payment_id)
                payload = {'payment': payment}
            elif method == 'POST' and path.endswith('/cancel') and path.startswith('/v2/payments/'):
                payment_id = path.split('/')[-2]
                self.assertEqual(payments[payment_id]['status'], 'APPROVED')
                if captured:
                    self.assertEqual(payment_id, 'TENDER-1')
                    for payment in payments.values():
                        payment['status'] = 'COMPLETED'
                    provider_order.update(state='COMPLETED', version=2, tenders=[
                        {'type': 'CARD', 'payment_id': payment['id'], 'amount_money': payment['total_money']}
                        for payment in payments.values()
                    ])
                    raise ConnectionError('The original settlement completed before Square could release the card')
                payments[payment_id]['status'] = 'CANCELED'
                if payment_id == 'TENDER-2':
                    lose_release_read = True
                    raise ConnectionError('Square accepted the release but the response was lost')
                payload = {'payment': payments[payment_id]}
            elif (method, path) == ('GET', '/v2/orders/SPLITORDER'):
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/orders/SPLITORDER/pay'):
                self.assertTrue(fully_authorized)
                self.assertEqual(kwargs['json']['payment_ids'], ['TENDER-1', 'TENDER-2'])
                raise ConnectionError('The register lost contact before Square could apply the settlement')
            elif (method, path) == ('PUT', '/v2/orders/SPLITORDER'):
                self.assertEqual(set(release_checks), {'TENDER-1', 'TENDER-2'})
                self.assertEqual(kwargs['json']['order']['state'], 'CANCELED')
                provider_order.update(state='CANCELED', version=2)
                payload = {'order': provider_order}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour(tour)

        expected = ['/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts', '/v2/terminals/checkouts']
        if fully_authorized:
            expected.append('/v2/orders/SPLITORDER/pay')
        expected.append('/v2/payments/TENDER-1/cancel')
        if not captured:
            expected.extend(['/v2/payments/TENDER-2/cancel', '/v2/orders/SPLITORDER'])
        self.assertEqual(mutations, expected)
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(len(order), 1)
        self.assertEqual(order.state, 'paid')
        if captured:
            self.assertEqual(order.payment_ids.payment_method_id, self.method)
            self.assertEqual({(line.transaction_id, line.amount) for line in order.payment_ids}, {
                ('TENDER-1', 3), ('TENDER-2', 9.34),
            })
        else:
            self.assertEqual(order.payment_ids.payment_method_id, self.cash_method)
            self.assertEqual(order.payment_ids.amount, 12.34)
            self.assertFalse(order.payment_ids.transaction_id)

    def test_expired_split_settlement_keeps_original_intent_and_can_be_abandoned(self):
        order = self._cashier_draft()
        first = order.payment_ids
        first.amount = 5
        provider_order = {}
        payments = {}
        mutations = []
        settlement = []
        unavailable_order_reads = 0

        def square_response(method, url, **kwargs):
            nonlocal unavailable_order_reads
            path = urlsplit(url).path
            if method != 'GET':
                mutations.append(path)
            if (method, path) == ('POST', '/v2/orders/calculate'):
                payload = {'order': self._draft_provider_order(kwargs['json']['order'], 1234)}
            elif (method, path) == ('POST', '/v2/orders'):
                provider_order.update(self._draft_provider_order(kwargs['json']['order'], 1234),
                                      id='SPLITORDER', version=1)
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                number = len(payments) + 1
                request = kwargs['json']['checkout']
                payment_id = f'TENDER-{number}'
                checkout = {
                    **request, 'id': f'CHECKOUT-{number}', 'location_id': 'SQUARELOC',
                    'status': 'COMPLETED', 'payment_ids': [payment_id],
                }
                payments[payment_id] = {
                    'id': payment_id, 'order_id': 'SPLITORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'status': 'APPROVED',
                    'amount_money': request['amount_money'], 'total_money': request['amount_money'],
                    'source_type': 'CARD',
                }
                payload = {'checkout': checkout}
            elif method == 'GET' and path.startswith('/v2/payments/'):
                payload = {'payment': payments[path.rsplit('/', 1)[1]]}
            elif (method, path) == ('GET', '/v2/orders/SPLITORDER'):
                if unavailable_order_reads:
                    unavailable_order_reads -= 1
                    raise ConnectionError('Square order status is unavailable')
                payload = {'order': provider_order}
            elif (method, path) == ('POST', '/v2/orders/SPLITORDER/pay'):
                settlement.append(kwargs['json'])
                self.assertEqual(settlement[0]['payment_ids'], ['TENDER-1', 'TENDER-2'])
                self.assertTrue(settlement[0]['idempotency_key'])
                payments['TENDER-1']['status'] = 'CANCELED'
                unavailable_order_reads = 2
                raise ConnectionError('A Square authorization expired before settlement')
            elif (method, path) == ('POST', '/v2/payments/TENDER-2/cancel'):
                payments['TENDER-2']['status'] = 'CANCELED'
                payload = {'payment': payments['TENDER-2']}
            elif (method, path) == ('PUT', '/v2/orders/SPLITORDER'):
                self.assertTrue(all(payment['status'] == 'CANCELED' for payment in payments.values()))
                provider_order.update(state='CANCELED', version=2)
                payload = {'order': provider_order}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        def send(line):
            prepared = order.square_prepare_payment(order.uuid, line.uuid)
            if prepared['state'] == 'prepared':
                prepared = order.square_prepare_linked_order(order.uuid, line.uuid)
            order.square_mark_dispatching(order.uuid, line.uuid, prepared['reference'])
            return self._send_terminal_checkout(order, line, prepared['reference'])

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.assertEqual(send(first)['state'], 'authorized')
            order.write({'payment_ids': [Command.create({
                'uuid': str(uuid4()), 'payment_method_id': self.method.id, 'amount': 7.34, 'payment_status': 'waiting',
            })]})
            second = order.payment_ids - first
            approved = send(second)
            self.assertEqual(approved['state'], 'authorized')
            key = (order.uuid, second.uuid, approved['reference'])
            self.assertEqual(order.square_prepare_settlement(*key)['state'], 'settling')
            self.assertFalse(settlement)
            self.assertEqual(order.square_settle_payment(*key)['state'], 'settling')
            self.assertEqual(order.square_cancel_payment(*key)['state'], 'settling')
            self.assertNotIn('/v2/payments/TENDER-2/cancel', mutations)
            self.assertEqual(order.square_settle_payment(*key)['state'], 'settlement_blocked')
            self.assertEqual(order.square_cancel_payment(*key)['state'], 'cancelled')

        self.assertEqual(mutations, [
            '/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts', '/v2/terminals/checkouts',
            '/v2/orders/SPLITORDER/pay', '/v2/payments/TENDER-2/cancel', '/v2/orders/SPLITORDER',
        ])
        self.assertEqual(len(settlement), 1)
        self.assertEqual(order.state, 'draft')
        self.assertFalse(order.payment_ids.filtered('transaction_id'))
        order.payment_ids.unlink()

    @staticmethod
    def _square_response(payload):
        response = Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        return response

    def test_card_captured_during_cancel_stays_on_original_sale(self):
        self.main_pos_config.auto_validate_terminal_payment = False
        money = {'amount': 1234, 'currency': self.currency}
        checkout = {}
        mutations = []

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            if (method, path) == ('POST', '/v2/orders/calculate'):
                return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1234)})
            if method != 'GET':
                mutations.append((method, path))
            if (method, path) == ('POST', '/v2/orders'):
                payload = {'order': {**self._draft_provider_order(kwargs['json']['order'], 1234), 'id': 'SQUAREORDER'}}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(kwargs['json']['checkout'], id='SQUARECHECKOUT', location_id='SQUARELOC', status='IN_PROGRESS')
                payload = {'checkout': checkout}
            elif (method, path) == ('POST', '/v2/terminals/checkouts/SQUARECHECKOUT/cancel'):
                checkout.update(status='COMPLETED', payment_ids=['SQUAREPAYMENT'])
                # The cancellation acknowledgment does not show the winning card capture.
                payload = {'checkout': {**checkout, 'status': 'CANCEL_REQUESTED', 'payment_ids': []}}
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                payload = {'checkout': checkout}
            elif (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                payload = {'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'terminal_checkout_id': 'SQUARECHECKOUT', 'reference_id': checkout['reference_id'],
                    'status': 'COMPLETED', 'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour('SquareTerminalCancelRacesPayment')

        self.assertEqual(mutations, [
            ('POST', '/v2/orders'),
            ('POST', '/v2/terminals/checkouts'),
            ('POST', '/v2/terminals/checkouts/SQUARECHECKOUT/cancel'),
        ])
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.state, 'paid')
        self.assertEqual(order.payment_ids.payment_method_id, self.method)
        self.assertEqual(order.payment_ids.transaction_id, 'SQUAREPAYMENT')
        self.assertEqual(order.payment_ids.amount, 12.34)
        self.assertEqual(order.lines.product_id, self.desk_pad)

    def test_lost_cancellation_replies_keep_sale_locked_until_verified_then_finish_cash(self):
        money = {'amount': 1234, 'currency': self.currency}
        checkout = {}
        square_order = {}
        mutations = []
        payment_status = 'APPROVED'
        lose_order_read = False

        def square_response(method, url, **kwargs):
            nonlocal lose_order_read, payment_status
            path = urlsplit(url).path
            if (method, path) == ('POST', '/v2/orders/calculate'):
                return self._square_response({'order': self._draft_provider_order(kwargs['json']['order'], 1234)})
            if method != 'GET':
                mutations.append((method, path))
            if (method, path) == ('POST', '/v2/orders'):
                square_order.update(self._draft_provider_order(kwargs['json']['order'], 1234), id='SQUAREORDER', version=1)
                payload = {'order': square_order}
            elif (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(kwargs['json']['checkout'], id='SQUARECHECKOUT', location_id='SQUARELOC', status='PENDING')
                payload = {'checkout': checkout}
            elif (method, path) == ('POST', '/v2/terminals/checkouts/SQUARECHECKOUT/cancel'):
                checkout.update(status='CANCELED', cancel_reason='SELLER_CANCELED', payment_ids=['SQUAREAPPROVAL'])
                square_order['tenders'] = [{'payment_id': 'SQUAREAPPROVAL'}]
                raise ConnectionError('Lost checkout cancellation response')
            elif (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                payload = {'checkout': checkout}
            elif (method, path) == ('GET', '/v2/payments/SQUAREAPPROVAL'):
                payload = {'payment': {
                    'id': 'SQUAREAPPROVAL', 'order_id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'terminal_checkout_id': 'SQUARECHECKOUT', 'reference_id': checkout['reference_id'],
                    'status': payment_status, 'amount_money': money, 'total_money': money, 'source_type': 'CARD',
                }}
            elif (method, path) == ('POST', '/v2/payments/SQUAREAPPROVAL/cancel'):
                payment_status = 'CANCELED'
                raise ConnectionError('Lost authorization cancellation response')
            elif (method, path) == ('PUT', '/v2/orders/SQUAREORDER'):
                request = kwargs['json']
                self.assertEqual(request['order']['version'], 1)
                self.assertEqual(request['order']['state'], 'CANCELED')
                self.assertTrue(request['idempotency_key'])
                self.assertEqual(checkout['status'], 'CANCELED')
                self.assertEqual(payment_status, 'CANCELED')
                square_order.update(state='CANCELED', version=2)
                lose_order_read = True
                raise ConnectionError('Lost order cancellation response')
            elif (method, path) == ('GET', '/v2/orders/SQUAREORDER'):
                if lose_order_read:
                    lose_order_read = False
                    raise ConnectionError('Unable to read Square order after cancellation')
                payload = {'order': square_order}
            else:
                self.fail(f'Unexpected Square request: {method} {path}')
            return self._square_response(payload)

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour('SquareTerminalLostCancellation')

        self.assertEqual(mutations, [
            ('POST', '/v2/orders'),
            ('POST', '/v2/terminals/checkouts'),
            ('POST', '/v2/terminals/checkouts/SQUARECHECKOUT/cancel'),
            ('POST', '/v2/payments/SQUAREAPPROVAL/cancel'),
            ('PUT', '/v2/orders/SQUAREORDER'),
        ])
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(len(order), 1)
        self.assertEqual(order.state, 'paid')
        self.assertEqual(order.amount_total, 15.53)
        self.assertEqual(order.lines.product_id, self.desk_pad | self.monitor_stand)
        self.assertEqual(order.payment_ids.payment_method_id, self.cash_method)
        self.assertEqual(order.payment_ids.amount, 15.53)
        attempt = self.env['pos.square.payment.attempt'].sudo().search([('order_id', '=', order.id)])
        self.assertEqual(attempt.square_order_link_id.square_order_id, 'SQUAREORDER')
        self.assertEqual(attempt.square_checkout_id, 'SQUARECHECKOUT')
        self.assertEqual(attempt.square_payment_id, 'SQUAREAPPROVAL')
        self.assertTrue(attempt.payment_uuid)
        self.assertNotEqual(attempt.payment_uuid, order.payment_ids.uuid)

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

    def test_manager_price_control_allows_native_manual_price(self):
        self.main_pos_config.restrict_price_control = True
        self.pos_user.group_ids = [Command.link(self.env.ref('point_of_sale.group_pos_manager').id)]

        def check_order(order):
            line, = order['line_items']
            self.assertEqual(line['name'], 'Desk Pad')
            self.assertEqual(line['base_price_money'], self._money(1100))

        self._run_priced_checkout('SquareTerminalManualPrice', ((1100, 0, 0),), check_order)
        line = self.pos_session.order_ids.lines
        self.assertEqual((line.price_type, line.price_unit), ('manual', 11))

    def test_restricted_cashier_cannot_submit_manual_price(self):
        self.main_pos_config.restrict_price_control = True
        order = self._cashier_draft(amount=11)
        order.lines.price_type = 'manual'
        with patch('odoo.addons.pos_square_terminal.square_api.request') as provider:
            with self.assertRaisesRegex(UserError, 'cannot be priced'), self.cr.savepoint():
                order.square_prepare_payment(order.uuid, order.payment_ids.uuid)
            provider.assert_not_called()

    def test_mixed_combo_recovers_original_cash_and_card_after_lost_settlement(self):
        euro = self.env.ref('base.EUR')
        euro.active = True
        self.main_pos_config.journal_id.currency_id = euro
        self.method.square_terminal_id.location_id.sudo().currency_code = 'EUR'
        self.currency = 'EUR'
        self.assertEqual(self.main_pos_config.currency_id, euro)
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

        self._run_mixed_checkout(
            'SquareTerminalMixedComboRecovery', ((2100, 350, 0), (3900, 0, 200)),
            2000, (4000,), check_order=check_order, lose_settlement=True,
        )
        order = self.pos_session.order_ids
        self.assertEqual(order.lines.product_id, combo | self.desk_pad | self.monitor_stand | self.whiteboard_pen)
        parent = order.lines.filtered(lambda line: line.product_id == combo)
        self.assertEqual(parent.combo_line_ids.product_id, self.desk_pad | self.monitor_stand)
        self.assertEqual(order.amount_total, 60.0)
        self.assertEqual(order.amount_tax, 3.50)

    def test_cash_after_two_tipped_card_authorizations_recovers_recording_and_closes_accounting(self):
        self.desk_pad.lst_price = 12
        tip_product = self._configure_device_tip(odoo_tipping=True)
        other_cash = (self.main_pos_config.payment_method_ids - self.cash_method).filtered('is_cash_count')
        self.main_pos_config.with_context(bypass_payment_method_ids_forbidden_change=True).write({
            'payment_method_ids': [Command.unlink(method.id) for method in other_cash],
        })
        cash_journal = self.cash_method.journal_id
        cash_account = cash_journal.default_account_id
        self._run_mixed_checkout(
            'SquareTerminalCardsThenCash', ((1200, 0, 0),), 300, (500, 400),
            card_tips=(50, 25), lose_cash=True,
        )
        session = self.pos_session.with_user(self.pos_user)
        order = session.order_ids
        tip_line = order.lines.filtered(lambda line: line.product_id == tip_product)
        self.assertEqual(tip_line.price_subtotal_incl, 0.75)
        self.assertEqual(order.amount_total, 12.75)
        closing = session.get_closing_control_data()
        self.assertEqual(closing['default_cash_details']['payment_amount'], 3)
        self.assertEqual(
            [(method['amount'], method['number']) for method in closing['non_cash_payment_methods']
             if method['id'] == self.method.id], [(9.75, 2)],
        )
        self.assertEqual(session.post_closing_cash_details(3), {'successful': True})
        self.assertEqual(session.close_session_from_ui(), {'successful': True})
        self.assertEqual(session.state, 'closed')
        statement_lines = session.sudo().statement_line_ids.filtered(lambda line: line.journal_id == cash_journal)
        self.assertEqual(statement_lines.amount, 3)
        self.assertEqual(statement_lines.move_id.state, 'posted')
        self.assertEqual(statement_lines.move_id.line_ids.filtered(lambda line: line.account_id == cash_account).debit, 3)
        bank_payment = session.sudo().bank_payment_ids.filtered(lambda payment: payment.pos_payment_method_id == self.method)
        self.assertEqual(bank_payment.amount, 9.75)
        self.assertEqual(bank_payment.journal_id, self.method.journal_id)

    def test_unsupported_voucher_is_explained_before_any_square_request(self):
        voucher = self.bank_payment_method.copy({'name': 'Store Voucher'})
        self.main_pos_config.with_context(bypass_payment_method_ids_forbidden_change=True).write({
            'payment_method_ids': [Command.link(voucher.id)],
        })
        with patch('odoo.addons.pos_square_terminal.square_api.request') as provider:
            self.start_pos_tour('SquareTerminalRejectsVoucher')
            provider.assert_not_called()
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.state, 'draft')
        self.assertFalse(order.payment_ids.filtered('transaction_id'))
        self.assertFalse(self.env['pos.square.order'].sudo().search_count([('order_id', '=', order.id)]))

    def test_odoo_tip_on_cash_and_card_survives_settlement_recovery(self):
        euro = self.env.ref('base.EUR')
        euro.active = True
        self.main_pos_config.journal_id.currency_id = euro
        self.method.square_terminal_id.location_id.sudo().currency_code = 'EUR'
        self.currency = 'EUR'
        self.desk_pad.lst_price = 60
        tip_product = self._configure_device_tip(odoo_tipping=True)
        original_tip_lines = set()

        def check_order(order):
            self.assertEqual([line['name'] for line in order['line_items']], [self.desk_pad.name])
            with self.registry.cursor() as cr:
                env = api.Environment(cr, self.env.uid, {})
                draft = env['pos.order'].search([('session_id', '=', self.pos_session.id)])
                tip_line = draft.lines.filtered(lambda line: line.product_id.id == tip_product.id)
                self.assertEqual(tip_line.price_subtotal_incl, 6)
                original_tip_lines.add(tip_line.id)

        self._run_mixed_checkout(
            'SquareTerminalOdooTipRecovery', ((6000, 0, 0),), 2200, (4000,),
            check_order=check_order, odoo_tips=(200, 400), lose_settlement=True,
        )
        order = self.pos_session.order_ids
        self.assertEqual((order.amount_total, order.amount_paid, order.tip_amount), (66, 66, 6))
        tip_line = order.lines.filtered(lambda line: line.product_id == tip_product)
        self.assertEqual(tip_line.price_subtotal_incl, 6)
        self.assertEqual(original_tip_lines, {tip_line.id})
        attempt = self.env['pos.square.payment.attempt'].sudo().search([('order_id', '=', order.id)])
        self.assertEqual((attempt.verified_base, attempt.verified_tip), (40, 4))

    def test_odoo_tip_remainder_goes_only_on_the_final_card(self):
        self.desk_pad.lst_price = 99.99
        self._configure_device_tip(odoo_tipping=True)
        self._run_mixed_checkout(
            'SquareTerminalOdooTipFinalCard', ((9999, 0, 0),), 0, (2500, 2500, 2500, 2499),
            odoo_tips=(0, 0, 0, 0, 1), lose_settlement=True,
        )
        order = self.pos_session.order_ids
        self.assertEqual((order.amount_total, order.amount_paid, order.tip_amount), (100, 100, 0.01))
        self.assertEqual(order.payment_ids.mapped('amount'), [25, 25, 25, 25])

    def test_odoo_tip_rounding_remainder_follows_two_card_shares_onto_cash(self):
        self.desk_pad.lst_price = 12
        tip_product = self._configure_device_tip(odoo_tipping=True)
        self._run_mixed_checkout(
            'SquareTerminalOdooTipRemainder', ((1200, 0, 0),), 402, (400, 400),
            odoo_tips=(2, 0, 0), lose_cash=True,
        )
        order = self.pos_session.order_ids
        self.assertEqual((order.amount_total, order.amount_paid, order.tip_amount), (12.02, 12.02, 0.02))
        tip_line = order.lines.filtered(lambda line: line.product_id == tip_product)
        self.assertEqual(tip_line.price_subtotal_incl, 0.02)

    def test_odoo_tip_on_one_card_preserves_the_original_gross(self):
        tip_product = self._configure_device_tip(odoo_tipping=True)
        tip_product.available_in_pos = True
        self.main_pos_config.square_terminal_tipping = False
        self._run_priced_checkout(
            'SquareTerminalSingleOdooTip', ((1234, 0, 0),), lambda order: None, odoo_tip_minor=200,
        )
        order = self.pos_session.order_ids
        self.assertEqual((order.amount_total, order.amount_paid, order.tip_amount), (14.34, 14.34, 2))
        tip_line = order.lines.filtered(lambda line: line.product_id == tip_product)
        self.assertEqual(tip_line.price_subtotal_incl, 2)
        self.assertEqual(order.payment_ids.amount, 14.34)

    def _run_mixed_checkout(self, tour, line_amounts, cash, cards, *, check_order=lambda order: None,
                            card_tips=(), odoo_tips=(), lose_settlement=False, lose_cash=False):
        fields = ('total_money', 'total_tax_money', 'total_discount_money')
        totals = {field: self._money(sum(amounts)) for field, amounts in zip(fields, zip(*line_amounts))}
        cash_tip = odoo_tips[0] if odoo_tips else 0
        tips = card_tips or odoo_tips[1:] or (0,) * len(cards)
        total_tip = cash_tip + sum(tips)
        provider_order = {}
        checkouts = {}
        payments = {}
        mutations = []
        cash_request = {}
        original_order = {}
        lose_readback = False

        def square_response(method, url, **kwargs):
            nonlocal lose_readback
            path = urlsplit(url).path
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
            if method == 'POST':
                mutations.append(path)
            if method == 'POST' and path in ('/v2/orders/calculate', '/v2/orders'):
                order = kwargs['json']['order']
                check_order(order)
                self.assertEqual(len(order['line_items']), len(line_amounts))
                response = {**order, **totals, 'line_items': [
                    {**line, **{field: self._money(amount) for field, amount in zip(fields, amounts)}}
                    for line, amounts in zip(order['line_items'], line_amounts)
                ]}
                if path == '/v2/orders':
                    self.assertFalse(provider_order)
                    provider_order.update(response, id='MIXEDORDER', version=1)
                    response = provider_order
                return self._square_response({'order': response})
            if (method, path) == ('POST', '/v2/terminals/checkouts'):
                number = len(checkouts) + 1
                checkout = kwargs['json']['checkout']
                self.assertEqual(checkout['order_id'], 'MIXEDORDER')
                self.assertEqual(checkout['payment_options'], {'autocomplete': False})
                self.assertEqual(checkout['amount_money'], self._money(cards[number - 1]))
                self.assertEqual(checkout['device_options']['tip_settings']['allow_tipping'], bool(card_tips))
                self.assertEqual(checkout.get('tip_money', self._money(0)),
                                 self._money(tips[number - 1] if odoo_tips else 0))
                self.assertTrue(all(payment['status'] == 'APPROVED' for payment in payments.values()))
                checkout_id = f'CHECKOUT-{number}'
                payment_id = f'CARD-{number}'
                checkouts[checkout_id] = {
                    **checkout, 'id': checkout_id, 'location_id': 'SQUARELOC',
                    'status': 'COMPLETED', 'payment_ids': [payment_id],
                }
                payments[payment_id] = {
                    'id': payment_id, 'order_id': 'MIXEDORDER', 'location_id': 'SQUARELOC',
                    'reference_id': checkout['reference_id'], 'source_type': 'CARD', 'status': 'APPROVED',
                    'amount_money': self._money(cards[number - 1]), 'tip_money': self._money(tips[number - 1]),
                    'total_money': self._money(cards[number - 1] + tips[number - 1]),
                }
                return self._square_response({'checkout': {**checkouts[checkout_id], 'status': 'PENDING', 'payment_ids': []}})
            if method == 'GET' and path.startswith('/v2/terminals/checkouts/'):
                return self._square_response({'checkout': checkouts[path.rsplit('/', 1)[1]]})
            if (method, path) == ('POST', '/v2/payments'):
                request = kwargs['json']
                self.assertEqual(request['source_id'], 'CASH')
                self.assertFalse(request['autocomplete'])
                self.assertEqual(request['order_id'], 'MIXEDORDER')
                self.assertEqual(request['location_id'], 'SQUARELOC')
                self.assertEqual(request['amount_money'], self._money(cash - cash_tip))
                self.assertEqual(request.get('tip_money', self._money(0)), self._money(cash_tip))
                self.assertEqual(request['cash_details'], {'buyer_supplied_money': self._money(cash)})
                self.assertTrue(request['idempotency_key'])
                self.assertEqual(len(checkouts), len(cards))
                with self.registry.cursor() as cr:
                    env = api.Environment(cr, self.env.uid, {})
                    draft = env['pos.order'].search([('session_id', '=', self.pos_session.id)])
                    actual_cash = draft.payment_ids.filtered_domain([('payment_method_id', '=', self.cash_method.id)])
                    self.assertEqual(actual_cash.amount, cash / 100)
                    self.assertFalse(actual_cash.transaction_id)
                if cash_request:
                    self.assertEqual(request, cash_request)
                else:
                    cash_request.update(request)
                    payments['CASH-1'] = {
                        'id': 'CASH-1', 'order_id': 'MIXEDORDER', 'location_id': 'SQUARELOC',
                        'reference_id': request['reference_id'], 'source_type': 'CASH', 'status': 'APPROVED',
                        'amount_money': self._money(cash - cash_tip), 'total_money': self._money(cash),
                        'tip_money': self._money(cash_tip),
                        'cash_details': {'buyer_supplied_money': self._money(cash), 'change_back_money': self._money(0)},
                    }
                    if lose_cash:
                        if odoo_tips:
                            provider_order.update(
                                total_tip_money=self._money(total_tip),
                                total_money=self._money(totals['total_money']['amount'] + total_tip),
                            )
                        raise ConnectionError('Square recorded the cash before the response was lost')
                return self._square_response({'payment': payments['CASH-1']})
            if method == 'GET' and path.startswith('/v2/payments/'):
                return self._square_response({'payment': payments[path.rsplit('/', 1)[1]]})
            if (method, path) == ('GET', '/v2/orders/MIXEDORDER'):
                if lose_readback:
                    lose_readback = False
                    raise ConnectionError('Square settlement readback was unavailable')
                return self._square_response({'order': provider_order})
            if (method, path) == ('POST', '/v2/orders/MIXEDORDER/pay'):
                request = kwargs['json']
                self.assertCountEqual(request['payment_ids'], payments)
                self.assertEqual(len(request['payment_ids']), len(cards) + bool(cash))
                self.assertEqual(request['order_version'], 1)
                self.assertTrue(request['idempotency_key'])
                self.assertEqual({payment['status'] for payment in payments.values()}, {'APPROVED'})
                with self.registry.cursor() as cr:
                    env = api.Environment(cr, self.env.uid, {})
                    native = env['pos.order'].search([('session_id', '=', self.pos_session.id)])
                    self.assertEqual(native.state, 'draft')
                    original_order.update(id=native.id, tender_ids=native.payment_ids.ids)
                tenders = []
                for payment_id in request['payment_ids']:
                    payment = payments[payment_id]
                    payment['status'] = 'COMPLETED'
                    tenders.append({
                        'payment_id': payment_id, 'location_id': 'SQUARELOC', 'type': payment['source_type'],
                        'amount_money': payment['total_money'], 'tip_money': payment.get('tip_money', self._money(0)),
                    })
                provider_order.update(
                    state='COMPLETED', version=2, tenders=tenders, total_tip_money=self._money(total_tip),
                    total_money=self._money(totals['total_money']['amount'] + total_tip),
                )
                if lose_settlement:
                    lose_readback = True
                    raise ConnectionError('Square settled the mixed order before the response was lost')
                return self._square_response({'order': provider_order})
            self.fail(f'Unexpected Square request: {method} {path}')

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour(tour)
        self.assertEqual(mutations, [
            '/v2/orders/calculate', '/v2/orders', *['/v2/terminals/checkouts'] * len(cards),
            *['/v2/payments'] * ((2 if lose_cash else 1) if cash else 0), '/v2/orders/MIXEDORDER/pay',
        ])
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.id, original_order['id'])
        self.assertCountEqual(order.payment_ids.ids, original_order['tender_ids'])
        self.assertEqual(order.state, 'paid')
        cash_line = order.payment_ids.filtered(lambda payment: payment.payment_method_id == self.cash_method)
        if cash:
            self.assertEqual(cash_line.amount, cash / 100)
            self.assertEqual(cash_line.transaction_id, 'CASH-1')
            self.assertFalse(cash_line.is_change)
        else:
            self.assertFalse(cash_line)
        card_lines = order.payment_ids - cash_line
        self.assertCountEqual(
            [(payment.amount, payment.payment_status, payment.transaction_id) for payment in card_lines],
            [((base + tip) / 100, 'done', f'CARD-{number}')
             for number, (base, tip) in enumerate(zip(cards, tips), 1)],
        )
        self.assertEqual({payment['status'] for payment in payments.values()}, {'COMPLETED'})

    def _included_vat(self):
        return self.env['account.tax'].create({
            'name': 'Included VAT', 'amount_type': 'percent', 'amount': 20,
            'price_include_override': 'tax_included', 'company_id': self._get_main_company().id,
        })

    def _money(self, amount):
        return {'amount': amount, 'currency': self.currency}

    def _run_priced_checkout(self, tour, line_amounts, check_order, *,
                             expected_state='paid', device_tip_minor=None, odoo_tip_minor=0):
        fields = ('total_money', 'total_tax_money', 'total_discount_money')
        gross = sum(amounts[0] for amounts in line_amounts)
        totals = {field: self._money(sum(amounts)) for field, amounts in zip(fields, zip(*line_amounts))}
        calls = []
        estimated = {}
        checkout = {}

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
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
                self.assertEqual(checkout.get('tip_money', self._money(0)), self._money(odoo_tip_minor))
                self.assertEqual(
                    checkout['device_options']['tip_settings']['allow_tipping'], device_tip_minor is not None,
                )
                checkout.update(id='SQUARECHECKOUT', location_id='SQUARELOC', status='PENDING')
                return self._square_response({'checkout': checkout})
            if (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                checkout.update(status='COMPLETED', payment_ids=['SQUAREPAYMENT'])
                return self._square_response({'checkout': checkout})
            if (method, path) == ('GET', '/v2/payments/SQUAREPAYMENT'):
                tip = odoo_tip_minor or device_tip_minor
                return self._square_response({'payment': {
                    'id': 'SQUAREPAYMENT', 'order_id': 'SQUAREORDER',
                    'location_id': 'SQUARELOC', 'reference_id': checkout['reference_id'],
                    'status': 'COMPLETED', 'amount_money': self._money(gross),
                    'total_money': self._money(gross + (tip or 0)), 'source_type': 'CARD',
                    **({'tip_money': self._money(tip)} if tip is not None else {}),
                }})
            self.fail(f'Unexpected Square request: {method} {path}')

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour(tour)
        self.assertEqual([path for method, path in calls if method == 'POST'], [
            '/v2/orders/calculate', '/v2/orders', '/v2/terminals/checkouts',
        ])
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.state, expected_state)
        self.assertEqual(order.payment_ids.transaction_id, 'SQUAREPAYMENT')

    def _send_terminal_checkout(self, order, payment, reference):
        # Odoo's test RPC cursors share a transaction but have separate ORM caches.
        self.env.flush_all()
        result = order.square_dispatch_payment(order.uuid, payment.uuid, reference)
        self.env.invalidate_all()
        return result

    def _cashier_draft(self, method=None, session=None, amount=12.34):
        method = method or self.method
        session = session or self.pos_session
        return self.env['pos.order'].with_user(self.pos_user).create({
            'uuid': str(uuid4()), 'session_id': session.id, 'state': 'draft',
            'amount_tax': 0, 'amount_total': amount, 'amount_return': 0, 'amount_paid': amount,
            'lines': [Command.create({
                'uuid': str(uuid4()), 'product_id': self.desk_pad.id, 'price_unit': amount,
                'qty': 1, 'price_subtotal': amount, 'price_subtotal_incl': amount,
            })],
            'payment_ids': [Command.create({
                'uuid': str(uuid4()), 'payment_method_id': method.id, 'amount': amount,
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

    def test_browser_supplied_tax_amounts_stop_before_square(self):
        tax = self._included_vat()
        self.desk_pad.write({'lst_price': 12, 'taxes_id': [Command.set(tax.ids)]})
        order = self._cashier_draft(amount=12)
        payment = order.payment_ids
        order.lines.write({
            'tax_ids': [Command.set(tax.ids)],
            'extra_tax_data': {
                'currency_id': order.currency_id.id, 'price_unit': 12, 'discount': 0, 'quantity': 1,
                'manual_tax_amounts': {str(tax.id): {'tax_amount_currency': 0}},
            },
        })
        with patch('odoo.addons.pos_square_terminal.square_api.request') as provider:
            with self.assertRaisesRegex(UserError, 'manual tax overrides'), self.cr.savepoint():
                order.square_prepare_payment(order.uuid, payment.uuid)
            provider.assert_not_called()

    def _draft_provider_order(self, order, total, *, line_total=1234):
        line, = order['line_items']
        return {
            **order, 'total_money': self._money(total), 'line_items': [{**line, 'total_money': self._money(line_total)}],
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
            result = self._send_terminal_checkout(order, payment, result['reference'])
            self.assertEqual(result['state'], 'blocked')
        self.assertEqual(calls, ['/v2/orders/calculate', '/v2/orders'])
        self.assertEqual(order.state, 'draft')
        self.assertFalse(payment.transaction_id)

    def test_uncertain_payment_blocks_register_close_and_another_sale_can_continue(self):
        checkout = {}

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            if method == 'POST' and path in ('/v2/orders/calculate', '/v2/orders'):
                order = self._draft_provider_order(kwargs['json']['order'], 1234)
                return self._square_response({'order': {**order, 'id': 'SQUAREORDER'}})
            if (method, path) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(kwargs['json']['checkout'])
                checkout.update(id='SQUARECHECKOUT', location_id='SQUARELOC', status='PENDING')
                return self._square_response({'checkout': checkout})
            if (method, path) == ('GET', '/v2/terminals/checkouts/SQUARECHECKOUT'):
                raise ConnectionError('Square timeout')
            self.fail(f'Unexpected Square request: {method} {path}')

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            self.start_pos_tour('SquareTerminalCloseWithUncertainPayment')

        self.env.invalidate_all()
        session = self.pos_session
        self.assertEqual(session.state, 'opened')
        draft = session.order_ids.filtered(lambda order: order.state == 'draft')
        self.assertEqual(len(draft), 1)
        self.assertFalse(draft.payment_ids.transaction_id)
        other = session.order_ids - draft
        self.assertEqual(other.state, 'paid')
        self.assertEqual(other.payment_ids.payment_method_id, self.cash_method)
        with self.assertRaisesRegex(UserError, 'Square payment'), self.cr.savepoint():
            session.with_user(self.pos_user).update_closing_control_state_session('')
        for close in (session.action_pos_session_closing_control, session.action_pos_session_close):
            with self.assertRaisesRegex(UserError, 'Square payment'), self.cr.savepoint():
                close()

    def test_verified_payment_resumes_invoice_after_selecting_the_customer(self):
        customer = self.env['res.partner'].create({'name': 'Square Invoice Customer'})
        self.pos_user.group_ids += self.env.ref('account.group_account_invoice')

        def check_order(order):
            self.assertNotIn('customer_id', order)
            self.assertNotIn('fulfillments', order)

        self._run_priced_checkout(
            'SquareTerminalInvoiceCustomer', ((1234, 0, 0),), check_order, expected_state='done',
        )
        order = self.pos_session.order_ids
        self.assertEqual(order.partner_id, customer)
        self.assertEqual(order.invoice_status, 'invoiced')
        self.assertEqual(order.account_move.partner_id, customer)
        self.assertEqual(order.account_move.state, 'posted')
        self.assertEqual(len(order.payment_ids), 1)

    def test_pending_handover_retries_on_the_original_connection_after_session_close(self):
        self.desk_pad.is_storable = True
        self._run_priced_checkout('SquareTerminalCounterHandover', ((1234, 0, 0),), lambda _order: None)
        session = self.pos_session.with_user(self.pos_user)
        order = session.order_ids
        payment = order.payment_ids
        picking_moves = order.picking_ids.move_ids.ids
        self.assertTrue(picking_moves)

        action = order.with_user(self.pos_admin).action_open_square_reconciliation()
        attempt = self.env[action['res_model']].with_user(self.pos_admin).search(action['domain'])
        self.assertEqual(attempt.handover_state, 'pending')
        attempt.connection_id.action_archive()
        original_reference = attempt.sudo().square_order_link_id.reference

        order_path = '/v2/orders/SQUAREORDER'
        attribute_path = order_path + '/custom-attributes/odoo-pos-in-store-handover'
        definition_path = '/v2/orders/custom-attribute-definitions'
        upserts = []
        definition = {}
        attribute = {}
        missing = self._square_response({'errors': [{'code': 'NOT_FOUND'}]})
        missing.status_code = 404

        def square_response(method, url, **kwargs):
            path = urlsplit(url).path
            self.assertEqual(urlsplit(url).netloc, 'connect.squareup.com')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer square-access-token')
            if (method, path) == ('GET', order_path):
                return self._square_response({'order': {
                    'id': 'SQUAREORDER', 'location_id': 'SQUARELOC',
                    'reference_id': original_reference, 'state': 'COMPLETED',
                }})
            if (method, path) == ('GET', attribute_path):
                self.assertTrue(definition, 'Square requires the definition before an attribute can be read')
                return self._square_response({'custom_attribute': attribute}) if attribute else missing
            if (method, path) == ('GET', definition_path + '/odoo-pos-in-store-handover'):
                return self._square_response({'custom_attribute_definition': definition}) if definition else missing
            if (method, path) == ('POST', definition_path):
                self.assertFalse(definition)
                definition.update(kwargs['json']['custom_attribute_definition'])
                self.assertEqual(definition['key'], 'odoo-pos-in-store-handover')
                self.assertEqual(definition['visibility'], 'VISIBILITY_READ_ONLY')
                return self._square_response({'custom_attribute_definition': definition})
            if (method, path) == ('POST', attribute_path):
                body = kwargs['json']
                self.assertEqual(set(body), {'idempotency_key', 'custom_attribute'})
                self.assertTrue(body['idempotency_key'])
                self.assertEqual(set(body['custom_attribute']), {'value'})
                self.assertRegex(body['custom_attribute']['value'], r'^In-store handover at .+Z$')
                upserts.append(body)
                if len(upserts) == 2:
                    attribute.update(key=definition['key'], **body['custom_attribute'])
                raise ConnectionError('Square response lost')
            self.fail(f'Unexpected Square request: {method} {path}')

        cron = self.env.ref('pos_square_terminal.ir_cron_square_terminal_handover')
        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response):
            cron.method_direct_trigger()
            self.env.invalidate_all()
            self.assertEqual(attempt.handover_state, 'pending')
            self.assertTrue(attempt.handover_error)
            self.assertEqual(len(upserts), 1)
            self.assertEqual(session.close_session_from_ui(), {'successful': True})
            self.assertEqual(session.state, 'closed')
            with freeze_time(fields.Datetime.now() + timedelta(minutes=6)):
                cron.method_direct_trigger()
                self.env.invalidate_all()
                self.assertEqual(attempt.handover_state, 'pending')
            with freeze_time(fields.Datetime.now() + timedelta(minutes=12)):
                cron.method_direct_trigger()
                self.env.invalidate_all()
                self.assertEqual(attempt.handover_state, 'sent')
                self.assertFalse(attempt.handover_error)
                cron.method_direct_trigger()
        self.assertEqual(len(upserts), 2)
        self.assertEqual(upserts[0], upserts[1])
        self.assertEqual(order.payment_ids, payment)
        self.assertEqual(payment.transaction_id, 'SQUAREPAYMENT')
        self.assertEqual(order.picking_ids.move_ids.ids, picking_moves)


@tagged('post_install', '-at_install', '-standard', 'square_terminal_discount')
class TestSquareTerminalGlobalDiscount(TestSquareTerminalCheckout):
    def test_global_discount_reaches_square_with_tax_and_itemized_odoo_receipt(self):
        tax = self._included_vat()
        self.desk_pad.write({'lst_price': 12, 'taxes_id': [Command.set(tax.ids)]})
        self.monitor_stand.lst_price = 5
        discount_product = self.env.ref('pos_discount.product_product_consumable')
        self.pos_session.post_closing_cash_details(0)
        self.pos_session.close_session_from_ui()
        self.main_pos_config.write({
            'module_pos_discount': True, 'discount_product_id': discount_product.id, 'discount_pc': 10,
        })
        self.main_pos_config.with_user(self.pos_user).open_ui()
        self.pos_session = self.main_pos_config.current_session_id
        self.pos_session.set_opening_control(0, '')

        def check_order(order):
            self.assertTrue(all(line['name'].startswith('Odoo receipt ') for line in order['line_items']))
            self.assertEqual([line['base_price_money'] for line in order['line_items']], [self._money(1200), self._money(500)])
            self.assertEqual([discount['amount_money'] for discount in order['discounts']], [self._money(120), self._money(50)])

        self._run_priced_checkout('SquareTerminalGlobalDiscount', ((1080, 180, 120), (450, 0, 50)), check_order)
        order = self.pos_session.order_ids
        self.assertAlmostEqual(order.amount_total, 15.30)
        self.assertAlmostEqual(order.amount_tax, 1.80)
        self.assertEqual(order.lines.product_id, self.desk_pad | self.monitor_stand | discount_product)
        self.assertEqual(sorted(order.lines.filtered(lambda line: line.product_id == discount_product).mapped('price_unit')), [-1.2, -0.5])

        # A cashier cannot apply 10% to the taxed goods and 20% to the untaxed goods in one Global Discount.
        with patch('odoo.addons.pos_square_terminal.square_api.request') as provider:
            with self.assertRaisesRegex(UserError, 'global discount does not match'), self.cr.savepoint():
                draft = self._cashier_draft(amount=14.80)
                draft.lines.unlink()
                draft.write({'amount_tax': 1.80, 'lines': [
                    Command.create({
                        'product_id': product.id, 'qty': 1, 'price_unit': price,
                        'price_subtotal': net, 'price_subtotal_incl': price,
                        'price_type': 'manual' if product == discount_product else 'original',
                        'tax_ids': [Command.set(taxes)], 'uuid': str(uuid4()),
                    }) for product, price, net, taxes in (
                        (self.desk_pad, 12, 10, tax.ids),
                        (self.monitor_stand, 5, 5, []),
                        (discount_product, -1.2, -1, tax.ids),
                        (discount_product, -1, -1, []),
                    )
                ]})
                draft.square_prepare_payment(draft.uuid, draft.payment_ids.uuid)
            provider.assert_not_called()

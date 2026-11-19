import json
import os
from unittest.mock import patch
from uuid import uuid4
from urllib.parse import urlsplit

from cryptography.fernet import Fernet
from requests import Response

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

    def test_first_card_is_verified_on_original_sale(self):
        mutations = []
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
                mutations.append(kwargs['json'])
                order = kwargs['json']['order']
                self.assertEqual(order['location_id'], 'SQUARELOC')
                self.assertTrue(order['reference_id'])
                self.assertNotIn('fulfillments', order)
                self.assertNotIn('customer_id', order)
                self.assertEqual({line['name'] for line in order['line_items']}, {'Desk Pad', 'Monitor Stand'})
                payload = {'order': {**order, 'id': 'SQUAREORDER', 'state': 'OPEN', 'total_money': money}}
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
            response = Response()
            response.status_code = 200
            response._content = json.dumps(payload).encode()
            return response

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

    def test_cash_stays_local_with_square_configured(self):
        with patch('odoo.addons.pos_square_terminal.square_api.request') as provider:
            self.start_pos_tour('SquareTerminalCashOnly')
            provider.assert_not_called()
        self.env.invalidate_all()
        order = self.pos_session.order_ids
        self.assertEqual(order.state, 'paid')
        self.assertEqual(order.payment_ids.payment_method_id, self.cash_method)
        self.assertFalse(self.env['pos.square.order'].sudo().search_count([('order_id', '=', order.id)]))

    def _cashier_draft(self, method=None):
        method = method or self.method
        return self.env['pos.order'].with_user(self.pos_user).create({
            'uuid': str(uuid4()), 'session_id': self.pos_session.id, 'state': 'draft',
            'amount_tax': 0, 'amount_total': 12.34, 'amount_return': 0, 'amount_paid': 12.34,
            'lines': [Command.create({
                'uuid': str(uuid4()), 'product_id': self.desk_pad.id, 'price_unit': 12.34,
                'qty': 1, 'price_subtotal': 12.34, 'price_subtotal_incl': 12.34,
            })],
            'payment_ids': [Command.create({
                'uuid': str(uuid4()), 'payment_method_id': method.id, 'amount': 12.34,
                'payment_status': 'waiting' if method == self.method else False,
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

    def test_linked_order_discrepancy_stops_terminal_request(self):
        order = self._cashier_draft()
        payment = order.payment_ids

        def square_response(method, url, **kwargs):
            self.assertEqual((method, urlsplit(url).path), ('POST', '/v2/orders'))
            response = Response()
            response.status_code = 200
            response._content = json.dumps({'order': {
                **kwargs['json']['order'], 'id': 'SQUAREORDER',
                'total_money': {'amount': 1235, 'currency': self.currency},
            }}).encode()
            return response

        with patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=square_response) as provider:
            order.square_prepare_payment(order.uuid, payment.uuid)
            order.square_prepare_linked_order(order.uuid, payment.uuid)
            result = order.square_dispatch_payment(order.uuid, payment.uuid)
            self.assertEqual(result['state'], 'blocked')
            provider.assert_called_once()
        self.assertEqual(order.state, 'draft')
        self.assertFalse(payment.transaction_id)

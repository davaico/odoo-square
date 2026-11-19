"""Run with an installed, disposable database in an ordinary Odoo shell.

    from odoo.addons.pos_square_terminal.tests.test_terminal_admission_concurrency import run
    run(env.registry)

The in-process Odoo test runner simulates commits with savepoints on one connection.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest import TestCase, TestLoader, TextTestRunner
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import uuid4

from cryptography.fernet import Fernet
from requests import Response

from odoo import Command, SUPERUSER_ID, api


class TestTerminalAdmissionConcurrency(TestCase):
    def test_two_registers_reserve_one_physical_terminal_only_once(self):
        registers = self._prepare_registers()
        before_reservation = Barrier(2)
        currency = registers[0][2]
        previews = []

        def square(method, url, **kwargs):
            self.assertEqual((method, urlsplit(url).path), ('POST', '/v2/orders/calculate'))
            order = kwargs['json']['order']
            money = {
                'total_money': {'amount': 1234, 'currency': currency},
                'total_tax_money': {'amount': 0, 'currency': currency},
                'total_discount_money': {'amount': 0, 'currency': currency},
            }
            previews.append(order['reference_id'])
            # Both independent transactions have passed the initial device lookup at this point.
            before_reservation.wait(timeout=15)
            order = {**order, **money, 'line_items': [{**line, **money} for line in order['line_items']]}
            response = Response()
            response.status_code = 200
            response._content = json.dumps({'order': order}).encode()
            return response

        def prepare(register):
            order_uuid, payment_uuid, _currency, cashier_id = register
            with self.registry.cursor() as cr:
                env = api.Environment(cr, cashier_id, {})
                cr.execute('SELECT pg_backend_pid()')
                pid, = cr.fetchone()
                result = env['pos.order'].square_prepare_payment(order_uuid, payment_uuid)
                return pid, result

        with patch.dict(os.environ, {'ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY': self.key}), patch(
            'odoo.addons.pos_square_terminal.square_api.request', side_effect=square,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(prepare, register) for register in registers]
                results = [future.result(timeout=25) for future in futures]
            self.assertEqual(len(set(pid for pid, _result in results)), 2)
            self.assertEqual(len(set(previews)), 2)
            self.assertCountEqual([result['state'] for _pid, result in results], ['prepared', 'busy'])

            for (order_uuid, payment_uuid, _currency, cashier_id), (_pid, result) in zip(registers, results):
                with self.registry.cursor() as cr:
                    orders = api.Environment(cr, cashier_id, {})['pos.order']
                    recovered = orders.square_check_payment(order_uuid, payment_uuid)
                    if result['state'] == 'prepared':
                        self.assertEqual(recovered['state'], 'prepared')
                        self.assertEqual(recovered['reference'], result['reference'])
                    else:
                        self.assertEqual(recovered, {'state': 'unstarted'})
            self.assertEqual(len(previews), 2)

    def _prepare_registers(self):
        self.key = Fernet.generate_key().decode()
        unique = uuid4().hex[:10]
        with patch.dict(os.environ, {'ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY': self.key}), self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            company = env.ref('base.main_company')
            bank = env['account.journal'].search([('company_id', '=', company.id), ('type', '=', 'bank')], limit=1)
            cashier = env['res.users'].with_context(no_reset_password=True).create({
                'name': 'Square concurrency cashier', 'login': f'square-race-{unique}',
                'company_id': company.id, 'company_ids': [Command.set(company.ids)],
                'group_ids': [Command.set([
                    env.ref('base.group_user').id, env.ref('point_of_sale.group_pos_user').id,
                ])],
            })
            product = env['product.product'].create({
                'name': 'Square concurrency product', 'available_in_pos': True, 'list_price': 12.34,
                'taxes_id': [Command.clear()],
            })
            connection = env['pos.square.connection'].create({
                'name': 'Square concurrency merchant', 'company_id': company.id,
                'application_id': 'sq0idp-concurrency', 'merchant_id': unique,
                'authorization_result': 'connected',
            })
            credential = connection._credential()
            credential.write({
                'access_token': credential._seal('square-concurrency-test-token'), 'expires_at': '2099-03-17 10:00:00',
            })
            currency = company.currency_id.name
            location = env['pos.square.location'].create({
                'connection_id': connection.id, 'square_id': 'RACELOCATION',
                'name': 'Square concurrency location', 'currency_code': currency, 'is_available': True,
            })

            registers = []
            for prefix in ('', 'device:'):
                terminal = env['pos.square.terminal'].create({
                    'name': 'Square concurrency terminal', 'connection_id': connection.id, 'location_id': location.id,
                    'device_id': prefix + 'SQUAREDEVICE', 'application_version': '6.77', 'readiness': 'ready',
                })
                method = env['pos.payment.method'].create({
                    'name': 'Square concurrency card', 'journal_id': bank.id, 'company_id': company.id,
                    'payment_method_type': 'terminal', 'use_payment_terminal': 'square_terminal',
                    'square_terminal_id': terminal.id,
                })
                config = env['pos.config'].create({
                    'name': f'Square concurrency register {unique} {len(registers)}', 'company_id': company.id,
                    'payment_method_ids': [Command.set(method.ids)],
                })
                config.with_user(cashier).open_ui()
                session = config.current_session_id
                session.set_opening_control(0, '')
                order_uuid, payment_uuid = str(uuid4()), str(uuid4())
                env['pos.order'].with_user(cashier).create({
                    'uuid': order_uuid, 'session_id': session.id, 'state': 'draft',
                    'amount_tax': 0, 'amount_total': 12.34, 'amount_return': 0, 'amount_paid': 12.34,
                    'lines': [Command.create({
                        'uuid': str(uuid4()), 'product_id': product.id, 'price_unit': 12.34,
                        'qty': 1, 'price_subtotal': 12.34, 'price_subtotal_incl': 12.34,
                    })],
                    'payment_ids': [Command.create({
                        'uuid': payment_uuid, 'payment_method_id': method.id, 'amount': 12.34,
                        'payment_status': 'waiting',
                    })],
                })
                registers.append((order_uuid, payment_uuid, currency, cashier.id))
        return registers


def run(registry):
    TestTerminalAdmissionConcurrency.registry = registry
    suite = TestLoader().loadTestsFromTestCase(TestTerminalAdmissionConcurrency)
    if not TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        raise SystemExit(1)

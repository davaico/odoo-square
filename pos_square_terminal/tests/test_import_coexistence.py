import json
import os
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hmac import digest
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import uuid4

from cryptography.fernet import Fernet
from freezegun import freeze_time
from requests import Response, Timeout

from odoo import Command, fields
from odoo.tests import tagged
from odoo.addons.point_of_sale.tests.test_frontend import TestPointOfSaleHttpCommon


@tagged('post_install', '-at_install', '-standard', 'square_terminal_import')
class TestSquareTerminalImport(TestPointOfSaleHttpCommon):

    def setUp(self):
        if 'square.config' not in self.env:
            self.skipTest('Square sales importer is not installed')
        super().setUp()
        self.enterContext(patch.dict(os.environ, {
            'ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY': Fernet.generate_key().decode(),
        }))
        company = self._get_main_company()
        currency = self.env.ref('base.EUR')
        currency.active = True
        company.currency_id = currency
        self.main_pos_config.available_pricelist_ids.currency_id = currency
        self.warehouse = self.main_pos_config.picking_type_id.warehouse_id
        self.importer = self.env['square.config'].create({
            'company_id': company.id,
            'square_application_id': 'separate-importer-app',
            'square_access_token': 'import-token',
            'square_environment': 'production',
            'square_webhook_signature_key': 'import-signing-key',
            'payment_journal_id': self.bank_journal.id,
            'location_mapping_ids': [Command.create({
                'square_location_id': 'SHOP', 'square_location_name': 'Counter',
                'warehouse_id': self.warehouse.id,
            })],
        })
        connection = self.env['pos.square.connection'].sudo().create({
            'name': 'Terminal merchant', 'company_id': company.id,
            'application_id': 'sq0idp-terminal-app', 'merchant_id': 'MERCHANT', 'authorization_result': 'connected',
        })
        credential = connection._credential()
        credential.write({
            'access_token': credential._seal('terminal-token'), 'expires_at': '2099-03-17 10:00:00',
        })
        location = self.env['pos.square.location'].sudo().create({
            'connection_id': connection.id, 'square_id': 'SHOP',
            'name': 'Counter', 'currency_code': 'EUR', 'is_available': True,
        })
        terminal = self.env['pos.square.terminal'].sudo().create({
            'name': 'Counter', 'connection_id': connection.id, 'location_id': location.id,
            'device_id': 'TERMINAL', 'application_version': '6.77', 'readiness': 'ready',
        })
        self.method = self.env['pos.payment.method'].create({
            'name': 'Square Counter', 'journal_id': self.bank_journal.id,
            'payment_method_type': 'terminal', 'use_payment_terminal': 'square_terminal',
            'square_terminal_id': terminal.id,
        })
        self.main_pos_config.payment_method_ids = [Command.link(self.method.id)]
        self.main_pos_config.with_user(self.pos_user).open_ui()
        self.session = self.main_pos_config.current_session_id
        self.session.set_opening_control(0, '')
        self.product = self.env['product.product'].create({
            'name': 'Tracked Odoo and Square item', 'default_code': 'SHARED-SKU',
            'type': 'consu', 'is_storable': True, 'list_price': 44, 'available_in_pos': True,
            'invoice_policy': 'delivery', 'taxes_id': [Command.clear()],
        })
        self.env['stock.quant']._update_available_quantity(self.product, self.warehouse.lot_stock_id, 10)
        self.resources = {
            '/v2/locations/SHOP': {'location': {'id': 'SHOP', 'merchant_id': 'MERCHANT'}},
            '/v2/catalog/object/VARIATION': {'object': {'item_variation_data': {'sku': 'SHARED-SKU'}}},
        }

    @staticmethod
    def money(amount):
        return {'amount': amount, 'currency': 'EUR'}

    @staticmethod
    def response(body):
        response = Response()
        response.status_code = 200
        response._content = json.dumps(body).encode()
        return response

    def _import_lookup(self, url, **kwargs):
        self.assertEqual(urlsplit(url).netloc, 'connect.squareup.com')
        self.assertEqual(kwargs['headers']['Authorization'], 'Bearer import-token')
        result = self.resources[urlsplit(url).path]
        if isinstance(result, Exception):
            raise result
        return self.response(result)

    def _event(self, event, event_id, resource):
        kind, action = event.split('.')
        key = f'{kind}_{action}' if kind == 'order' else kind
        route = '/square/webhook'
        signed_url = 'https://' + urlsplit(self.base_url()).netloc + route
        body = json.dumps({
            'event_id': event_id, 'merchant_id': 'MERCHANT', 'type': event,
            'data': {'id': resource.get('order_id') or resource.get('id'),
                     'type': kind, 'object': {key: resource}},
        }).encode()
        signature = b64encode(digest(b'import-signing-key', signed_url.encode() + body, 'sha256')).decode()
        response = self.url_open(route, data=body, headers={
            'Content-Type': 'application/json', 'x-square-hmacsha256-signature': signature,
        })
        self.env.invalidate_all()
        return response

    def _cashier_order(self):
        pos = self.env['pos.order'].with_user(self.pos_user)
        return pos.create({
            'uuid': str(uuid4()), 'session_id': self.session.id, 'state': 'draft',
            'amount_tax': 0, 'amount_total': 44, 'amount_return': 0, 'amount_paid': 44,
            'lines': [Command.create({
                'uuid': str(uuid4()), 'product_id': self.product.id, 'price_unit': 44,
                'qty': 1, 'price_subtotal': 44, 'price_subtotal_incl': 44,
            })],
            'payment_ids': [Command.create({
                'uuid': str(uuid4()), 'payment_method_id': self.method.id,
                'amount': 44, 'payment_status': 'waiting',
            })],
        })

    def _register_square_sale(self, order_id):
        payment_id = f'{order_id}-PAYMENT'
        sale = {
            'id': order_id, 'location_id': 'SHOP', 'state': 'COMPLETED',
            'line_items': [{
                'uid': 'LINE', 'name': self.product.name,
                'catalog_object_id': 'VARIATION', 'quantity': '1',
                'base_price_money': self.money(4400), 'total_money': self.money(4400),
            }],
            'total_money': self.money(4400),
            'tenders': [{
                'id': payment_id, 'type': 'CARD', 'card_details': {'status': 'CAPTURED'},
                'amount_money': self.money(4400),
            }],
        }
        self.resources[f'/v2/orders/{order_id}'] = {'order': sale}
        self.resources[f'/v2/payments/{payment_id}'] = {'payment': {
            'id': payment_id, 'order_id': order_id, 'location_id': 'SHOP',
            'status': 'COMPLETED', 'amount_money': self.money(4400),
        }}
        return sale

    def _terminal_provider(self, order, on_create=None, catalog=None, closed_at=None):
        checkout = {}
        payment_id = f'{order["id"]}-PAYMENT'
        payment = self.resources[f'/v2/payments/{payment_id}']['payment']

        def respond(method, url, **kwargs):
            endpoint = urlsplit(url).path
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer terminal-token')
            if (method, endpoint) == ('POST', '/v2/catalog/search'):
                return self.response(catalog or {'objects': []})
            if (method, endpoint) == ('POST', '/v2/orders/calculate'):
                preview = kwargs['json']['order']
                lines = [{**line, 'total_money': self.money(4400)} for line in preview['line_items']]
                return self.response({'order': {**preview, 'total_money': self.money(4400), 'line_items': lines}})
            if (method, endpoint) == ('POST', '/v2/orders'):
                request = kwargs['json']['order']
                order.update(request)
                order['line_items'] = [{**line, 'total_money': self.money(4400)} for line in order['line_items']]
                if on_create:
                    on_create()
                return self.response({'order': order})
            if (method, endpoint) == ('POST', '/v2/terminals/checkouts'):
                checkout.update(kwargs['json']['checkout'])
                checkout.update(id='CHECKOUT', location_id='SHOP', status='PENDING')
                payment.update(reference_id=checkout['reference_id'], terminal_checkout_id='CHECKOUT',
                               source_type='CARD', total_money=self.money(4400))
                return self.response({'checkout': checkout})
            if (method, endpoint) == ('GET', '/v2/terminals/checkouts/CHECKOUT'):
                order['state'] = 'COMPLETED'
                if closed_at:
                    order['closed_at'] = closed_at
                return self.response({'checkout': {**checkout, 'status': 'COMPLETED', 'payment_ids': [payment_id]}})
            if (method, endpoint) == ('GET', f'/v2/payments/{payment_id}'):
                return self.response({'payment': payment})
            self.fail(f'Unexpected Terminal request {method} {endpoint}')

        return patch('odoo.addons.pos_square_terminal.square_api.request', side_effect=respond)

    def _cashier_checkout(self, order, before_terminal_confirmation=None):
        payment = order.payment_ids
        self.assertEqual(order.square_prepare_payment(order.uuid, payment.uuid)['state'], 'prepared')
        prepared = order.square_prepare_linked_order(order.uuid, payment.uuid)
        self.assertEqual(prepared['state'], 'ready')
        reference = prepared['reference']
        self.assertEqual(order.square_mark_dispatching(order.uuid, payment.uuid, reference)['state'], 'dispatching')
        self.assertEqual(order.square_dispatch_payment(order.uuid, payment.uuid, reference)['state'], 'pending')
        if before_terminal_confirmation:
            before_terminal_confirmation()
        self.assertEqual(order.square_check_payment(order.uuid, payment.uuid)['state'], 'verified')
        order.sync_from_ui([{
            'uuid': order.uuid, 'name': order.name, 'session_id': self.session.id,
            'lines': [], 'payment_ids': [], 'amount_return': 0, 'state': 'paid',
        }])
        self.assertEqual(order.state, 'paid')
        self.assertEqual(order.payment_ids.transaction_id, 'TERMINAL-ORDER-PAYMENT')

    def _assert_independent_sales(self, odoo_order):
        sales = self.env['sale.order'].search([('square_order_id', 'in', ['TERMINAL-ORDER', 'REGISTER-ORDER'])])
        self.assertEqual(sales.square_order_id, 'REGISTER-ORDER')
        self.assertEqual(sales.invoice_ids.amount_total, 44)
        self.assertEqual(sales.invoice_ids.reconciled_payment_ids.amount, 44)
        self.assertEqual(sales.picking_ids.state, 'done')
        self.assertEqual(sales.picking_ids.move_ids.quantity, 1)
        self.assertEqual(odoo_order.amount_paid, 44)
        self.assertEqual(odoo_order.picking_ids.state, 'done')
        self.assertEqual(odoo_order.picking_ids.move_ids.quantity, 1)
        self.assertEqual(self.product.with_context(location=self.warehouse.lot_stock_id.id).qty_available, 8)

    def test_catalog_sale_converges_when_square_applies_the_count_first(self):
        self._assert_catalog_sale_stock(square_deduction_first=False)

    def test_catalog_sale_converges_at_session_close_when_square_deducts_first(self):
        self._assert_catalog_sale_stock(square_deduction_first=True, stock_at_closing=True)

    def _assert_catalog_sale_stock(self, square_deduction_first, stock_at_closing=False):
        self.session.update_stock_at_closing = stock_at_closing
        odoo_order = self._cashier_order()
        terminal_order = self._register_square_sale('TERMINAL-ORDER')
        catalog = {
            'objects': [{
                'id': 'VARIATION', 'type': 'ITEM_VARIATION',
                'item_variation_data': {'item_id': 'ITEM', 'sku': self.product.default_code, 'track_inventory': True},
            }],
            'related_objects': [{'id': 'ITEM', 'type': 'ITEM'}],
        }
        location = {'id': 'SHOP', 'status': 'ACTIVE', 'merchant_id': 'MERCHANT'}
        self.resources['/v2/locations'] = {'locations': [location]}
        self.resources['/v2/locations/SHOP'] = {'location': location}
        local_time = fields.Datetime.now().replace(tzinfo=timezone.utc)
        # Square's clock is slightly ahead of Odoo's when the native move finishes.
        deduction_time = local_time + timedelta(seconds=2)
        square_changes = {}

        def square_stock():
            quantity = Decimal(10)
            for _, kind, amount in sorted(square_changes.values()):
                quantity = amount if kind == 'count' else quantity + amount
            return quantity

        def import_post(url, **kwargs):
            self.assertEqual(urlsplit(url).netloc, 'connect.squareup.com')
            self.assertEqual(kwargs['headers']['Authorization'], 'Bearer import-token')
            endpoint = urlsplit(url).path
            if endpoint == '/v2/catalog/search':
                return self.response(catalog)
            self.assertEqual(endpoint, '/v2/inventory/changes/batch-create')
            request = kwargs['json']
            [change] = request['changes']
            self.assertEqual(change['type'], 'PHYSICAL_COUNT')
            count = change['physical_count']
            self.assertEqual((count['location_id'], count['catalog_object_id']), ('SHOP', 'VARIATION'))
            occurred_at = datetime.fromisoformat(count['occurred_at'].replace('Z', '+00:00'))
            self.assertLessEqual(occurred_at, datetime.now(timezone.utc))
            square_changes.setdefault(request['idempotency_key'], (occurred_at, 'count', Decimal(count['quantity'])))
            return self.response({'counts': []})

        with (
            freeze_time(local_time) as clock,
            self._terminal_provider(terminal_order, catalog=catalog, closed_at=deduction_time.isoformat()),
            patch('odoo.addons.odoo_square.models.square_api_client.requests.get', side_effect=self._import_lookup),
            patch('odoo.addons.odoo_square.models.square_api_client.requests.post', side_effect=import_post),
        ):
            self._cashier_checkout(odoo_order)
            self.assertEqual(terminal_order['line_items'][0]['catalog_object_id'], 'VARIATION')
            pickings = odoo_order.picking_ids
            if stock_at_closing:
                self.assertFalse(pickings)
                self.session.action_pos_session_closing_control()
                self.assertEqual(self.session.state, 'closed')
                pickings = self.session.picking_ids
            self.assertEqual(pickings.move_ids.quantity, 1)
            self.assertEqual(pickings.state, 'done')
            if square_deduction_first:
                square_changes['square-sale'] = (deduction_time, 'sale', Decimal(-1))
            cron = self.env.ref('odoo_square.ir_cron_square_stock_counts')
            publish = cron.ir_actions_server_id.with_user(cron.user_id)
            publish.run()  # Resolve the SKU.
            if stock_at_closing:
                # Square has accepted payment but its order read still lacks completion metadata.
                published_closed_at = terminal_order.pop('closed_at')
                publish.run()
                self.assertEqual(set(square_changes), {'square-sale'})
                terminal_order['closed_at'] = published_closed_at
                clock.move_to(local_time + timedelta(minutes=6))
            else:
                # The original warehouse observation would precede the Square deduction.
                publish.run()
                self.assertFalse(square_changes)
                clock.move_to(local_time + timedelta(seconds=4))
            publish.run()  # Persist the effective timestamp.
            publish.run()  # Send the count.
            self.assertEqual(square_stock(), 9)
            square_changes['square-sale'] = (deduction_time, 'sale', Decimal(-1))
            for event_id in ('terminal-inventory-sale', 'terminal-inventory-sale', 'terminal-inventory-replay'):
                response = self._event('order.created', event_id, terminal_order)
                self.assertEqual(response.status_code, 200, response.text)
            publish.run()

        self.assertEqual(square_stock(), 9)
        self.assertEqual(len(square_changes), 2)
        self.assertEqual(self.product.with_context(location=self.warehouse.lot_stock_id.id).qty_available, 9)
        self.assertFalse(self.env['sale.order'].search([('square_order_id', '=', 'TERMINAL-ORDER')]))
        moves = self.env['stock.move'].search([('product_id', '=', self.product.id), ('state', '=', 'done')])
        self.assertEqual(moves, pickings.move_ids)

    def test_equal_sales_stay_independent_when_the_terminal_order_arrives_first(self):
        odoo_order = self._cashier_order()
        terminal_order = self._register_square_sale('TERMINAL-ORDER')
        square_order = self._register_square_sale('REGISTER-ORDER')

        def early_terminal_order():
            # Square publishes the order before the integration receives its ID.
            notice = {'order_id': 'TERMINAL-ORDER', 'location_id': 'SHOP', 'state': 'OPEN'}
            response = self._event('order.created', 'early-terminal-order', notice)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['status'], 'ignored', response.text)

        with (
            patch('odoo.addons.odoo_square.models.square_api_client.requests.get', side_effect=self._import_lookup),
            self._terminal_provider(terminal_order, early_terminal_order),
        ):
            self._cashier_checkout(odoo_order)
            response = self._event('order.created', 'register-order', square_order)
            self.assertEqual(response.json()['status'], 'success', response.text)
        self._assert_independent_sales(odoo_order)

    def test_early_terminal_refund_and_return_are_excluded_after_a_square_sale(self):
        odoo_order = self._cashier_order()
        terminal_order = self._register_square_sale('TERMINAL-ORDER')
        square_order = self._register_square_sale('REGISTER-ORDER')
        return_order = {
            'id': 'TERMINAL-RETURN', 'location_id': 'SHOP', 'state': 'COMPLETED',
            'returns': [{'source_order_id': 'TERMINAL-ORDER'}],
        }
        refund = {
            'id': 'TERMINAL-REFUND', 'status': 'COMPLETED', 'amount_money': self.money(1000),
            'order_id': return_order['id'], 'payment_id': 'TERMINAL-ORDER-PAYMENT', 'location_id': 'SHOP',
        }
        self.resources['/v2/orders/TERMINAL-RETURN'] = {'order': return_order}
        self.resources['/v2/refunds/TERMINAL-REFUND'] = {'refund': refund}
        refund_count = self.env['square.refund'].search_count([])

        def deliver_events():
            self.assertFalse(odoo_order.payment_ids.transaction_id)
            payment = self.resources['/v2/payments/TERMINAL-ORDER-PAYMENT']['payment']
            events = (
                ('refund.created', 'refund-first', refund),
                ('order.created', 'return-order-second', return_order),
                ('payment.updated', 'payment-third', payment),
            )
            for event, event_id, body in events * 2:
                response = self._event(event, event_id, body)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json(), {
                    'status': 'ignored', 'message': 'Odoo POS Square Terminal operation',
                }, response.text)

        with (
            patch('odoo.addons.odoo_square.models.square_api_client.requests.get', side_effect=self._import_lookup),
            self._terminal_provider(terminal_order),
        ):
            self.assertEqual(self._event('order.created', 'register-first', square_order).json()['status'], 'success')
            self._cashier_checkout(odoo_order, deliver_events)
            self.assertEqual(self._event('order.created', 'register-first', square_order).json()['status'],
                             'already_processed')
            self.resources['/v2/orders/TERMINAL-ORDER'] = Timeout('Square order lookup unavailable')
            payment = self.resources['/v2/payments/TERMINAL-ORDER-PAYMENT']['payment']
            response = self._event('payment.updated', 'linked-payment-replay', payment)
            self.assertEqual(response.json(), {
                'status': 'ignored', 'message': 'Odoo POS Square Terminal operation',
            }, response.text)

        self._assert_independent_sales(odoo_order)
        self.assertEqual(self.env['square.refund'].search_count([]), refund_count)
        self.assertFalse(self.env['sale.order'].search_count([('square_order_id', '=', return_order['id'])]))

    def test_manual_resync_defers_only_the_order_with_ambiguous_ownership(self):
        odoo_order = self._cashier_order()
        terminal_order = self._register_square_sale('TERMINAL-ORDER')
        square_order = self._register_square_sale('REGISTER-ORDER')
        square_order['reference_id'] = 'odoo-' + uuid4().hex
        with self._terminal_provider(terminal_order):
            self._cashier_checkout(odoo_order)
        wizard = self.env['square.manual.resync.wizard'].with_context(
            allowed_company_ids=self.importer.company_id.ids,
        ).create({'config_id': self.importer.id})

        def search(url, **kwargs):
            self.assertEqual(urlsplit(url).path, '/v2/orders/search')
            self.assertEqual(kwargs['json']['location_ids'], ['SHOP'])
            return self.response({'orders': [terminal_order, square_order]})

        location = self.resources['/v2/locations/SHOP']
        self.resources['/v2/locations/SHOP'] = Timeout('Square location lookup unavailable')
        with (
            patch('odoo.addons.odoo_square.models.square_api_client.requests.get', side_effect=self._import_lookup),
            patch('odoo.addons.odoo_square.models.square_api_client.requests.post', side_effect=search),
        ):
            notice = {'order_id': terminal_order['id'], 'location_id': 'SHOP', 'state': 'COMPLETED'}
            response = self._event('order.updated', 'uncertain-owner', notice)
            self.assertEqual(response.status_code, 503, response.text)
            wizard.action_scan_missing_orders()
            self.assertEqual(wizard.line_ids.square_order_id, 'REGISTER-ORDER')
            self.assertEqual(wizard.deferred_order_references, 'TERMINAL-ORDER')

            # A selection from an older browser tab must still be rechecked.
            wizard.line_ids = [Command.create({'square_order_id': 'TERMINAL-ORDER'})]
            self.resources['/v2/locations/SHOP'] = location
            wizard.action_validate_resync()
            self.assertEqual(wizard.processed_count, 1)
            self.assertEqual(wizard.error_count, 0)
            replay = self._event('order.updated', 'uncertain-owner', notice)
            self.assertEqual(replay.json()['status'], 'ignored', replay.text)
            wizard.action_scan_missing_orders()
            self.assertFalse(wizard.line_ids)
            self.assertFalse(wizard.deferred_order_references)

        self._assert_independent_sales(odoo_order)

    def test_queue_replay_finishes_independent_sales_while_ownership_is_unresolved(self):
        odoo_order = self._cashier_order()
        terminal_order = self._register_square_sale('TERMINAL-ORDER')
        square_order = self._register_square_sale('REGISTER-ORDER')
        with self._terminal_provider(terminal_order):
            self._cashier_checkout(odoo_order)

        queue = self.env['square.webhook.queue'].sudo()
        with patch('odoo.addons.odoo_square.models.square_api_client.requests.get', side_effect=self._import_lookup):
            self.assertEqual(self._event('order.created', 'register', square_order).json()['status'], 'success')
            # Replay metadata saved by the signed webhook ingress.
            terminal_update, register_update = queue.create([{
                'webhook_event_id': order_id + '-queued-update',
                'event_type': 'order.updated',
                'order_data': json.dumps({'order_id': order_id, 'location_id': 'SHOP', 'state': 'COMPLETED'}),
                'square_order_id': order_id,
                'config_id': self.importer.id,
                'signature_verified': True,
                'next_retry_time': fields.Datetime.now(),
            } for order_id in ('TERMINAL-ORDER', 'REGISTER-ORDER')])
            location = self.resources['/v2/locations/SHOP']
            self.resources['/v2/locations/SHOP'] = Timeout('Square location lookup unavailable')
            queue.process_pending_events()
            self.assertEqual(terminal_update.state, 'pending')
            self.assertEqual(terminal_update.retry_count, 1)
            self.assertEqual(register_update.state, 'completed')

            self.resources['/v2/locations/SHOP'] = location
            terminal_update.next_retry_time = fields.Datetime.now()
            queue.process_pending_events()
            queue.process_pending_events()
            self.assertEqual(terminal_update.state, 'completed')
            self.assertEqual(terminal_update.order_data, '{}')

        self._assert_independent_sales(odoo_order)

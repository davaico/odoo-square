import json
import os
from datetime import timedelta
from unittest.mock import patch

from cryptography.fernet import Fernet
from lxml import html
from requests import Response

from odoo import Command, fields
from odoo.tests import HttpCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestSquareCheckoutScope(HttpCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env.ref('website.default_website')
        cls.company = cls.website.company_id
        cls.customer = new_test_user(
            cls.env, login='square_scope_buyer', password='square_scope_buyer', groups='base.group_portal',
        )
        cls.customer.partner_id.write({
            'name': 'International Buyer', 'email': 'international@example.test', 'phone': '5555550100',
            'street': '1 High St', 'city': 'Berlin', 'zip': '10115', 'country_id': cls.env.ref('base.de').id,
        })
        cls.env['ir.config_parameter'].set_param('sale.automatic_invoice', 'True')
        cls.env['ir.config_parameter'].set_param('sale.async_emails', 'True')

    def setUp(self):
        super().setUp()
        key = patch.dict(os.environ, {'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY': Fernet.generate_key().decode()})
        key.start()
        self.addCleanup(key.stop)

    def _configure_provider(
        self, company, website, location_code, country, currency, *, production=False, journal_currency=None,
    ):
        journal = self.env['account.journal'].with_company(company).create({
            'name': f'Square {location_code} Bank',
            'code': f'SQ{self.env["account.journal"].search_count([]):03d}',
            'company_id': company.id,
            'type': 'bank',
            'currency_id': journal_currency.id if journal_currency else False,
        })
        provider = self.env.ref('payment_square_online.payment_provider_square_online').copy({
            'name': f'Square {location_code}',
            'company_id': company.id,
            'website_id': website.id if website else False,
            'journal_id': journal.id,
            'square_environment': 'production' if production else 'sandbox',
            'square_application_id': ('sq0idp-' if production else 'sandbox-sq0idb-') + location_code,
        })
        provider.square_application_secret = 'fixture-secret'
        connection = self.env['payment.square.connection'].search([('provider_id', '=', provider.id)])
        connection.write({
            'merchant_id': f'MERCHANT-{location_code}',
            'access_token': connection._seal(f'access-{location_code}'),
            'expires_at': fields.Datetime.now() + timedelta(days=1),
        })
        location = self.env['payment.square.location'].create({
            'provider_id': provider.id, 'square_id': location_code, 'name': location_code,
            'country_code': country, 'currency_code': currency,
        })
        provider.write({
            'square_location_id': location.id, 'state': 'enabled' if production else 'test', 'is_published': True,
        })
        return provider

    def _rpc(self, route, **params):
        result = self.url_open(route, data=json.dumps({
            'jsonrpc': '2.0', 'method': 'call', 'params': params,
        }), headers={'Content-Type': 'application/json'}).json()
        self.env.invalidate_all()
        return result

    def _result(self, route, **params):
        response = self._rpc(route, **params)
        self.assertNotIn('error', response, response.get('error', {}).get('data', {}).get('message'))
        return response['result']

    def _open_store(self, website, user=None):
        user = user or self.customer
        self.authenticate(user.login, user.login)
        if website.domain:
            self.opener.headers['Host'] = website.domain.removeprefix('http://').removeprefix('https://')

    def _cart(self, website, currency, item_price, shipping):
        company = website.company_id
        tax = self.env['account.tax'].create({
            'name': f'Square scope sales tax {website.id} {currency.name}', 'amount': 10, 'amount_type': 'percent',
            'type_tax_use': 'sale', 'company_id': company.id,
        })
        product = self.env['product.product'].create({
            'name': 'Square scope item', 'type': 'consu', 'website_published': True,
            'company_id': company.id, 'taxes_id': [Command.set(tax.ids)],
        })
        shipping_product = self.env['product.product'].create({
            'name': 'Square scope shipping', 'type': 'service', 'company_id': company.id,
            'taxes_id': [Command.clear()],
        })
        carrier = self.env['delivery.carrier'].create({
            'name': 'Square scope delivery', 'delivery_type': 'fixed',
            'product_id': shipping_product.id, 'fixed_price': shipping, 'website_published': True,
            'website_id': website.id,
        })
        if not currency.active:
            currency.active = True
        pricelist = self.env['product.pricelist'].create({
            'name': f'Square {currency.name} store', 'currency_id': currency.id,
            'website_id': website.id, 'company_id': company.id,
            'item_ids': [Command.create({
                'applied_on': '1_product', 'product_tmpl_id': product.product_tmpl_id.id,
                'compute_price': 'fixed', 'fixed_price': item_price,
            })],
        })
        self.customer.partner_id.with_company(company).property_product_pricelist = pricelist
        self._open_store(website)
        self._result('/shop/cart/update_json', product_id=product.id, add_qty=1)
        order = self.env['sale.order'].search([
            ('partner_id', '=', self.customer.partner_id.id), ('website_id', '=', website.id),
            ('state', '=', 'draft'),
        ], limit=1)
        self.assertTrue(order)
        order.set_delivery_line(carrier, shipping)
        self.assertEqual(order.company_id, company)
        self.assertEqual(order.currency_id, currency)
        self.assertEqual(order.amount_untaxed, item_price + shipping)
        return order

    def _square_forms(self):
        response = self.url_open('/shop/payment')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.url.endswith('/shop/payment'), response.url)
        return {
            form.get('data-square-location-id'): form
            for form in html.fromstring(response.content).xpath('//*[@name="o_square_card"]')
        }

    def _prepare_params(self, order, provider, signature):
        return {
            'access_token': order._portal_ensure_token(),
            'provider_id': provider.id,
            'payment_method_id': self.env.ref('payment.payment_method_card').id,
            'flow': 'direct',
            'token_id': None, 'tokenization_requested': False,
            'landing_route': '/shop/payment/validate',
            'amount': order.amount_total,
            'square_cart_signature': signature,
        }

    def _prepare(self, order, provider, location_code):
        signature = self._square_forms()[location_code].get('data-square-cart-signature')
        return self._result(
            f'/shop/payment/transaction/{order.id}', **self._prepare_params(order, provider, signature),
        )

    def _complete(self, prepared, location_code, amount, currency, *, production=False):
        expected_money = {'amount': amount, 'currency': currency}
        expected_host = 'connect.squareup.com' if production else 'connect.squareupsandbox.com'

        def square_response(method, url, **kwargs):
            self.assertEqual(method, 'POST')
            self.assertEqual(kwargs['headers']['Authorization'], f'Bearer access-{location_code}')
            request = kwargs['json']
            if url == f'https://{expected_host}/v2/orders':
                order = request['order']
                self.assertEqual(order['location_id'], location_code)
                self.assertEqual(order['line_items'][0]['base_price_money'], expected_money)
                body = {'order': {
                    'id': f'SQ-ORDER-{location_code}', 'location_id': location_code,
                    'reference_id': order['reference_id'], 'state': 'OPEN', 'total_money': expected_money,
                }}
            else:
                self.assertEqual(url, f'https://{expected_host}/v2/payments')
                self.assertEqual(request['location_id'], location_code)
                self.assertEqual(request['order_id'], f'SQ-ORDER-{location_code}')
                self.assertEqual(request['amount_money'], expected_money)
                self.assertIs(type(request['amount_money']['amount']), int)
                body = {'payment': {
                    'id': f'SQ-PAYMENT-{location_code}', 'status': 'COMPLETED', 'source_type': 'CARD',
                    'location_id': location_code, 'reference_id': request['reference_id'],
                    'order_id': request['order_id'], 'amount_money': expected_money,
                }}
            response = Response()
            response.status_code = 200
            response._content = json.dumps(body).encode()
            return response

        with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_response) as square:
            self.assertEqual(self._result(
                '/payment/square/online/submit', **prepared['square_submission'], source_id='cnon:scope-buyer',
            )['state'], 'done')
            self.assertEqual(square.call_count, 2)
        self._result('/payment/status/poll')

    def test_web_checkout_and_refund_preserve_all_six_currencies(self):
        for country, code in (
            ('AU', 'AUD'), ('CA', 'CAD'), ('FR', 'EUR'), ('GB', 'GBP'), ('JP', 'JPY'), ('US', 'USD'),
        ):
            with self.subTest(currency=code):
                currency = self.env.ref(f'base.{code}')
                provider = self._configure_provider(
                    self.company, self.website, code, country, code,
                    journal_currency=self.company.currency_id if code == 'CAD' else None,
                )
                if code == 'CAD':
                    self.assertNotEqual(self.company.currency_id, currency)
                    self.env['res.currency.rate'].create({
                        'name': fields.Date.today(), 'currency_id': currency.id,
                        'company_id': self.company.id, 'rate': 0.25,
                    })
                if code == 'JPY':
                    item_price, shipping, tax, total, verification = 4015, 626, 402, 5043, '5043'
                else:
                    item_price, shipping, tax, total, verification = 40.15, 6.26, 4.02, 50.43, '50.43'
                order = self._cart(self.website, currency, item_price, shipping)
                self.assertEqual(order.amount_tax, tax)
                self.assertEqual(order.amount_total, total)
                prepared = self._prepare(order, provider, code)
                self.assertEqual(prepared['square_verification']['amount'], verification)
                self.assertEqual(prepared['square_verification']['currencyCode'], code)
                self.assertEqual(prepared['square_verification']['billingContact']['countryCode'], 'DE')
                self._complete(prepared, code, 5043, code)
                self.assertEqual(order.state, 'sale')
                self.assertEqual(order.invoice_ids.amount_residual, 0)
                transaction = order.transaction_ids
                self.assertEqual(transaction.square_amount_minor, 5043)
                self.assertEqual(transaction.square_currency_code, code)
                self.assertEqual(transaction.payment_id.currency_id, currency)
                self.assertEqual(transaction.payment_id.amount, total)
                if code == 'CAD':
                    payment = transaction.payment_id
                    self.assertEqual(payment.journal_id.currency_id, self.company.currency_id)
                    receivable = payment.move_id.line_ids.filtered(
                        lambda line: line.account_id.account_type == 'asset_receivable',
                    )
                    invoice_receivable = order.invoice_ids.line_ids.filtered(
                        lambda line: line.display_type == 'payment_term',
                    )
                    self.assertEqual(receivable.amount_currency, -50.43)
                    self.assertEqual(receivable.balance, -201.72)
                    self.assertEqual(receivable.amount_residual_currency, 0)
                    self.assertTrue(receivable.full_reconcile_id)
                    self.assertEqual(receivable.full_reconcile_id, invoice_receivable.full_reconcile_id)

                self.authenticate('admin', 'admin')

                def square_refund(method, url, **kwargs):
                    self.assertEqual(method, 'POST')
                    self.assertTrue(url.endswith('/v2/refunds'))
                    request = kwargs['json']
                    self.assertEqual(request['payment_id'], f'SQ-PAYMENT-{code}')
                    self.assertEqual(request['amount_money'], {'amount': 5043, 'currency': code})
                    self.assertIs(type(request['amount_money']['amount']), int)
                    response = Response()
                    response.status_code = 200
                    response._content = json.dumps({'refund': {
                        'id': f'SQ-REFUND-{code}', 'status': 'COMPLETED', 'location_id': code,
                        'payment_id': f'SQ-PAYMENT-{code}', 'amount_money': {'amount': 5043, 'currency': code},
                        'reason': request['reason'],
                    }}).encode()
                    return response

                with patch('odoo.addons.payment_square_online.square_api.request', side_effect=square_refund) as square:
                    self._result('/web/dataset/call_button', model='payment.transaction', method='action_refund',
                                 args=[transaction.ids], kwargs={})
                    square.assert_called_once()
                refund = transaction.child_transaction_ids
                self.assertEqual(refund.amount, -total)
                self.assertEqual(refund.currency_id, currency)
                self.assertEqual(refund.payment_id.payment_type, 'outbound')
                self.assertEqual(refund.payment_id.currency_id, currency)
                self.assertEqual(refund.payment_id.amount, total)
                self.assertEqual(refund.payment_id.source_payment_id, transaction.payment_id)
                self.assertEqual(refund.payment_id.move_id.state, 'posted')
                if code == 'CAD':
                    receivable = refund.payment_id.move_id.line_ids.filtered(
                        lambda line: line.account_id.account_type == 'asset_receivable',
                    )
                    self.assertEqual(refund.payment_id.journal_id.currency_id, self.company.currency_id)
                    self.assertEqual(receivable.amount_currency, 50.43)
                    self.assertEqual(receivable.balance, 201.72)
                    self.assertEqual(receivable.amount_residual_currency, 50.43)
                    self.assertFalse(receivable.reconciled)

    def test_checkout_cannot_bypass_native_card_availability_with_stale_form(self):
        provider = self._configure_provider(self.company, self.website, 'USA', 'US', 'USD')
        order = self._cart(self.website, self.env.ref('base.USD'), 40.15, 6.26)
        signature = self._square_forms()['USA'].get('data-square-cart-signature')
        params = self._prepare_params(order, provider, signature)
        prepared = self._result(f'/shop/payment/transaction/{order.id}', **params)

        card = self.env.ref('payment.payment_method_card')
        card.supported_country_ids = [Command.set(self.env.ref('base.us').ids)]
        self.assertFalse(self._square_forms())
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            self.assertIn('error', self._rpc(f'/shop/payment/transaction/{order.id}', **params))
            self.assertIn('error', self._rpc(
                '/payment/square/online/submit', **prepared['square_submission'], source_id='cnon:stale-card',
            ))
            square.assert_not_called()
        self.assertEqual(order.state, 'draft')
        self.assertEqual(order.transaction_ids.state, 'draft')

    def test_checkout_rejects_a_total_below_the_location_currency_precision(self):
        currency = self.env.ref('base.CAD')
        currency.rounding = 0.001
        provider = self._configure_provider(self.company, self.website, 'CANADA', 'CA', 'CAD')
        order = self._cart(self.website, currency, 40.15, 6.26)
        self.env.invalidate_all()
        self.assertEqual(order.amount_total, 50.425)
        self.assertFalse(self._square_forms())
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            result = self._rpc(
                f'/shop/payment/transaction/{order.id}',
                **self._prepare_params(order, provider, 'stale-form'),
            )
            self.assertIn('Square is unavailable', result['error']['data']['message'])
            square.assert_not_called()
        self.assertFalse(order.transaction_ids)

    def test_checkout_rechecks_publication_currency_and_location_before_sending_payment(self):
        provider = self._configure_provider(self.company, self.website, 'US-FIRST', 'US', 'USD')
        order = self._cart(self.website, self.env.ref('base.USD'), 40.15, 6.26)
        signature = self._square_forms()['US-FIRST'].get('data-square-cart-signature')
        route = f'/shop/payment/transaction/{order.id}'
        params = self._prepare_params(order, provider, signature)
        prepared = self._result(route, **params)
        location = provider.square_location_id
        incompatible_location = location.copy({
            'square_id': 'CA-LOCATION', 'country_code': 'CA', 'currency_code': 'CAD',
        })
        replacement_location = location.copy({'square_id': 'US-REPLACEMENT'})

        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            for change in ({'is_published': False}, {
                'is_published': True, 'square_location_id': incompatible_location.id,
            }):
                provider.write(change)
                self.assertFalse(self._square_forms())
                self.assertIn('Square is unavailable', self._rpc(route, **params)['error']['data']['message'])
                self.assertIn('Square is unavailable', self._rpc(
                    '/payment/square/online/submit', **prepared['square_submission'], source_id='cnon:stale-config',
                )['error']['data']['message'])
            provider.square_location_id = replacement_location
            self.assertEqual(set(self._square_forms()), {'US-REPLACEMENT'})
            self.assertIn('Checkout has changed', self._rpc(
                '/payment/square/online/submit', **prepared['square_submission'], source_id='cnon:stale-location',
            )['error']['data']['message'])
            square.assert_not_called()
        self.assertEqual(order.state, 'draft')
        self.assertEqual(order.transaction_ids.square_location_ref, 'US-FIRST')
        self.assertFalse(order.transaction_ids.payment_id)

    def test_two_companies_and_multiple_websites_keep_their_payment_ownership(self):
        self.website.domain = 'http://square-first.example.test'
        website_model = self.env['website']
        sibling = website_model.create({
            'name': 'Square sibling store', 'domain': 'http://square-sibling.example.test',
            'company_id': self.company.id,
        })
        other_company = self.env['res.company'].create({
            'name': 'Square other company', 'currency_id': self.company.currency_id.id,
        })
        self.env['account.chart.template'].try_loading(self.company.chart_template, other_company)
        other_website = website_model.create({
            'name': 'Square other company store', 'domain': 'http://square-other.example.test',
            'company_id': other_company.id,
        })
        first_provider = self._configure_provider(self.company, self.website, 'FIRST', 'US', 'USD')
        other_provider = self._configure_provider(other_company, None, 'OTHER', 'US', 'USD', production=True)
        currency = self.env.ref('base.USD')

        # Keep the two storefront sessions alive simultaneously while each draft is prepared.
        first_order = self._cart(self.website, currency, 40.15, 6.26)
        first_order.carrier_id.website_id = False
        self.assertEqual(set(self._square_forms()), {'FIRST'})
        first_signature = self._square_forms()['FIRST'].get('data-square-cart-signature')
        first_params = self._prepare_params(first_order, first_provider, first_signature)
        first_route = f'/shop/payment/transaction/{first_order.id}'
        first_prepared = self._result(first_route, **first_params)
        first_opener, first_session = self.opener, self.session
        self.session = None

        sibling_order = self._cart(sibling, currency, 40.15, 6.26)
        self.assertFalse(self._square_forms())
        first_provider.website_id = False
        sibling_signature = self._square_forms()['FIRST'].get('data-square-cart-signature')
        first_provider.website_id = self.website
        self.assertFalse(self._square_forms())
        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            self.assertIn('Square is unavailable', self._rpc(
                f'/shop/payment/transaction/{sibling_order.id}',
                **self._prepare_params(sibling_order, first_provider, sibling_signature),
            )['error']['data']['message'])
            square.assert_not_called()

        other_order = self._cart(other_website, currency, 40.15, 6.26)
        self.assertEqual(set(self._square_forms()), {'OTHER'})
        other_prepared = self._prepare(other_order, other_provider, 'OTHER')
        other_opener, other_session = self.opener, self.session

        with patch('odoo.addons.payment_square_online.square_api.request') as square:
            self.assertIn('error', self._rpc(first_route, **first_params))
            self.assertIn('error', self._rpc(
                '/payment/square/online/submit',
                **(other_prepared['square_submission'] | {
                    'transaction_id': first_prepared['square_submission']['transaction_id'],
                }),
                source_id='cnon:swapped-transaction',
            ))
            self.opener, self.session = first_opener, first_session
            self.assertIn('error', self._rpc(first_route, **(first_params | {'provider_id': other_provider.id})))
            self.assertIn('error', self._rpc(
                first_route, **first_params, square_location_id=other_provider.square_location_id.id,
            ))
            # Reuse the correct signed attempt and browser session at another store's URL.
            self.opener.headers['Host'] = 'square-sibling.example.test'
            self.assertIn('current website cart', self._rpc(first_route, **first_params)['error']['data']['message'])
            self.assertIn('error', self._rpc(
                '/payment/square/online/submit', **first_prepared['square_submission'], source_id='cnon:wrong-store',
            ))
            self.opener.headers['Host'] = 'square-first.example.test'
            square.assert_not_called()

        self._complete(first_prepared, 'FIRST', 5043, 'USD')
        self.opener, self.session = other_opener, other_session
        self._complete(other_prepared, 'OTHER', 5043, 'USD', production=True)
        self.assertFalse(sibling_order.transaction_ids)
        for order, provider, location, environment in (
            (first_order, first_provider, 'FIRST', 'sandbox'),
            (other_order, other_provider, 'OTHER', 'production'),
        ):
            with self.subTest(location=location):
                self.assertEqual(order.state, 'sale')
                transaction = order.transaction_ids
                self.assertEqual(transaction.company_id, order.company_id)
                self.assertEqual(transaction.provider_id, provider)
                self.assertEqual(transaction.square_merchant_id, f'MERCHANT-{location}')
                self.assertEqual(transaction.square_environment, environment)
                self.assertEqual(transaction.square_location_ref, location)
                self.assertEqual(transaction.square_currency_code, 'USD')
                self.assertEqual(transaction.payment_id.company_id, order.company_id)
                self.assertEqual(transaction.payment_id.journal_id, provider.journal_id)
                self.assertEqual(order.invoice_ids.amount_residual, 0)

        manager = new_test_user(
            self.env, login='square_other_company_manager', password='square_other_company_manager',
            groups='base.group_system,account.group_account_manager',
            company_id=other_company.id, company_ids=[Command.set(other_company.ids)],
        )
        self._open_store(other_website, manager)
        for record, protected_fields in (
            (first_provider, ['square_merchant_id', 'square_application_secret']),
            (first_provider.square_location_id, ['square_id']),
            (first_order.transaction_ids, ['square_merchant_id', 'square_location_ref']),
        ):
            model = record._name
            response = self._rpc(
                f'/web/dataset/call_kw/{model}/read', model=model, method='read',
                args=[record.ids, protected_fields], kwargs={},
            )
            self.assertEqual(response['error']['data']['name'], 'odoo.exceptions.AccessError')

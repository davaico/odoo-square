# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

from lxml import html

from odoo import Command, fields
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tests import HttpCase, tagged
from odoo.tests.common import JsonRpcException, new_test_user


@tagged("post_install", "-at_install")
class TestSquareOnlineMarkets(HttpCase):
    square_boundary = "odoo.addons.payment_square_online.square_api.requests.request"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env.ref("website.default_website")
        cls.company = cls.website.company_id
        cls.provider = cls.env.ref("payment_square_online.payment_provider_square_online")
        cls.card_method = cls.env.ref("payment.payment_method_card")
        cls.journal = cls.env["account.journal"].search([
            ("company_id", "=", cls.company.id), ("type", "=", "bank"),
        ], limit=1)
        cls.partner = cls.env["res.partner"].create({
            "name": "International Checkout Customer",
            "email": "international@example.test",
            "phone": "+65 6123 4567",
            "street": "20 Raffles Place",
            "city": "Singapore",
            "zip": "048620",
            "country_id": cls.env.ref("base.sg").id,
        })

    @staticmethod
    def _response(body):
        return Mock(status_code=200, json=Mock(return_value=body))

    def _connect(self, company, seller_country, currency, name):
        merchant_id = f"MERCHANT-{name}"
        connection = self.env["square.online.connection"].create({
            "name": name,
            "company_id": company.id,
            "application_id": f"sandbox-sq0idp-{name}",
            "application_secret": "test-app-secret",
        })
        with patch(self.square_boundary, side_effect=[
            self._response({
                "merchant_id": merchant_id,
                "access_token": f"access-{name}",
                "refresh_token": f"refresh-{name}",
                "token_type": "bearer",
                "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }),
            self._response({"locations": [{
                "id": f"LOCATION-{name}",
                "name": name,
                "merchant_id": merchant_id,
                "country": seller_country,
                "currency": currency,
                "status": "ACTIVE",
                "capabilities": ["CREDIT_CARD_PROCESSING"],
            }]}),
        ]):
            connection._square_online_authorize(f"authorization-{name}")
        connection.location_id = connection.available_location_ids
        return connection

    def _enable_provider(self, provider, journal, connection, website=None):
        provider.write({
            "square_online_sandbox_connection_id": connection.id,
            "journal_id": journal.id,
            "website_id": website.id if website else False,
            "state": "test",
            "is_published": True,
        })

    def _make_cart(self, website, currency_code, *, goods_price=23.50):
        company = website.company_id
        currency = self.env.ref(f"base.{currency_code}")
        currency.active = True
        if currency != company.currency_id:
            rates = self.env["res.currency.rate"]
            date = fields.Date.today()
            if not rates.search_count([
                ("currency_id", "=", currency.id), ("company_id", "=", company.id), ("name", "=", date),
            ]):
                rates.create({
                    "currency_id": currency.id, "company_id": company.id, "name": date, "rate": 2.0,
                })
        taxes = self.env["account.tax"].with_company(company)
        tax = taxes.search([("name", "=", "Square market sales tax"), ("company_id", "=", company.id)])
        if not tax:
            tax = taxes.create({
                "name": "Square market sales tax", "company_id": company.id,
                "amount": 10, "amount_type": "percent", "type_tax_use": "sale",
            })
        product, shipping = self.env["product.product"].with_company(company).create([{
            "name": "Square market goods",
            "type": "consu",
            "invoice_policy": "order",
            "taxes_id": [Command.set(tax.ids)],
        }, {
            "name": "Square market shipping",
            "type": "service",
            "taxes_id": [Command.clear()],
        }])
        pricelist = self.env["product.pricelist"].create({
            "name": f"Square market {currency_code}", "currency_id": currency.id,
            "company_id": company.id, "website_id": website.id,
            "item_ids": [
                Command.create({
                    "applied_on": "1_product", "product_tmpl_id": product.product_tmpl_id.id,
                    "compute_price": "fixed", "fixed_price": goods_price,
                }),
                Command.create({
                    "applied_on": "1_product", "product_tmpl_id": shipping.product_tmpl_id.id,
                    "compute_price": "fixed", "fixed_price": 5,
                }),
            ],
        })
        carrier = self.env["delivery.carrier"].with_company(company).create({
            "name": "Square market delivery", "company_id": company.id,
            "product_id": shipping.id, "delivery_type": "fixed", "fixed_price": 5,
            "is_published": True,
        })
        order = self.env["sale.order"].with_company(company).create({
            "company_id": company.id, "website_id": website.id,
            "pricelist_id": pricelist.id, "partner_id": self.partner.id,
            "order_line": [Command.create({"product_id": product.id, "product_uom_qty": 2})],
        })
        order._set_delivery_method(carrier)
        return order

    def _site_headers(self, website):
        if not website.domain:
            return {}
        host = website.domain.removeprefix("https://")
        self.opener.cookies.set("session_id", self.session.sid, domain=host)
        return {"Host": host}

    def _open_payment_page(self, order, *, new_session=True):
        if new_session:
            self.authenticate(None, None, session_extra={"sale_order_id": order.id})
        page = self.url_open("/shop/payment", allow_redirects=False, headers=self._site_headers(order.website_id))
        self.assertEqual(page.status_code, 200, page.headers.get("location"))
        return html.fromstring(page.content)

    def _prepare(self, order, page, provider, *, request_website=None):
        form = page.xpath('//*[@id="o_payment_form"]')[0]
        cart = form.xpath('.//*[@data-square-online-cart]')
        return self.make_jsonrpc_request(f"/shop/payment/transaction/{order.id}", {
            "provider_id": provider.id,
            "payment_method_id": self.card_method.id,
            "amount": order.amount_total,
            "flow": "direct",
            "token_id": None,
            "tokenization_requested": False,
            "landing_route": form.get("data-landing-route"),
            "is_validation": False,
            "access_token": form.get("data-access-token"),
            "square_online_cart": cart[0].get("data-square-online-cart"),
        }, headers=self._site_headers(request_website or order.website_id))

    def _submit(self, draft, website, **extra):
        return self.make_jsonrpc_request("/payment/square_online/submit", {
            "transaction_id": draft["square_online_transaction_id"],
            "access_token": draft["square_online_access_token"],
            "source_id": "cnon:market-test-token",
            **extra,
        }, headers=self._site_headers(website))

    def _assert_checkout_paid(
        self, order, provider, connection, journal, *, amount, minor, company_amount, tax,
        verification_amount, replacement_journal=None,
    ):
        page = self._open_payment_page(order)
        self.assertTrue(page.xpath('.//*[@data-provider-code="square_online"]'))
        draft = self._prepare(order, page, provider)
        currency = order.currency_id.name
        self.assertEqual(draft["square_online_verification"]["amount"], verification_amount)
        self.assertEqual(draft["square_online_verification"]["currencyCode"], currency)
        self.assertEqual(draft["square_online_verification"]["billingContact"]["countryCode"], "SG")

        def capture(method, url, *, headers, json, **kwargs):
            self.assertEqual((method, url), ("POST", "https://connect.squareupsandbox.com/v2/payments"))
            self.assertEqual(headers["Authorization"], f"Bearer access-{connection.name}")
            self.assertEqual(json["location_id"], f"LOCATION-{connection.name}")
            self.assertEqual(json["amount_money"], {"amount": minor, "currency": currency})
            self.assertIs(type(json["amount_money"]["amount"]), int)
            return self._response({"payment": {
                "id": f"PAYMENT-{order.id}",
                "status": "COMPLETED",
                "reference_id": json["reference_id"],
                "location_id": json["location_id"],
                "source_type": "CARD",
                "amount_money": json["amount_money"],
            }})

        with patch(self.square_boundary, side_effect=capture) as external:
            self.assertEqual(self._submit(draft, order.website_id)["state"], "done")
            if replacement_journal:
                provider.journal_id = replacement_journal
            self.assertEqual(self.make_jsonrpc_request(
                "/payment/status/poll", headers=self._site_headers(order.website_id),
            )["state"], "done")
        external.assert_called_once()

        self.env.invalidate_all()
        self.assertEqual(order.amount_tax, tax)
        self.assertEqual(order.amount_delivery, 5)
        self.assertEqual(order.state, "sale")
        self.assertEqual(len(order.transaction_ids), 1)
        tx = order.transaction_ids
        self.assertEqual(tx.company_id, order.company_id)
        self.assertEqual(tx.provider_id, provider)
        self.assertEqual(tx.square_online_connection_id, connection)
        self.assertEqual(tx.square_online_website_id, order.website_id)
        self.assertEqual(tx.square_online_merchant_id, f"MERCHANT-{connection.name}")
        self.assertEqual(tx.square_online_location_id, f"LOCATION-{connection.name}")
        self.assertEqual(tx.square_online_environment, "sandbox")
        self.assertEqual(tx.currency_id, order.currency_id)
        self.assertEqual(tx.square_online_currency_code, currency)
        self.assertEqual(tx.square_online_amount_minor, minor)
        self.assertEqual(tx.amount, amount)

        payment = tx.payment_id
        self.assertEqual(payment.payment_type, "inbound")
        self.assertEqual(payment.company_id, order.company_id)
        self.assertEqual(payment.journal_id, journal)
        self.assertEqual(payment.currency_id, order.currency_id)
        self.assertEqual(payment.amount, amount)
        self.assertEqual(payment.move_id.state, "posted")
        invoice = order.invoice_ids
        self.assertEqual(invoice.state, "posted")
        self.assertEqual(invoice.currency_id, order.currency_id)
        self.assertEqual(invoice.amount_total, amount)
        self.assertEqual(invoice.amount_residual, 0)
        receivable = payment.move_id.line_ids.filtered(lambda line: line.account_id.account_type == "asset_receivable")
        invoice_receivable = invoice.line_ids.filtered(lambda line: line.account_id.account_type == "asset_receivable")
        self.assertTrue(receivable.reconciled)
        self.assertTrue(invoice_receivable.reconciled)
        self.assertEqual((receivable.amount_currency, receivable.balance), (-amount, -company_amount))
        self.assertEqual((invoice_receivable.amount_currency, invoice_receivable.balance), (amount, company_amount))
        outstanding = payment.move_id.line_ids - receivable
        self.assertEqual((outstanding.amount_currency, outstanding.balance), (amount, company_amount))

    def test_all_seller_currencies_charge_and_reconcile_the_full_website_total(self):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        self.assertEqual(self.company.currency_id.name, "USD")
        self.assertFalse(self.journal.currency_id)
        for seller, currency, goods_price, tax, total, minor, company_amount, verification in (
            ("AU", "AUD", 23.50, 4.70, 56.70, 5670, 28.35, "56.70"),
            ("CA", "CAD", 23.50, 4.70, 56.70, 5670, 28.35, "56.70"),
            ("FR", "EUR", 23.50, 4.70, 56.70, 5670, 28.35, "56.70"),
            ("GB", "GBP", 23.50, 4.70, 56.70, 5670, 28.35, "56.70"),
            ("JP", "JPY", 25, 5, 60, 60, 30, "60"),
            ("US", "USD", 23.50, 4.70, 56.70, 5670, 56.70, "56.70"),
        ):
            with self.subTest(currency=currency):
                connection = self._connect(self.company, seller, currency, currency)
                self._enable_provider(self.provider, self.journal, connection)
                order = self._make_cart(self.website, currency, goods_price=goods_price)
                self._assert_checkout_paid(
                    order, self.provider, connection, self.journal, amount=total, minor=minor,
                    company_amount=company_amount, tax=tax, verification_amount=verification,
                )

    def test_checkout_hides_and_rejects_ineligible_square_configuration(self):
        usd = self._connect(self.company, "US", "USD", "eligible")
        cad = self._connect(self.company, "CA", "CAD", "incompatible")
        order = self._make_cart(self.website, "USD")

        for field, unavailable in (
            ("state", "disabled"),
            ("is_published", False),
            ("square_online_sandbox_connection_id", cad.id),
        ):
            with self.subTest(field=field), patch(self.square_boundary) as external:
                self._enable_provider(self.provider, self.journal, usd)
                page = self._open_payment_page(order)
                draft = self._prepare(order, page, self.provider)
                self.provider.write({field: unavailable})

                with self.assertRaises(JsonRpcException):
                    self._submit(draft, self.website)
                with self.assertRaises(JsonRpcException):
                    self._prepare(order, page, self.provider)
                current_page = self._open_payment_page(order, new_session=False)
                self.assertFalse(current_page.xpath('.//*[@data-provider-code="square_online"]'))
                self.env.invalidate_all()
                self.assertEqual(order.state, "draft")
                self.assertFalse(order.transaction_ids.filtered("square_online_dispatch_at"))
                self.assertFalse(order.transaction_ids.payment_id)
                external.assert_not_called()

    def test_website_checkouts_keep_companies_locations_and_journals_separate(self):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        eur = self.env.ref("base.EUR")
        eur.active = True
        company_b = self.env["res.company"].create({
            "name": "Square Company B", "country_id": self.env.ref("base.us").id,
        })
        self.env["account.chart.template"].try_loading(self.company.chart_template, company_b)
        journal_b = self.env["account.journal"].search([
            ("type", "=", "bank"), ("company_id", "=", company_b.id),
        ], limit=1)
        journal_b.currency_id = eur

        other_website = self.env["website"].create({
            "name": "Square Website B", "domain": "https://company-b.example.test", "company_id": company_b.id,
        })
        sibling_website = self.env["website"].create({
            "name": "Square Sibling Website", "domain": "https://sibling.example.test", "company_id": self.company.id,
        })
        self.assertFalse(self.card_method.active)
        provider_b = self.provider.with_company(company_b).copy(default={
            "name": "Square Company B", "company_id": company_b.id,
        })
        self.assertEqual(provider_b.with_context(active_test=False).payment_method_ids, self.card_method)
        connection_a = self._connect(self.company, "US", "USD", "company-a")
        connection_b = self._connect(company_b, "FR", "EUR", "company-b")
        self._enable_provider(self.provider, self.journal, connection_a, self.website)
        self._enable_provider(provider_b, journal_b, connection_b, other_website)

        with self.assertRaises(UserError):
            self.provider.square_online_sandbox_connection_id = connection_b
        with self.assertRaises(UserError):
            self.provider.website_id = other_website
        wrong_journal = self.env["account.journal"].with_company(company_b).create({
            "name": "Fixed GBP bank journal", "code": "FGBP", "type": "bank",
            "currency_id": self.env.ref("base.GBP").id,
        })
        with self.assertRaisesRegex(UserError, "fixed journal currency"), self.env.cr.savepoint():
            provider_b.journal_id = wrong_journal

        order_a = self._make_cart(self.website, "USD")
        order_b = self._make_cart(other_website, "EUR")
        sibling_order = self._make_cart(sibling_website, "USD")
        sibling_page = self._open_payment_page(sibling_order)
        self.assertFalse(sibling_page.xpath('.//*[@data-provider-code="square_online"]'))
        self.provider.website_id = False
        sibling_page = self._open_payment_page(sibling_order)
        self.assertTrue(sibling_page.xpath('.//*[@data-provider-code="square_online"]'))
        self.provider.website_id = self.website

        with patch(self.square_boundary) as external:
            page_b = self._open_payment_page(order_b)
            self.assertTrue(page_b.xpath('.//*[@data-provider-code="square_online"]'))
            page_a = self._open_payment_page(order_a)
            with self.assertRaises(JsonRpcException):
                self._prepare(order_a, page_a, provider_b)
            with self.assertRaises(JsonRpcException):
                self._prepare(order_b, page_b, provider_b, request_website=self.website)
            with self.assertRaises(JsonRpcException):
                self._prepare(order_a, page_a, self.provider, request_website=sibling_website)

            draft_a = self._prepare(order_a, page_a, self.provider)
            with self.assertRaises(JsonRpcException):
                self._submit(draft_a, sibling_website)
            with self.assertRaises(JsonRpcException):
                self._submit(draft_a, self.website, location_id="LOCATION-company-b")
            self._open_payment_page(order_b)
            with self.assertRaises(JsonRpcException):
                self._submit(draft_a, other_website)
            external.assert_not_called()

        # Use a new cart on A so neither checkout relies on a previous draft.
        paid_a = self._make_cart(self.website, "USD")
        self._assert_checkout_paid(
            paid_a, self.provider, connection_a, self.journal, amount=56.70, minor=5670,
            company_amount=56.70, tax=4.70, verification_amount="56.70",
        )
        replacement_journal = self.env["account.journal"].with_company(company_b).create({
            "name": "New bank journal", "code": "NBK", "type": "bank",
        })
        self._assert_checkout_paid(
            order_b, provider_b, connection_b, journal_b, amount=56.70, minor=5670,
            company_amount=28.35, tax=4.70, verification_amount="56.70",
            replacement_journal=replacement_journal,
        )
        self.assertEqual(provider_b.journal_id, replacement_journal)

        administrator = new_test_user(
            self.env, login="square-market-admin-a", groups="base.group_system,account.group_account_manager",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )
        accounting_manager = new_test_user(
            self.env, login="square-market-accounting-a", groups="account.group_account_manager",
            company_id=self.company.id, company_ids=[Command.set(self.company.ids)],
        )

        def own(records):
            return records.with_user(administrator).with_context(allowed_company_ids=self.company.ids)

        self.assertEqual(own(connection_a).read(["merchant_id"])[0]["merchant_id"], "MERCHANT-company-a")
        self.assertEqual(own(paid_a.transaction_ids).read(["state"])[0]["state"], "done")
        for records in (connection_b, connection_b.location_id, order_b.transaction_ids):
            with self.assertRaises(AccessError):
                own(records).read()
        with self.assertRaises(AccessError):
            connection_a.with_user(accounting_manager).read(["merchant_id"])

        detached = {
            "square_online_sandbox_connection_id": False, "square_online_production_connection_id": False,
            "website_id": False, "journal_id": False, "state": "disabled",
        }
        for reassignment in ({"company_id": company_b.id}, {"code": "none"}):
            with self.assertRaisesRegex(
                ValidationError, "must keep its company and provider type",
            ), self.env.cr.savepoint():
                self.provider.write({**detached, **reassignment})
        self.assertEqual(paid_a.transaction_ids.company_id, self.company)
        self.assertEqual(paid_a.transaction_ids.provider_code, "square_online")

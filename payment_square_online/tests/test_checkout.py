# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import requests
from lxml import html

from odoo import Command
from odoo.tests import HttpCase, tagged
from odoo.tests.common import JsonRpcException, new_test_user


@tagged("post_install", "-at_install")
class TestSquareOnlineCheckout(HttpCase):
    square_boundary = "odoo.addons.payment_square_online.square_api.requests.request"
    submit_route = "/payment/square_online/submit"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env.ref("website.default_website")
        cls.provider = cls.env.ref("payment_square_online.payment_provider_square_online")
        cls.company = cls.provider.company_id
        cls.currency = cls.company.currency_id
        cls.partner = cls.env["res.partner"].create({
            "name": "Riley Checkout",
            "email": "riley@example.test",
            "phone": "+1 555 555 0199",
            "street": "25 Main Street",
            "city": "Albany",
            "zip": "12201",
            "country_id": cls.env.ref("base.us").id,
            "state_id": cls.env.ref("base.state_us_27").id,
        })
        cls.portal = new_test_user(
            cls.env, login="square-checkout-customer", groups="base.group_portal",
            partner_id=cls.partner.id, name=cls.partner.name, email=cls.partner.email,
        )
        cls.connection = cls.env["square.online.connection"].create({
            "name": "Checkout Sandbox",
            "company_id": cls.company.id,
            "application_id": "sandbox-sq0idp-example",
            "application_secret": "square-checkout-test-secret",
        })
        with patch(cls.square_boundary) as external:
            external.side_effect = [
                cls._square_response({
                    "merchant_id": "MERCHANT_TEST",
                    "access_token": "checkout-access-test",
                    "refresh_token": "checkout-refresh-test",
                    "token_type": "bearer",
                    "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
                }),
                cls._square_response({"locations": [{
                    "id": "SQUARE-NY",
                    "name": "Checkout Store",
                    "merchant_id": "MERCHANT_TEST",
                    "country": "US",
                    "currency": "USD",
                    "status": "ACTIVE",
                    "capabilities": ["CREDIT_CARD_PROCESSING"],
                }]}),
            ]
            cls.connection._square_online_authorize("checkout-fixture")
        cls.connection.location_id = cls.connection.available_location_ids
        cls.journal = cls.env["account.journal"].search([
            ("company_id", "=", cls.company.id), ("type", "=", "bank"),
        ], limit=1)
        cls.provider.write({
            "square_online_sandbox_connection_id": cls.connection.id,
            "journal_id": cls.journal.id,
            "state": "test",
            "is_published": True,
        })
        cls.method = cls.env.ref("payment.payment_method_card")
        cls.product = cls.env["product.product"].create({
            "name": "Website test service",
            "type": "service",
            "list_price": 23.50,
            "website_published": True,
            "taxes_id": [Command.clear()],
        })

    @staticmethod
    def _square_response(body, status=200):
        return Mock(status_code=status, json=Mock(return_value=body))

    def _make_order(self, product=None):
        return self.env["sale.order"].create({
            "partner_id": self.partner.id,
            "website_id": self.website.id,
            "order_line": [Command.create({"product_id": (product or self.product).id, "product_uom_qty": 2})],
        })

    def _open_payment_page(self, order):
        self.authenticate(None, None, session_extra={"sale_order_id": order.id})
        page = self.url_open("/shop/payment", allow_redirects=False)
        self.assertEqual(page.status_code, 200, page.headers.get("location"))
        form = html.fromstring(page.content).xpath('//*[@id="o_payment_form"]')[0]
        square = form.xpath('.//*[@data-provider-code="square_online"]')
        self.assertTrue(square, "An eligible checkout offers Square")
        return form

    def _prepare(self, order, form, **overrides):
        return self.make_jsonrpc_request(f"/shop/payment/transaction/{order.id}", {
            "provider_id": self.provider.id,
            "payment_method_id": self.method.id,
            "amount": order.amount_total,
            "flow": "direct",
            "token_id": None,
            "tokenization_requested": False,
            "landing_route": form.get("data-landing-route"),
            "is_validation": False,
            "access_token": form.get("data-access-token"),
            "square_online_cart": form.xpath('.//*[@data-square-online-cart]')[0].get(
                "data-square-online-cart"
            ),
            **overrides,
        })

    def _submit(self, draft, **overrides):
        return self.make_jsonrpc_request(self.submit_route, {
            "transaction_id": draft["square_online_transaction_id"],
            "access_token": draft["square_online_access_token"],
            "source_id": "cnon:checkout-test-token",
            **overrides,
        })

    def _completed_payment(self, method, url, *, headers, json, **kwargs):
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://connect.squareupsandbox.com/v2/payments")
        self.assertEqual(headers["Authorization"], "Bearer checkout-access-test")
        self.assertEqual(json["source_id"], "cnon:checkout-test-token")
        self.assertEqual(json["location_id"], "SQUARE-NY")
        self.assertEqual(json["idempotency_key"], json["reference_id"])
        self.assertTrue(json["autocomplete"])
        self.assertFalse(json["accept_partial_authorization"])
        return self._square_response({"payment": {
            "id": "SQUARE-PAYMENT-TEST",
            "status": "COMPLETED",
            "reference_id": json["reference_id"],
            "location_id": "SQUARE-NY",
            "source_type": "CARD",
            "amount_money": json["amount_money"],
            "order_id": "SQUARE-ORDER-TEST",
        }})

    @patch(square_boundary)
    def test_native_website_checkout_captures_and_accounts_once(self, external):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "True")
        order = self._make_order()
        form = self._open_payment_page(order)
        draft = self._prepare(order, form)
        external.assert_not_called()
        external.side_effect = self._completed_payment
        self.assertEqual(self._submit(draft)["state"], "done")
        self.assertEqual(self._submit(draft)["state"], "done")
        external.assert_called_once()
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()
        self.assertEqual(order.state, "sale")
        self.assertEqual(len(order.transaction_ids), 1)
        tx = order.transaction_ids
        self.assertEqual(tx.provider_reference, "SQUARE-PAYMENT-TEST")
        self.assertEqual(tx.payment_id.amount, 47)
        self.assertEqual(tx.payment_id.journal_id, self.journal)
        self.assertEqual(order.invoice_ids.state, "posted")
        self.assertEqual(order.invoice_ids.amount_residual, 0)

    @patch(square_boundary)
    def test_checkout_captures_tax_and_shipping_without_completing_stock_delivery(self, external):
        self.env["ir.config_parameter"].set_param("sale.automatic_invoice", "False")
        tax = self.env["account.tax"].create({
            "name": "Square checkout sales tax",
            "amount": 10.0,
            "amount_type": "percent",
            "type_tax_use": "sale",
            "company_id": self.company.id,
        })
        goods = self.env["product.product"].create({
            "name": "Square checkout physical goods",
            "type": "consu",
            "is_storable": True,
            "list_price": 23.50,
            "website_published": True,
            "taxes_id": [Command.set(tax.ids)],
        })
        carrier = self.env["delivery.carrier"].create({
            "name": "Square checkout shipping",
            "product_id": self.product.id,
            "delivery_type": "fixed",
            "fixed_price": 5.0,
            "is_published": True,
        })
        order = self._make_order(goods)
        order._set_delivery_method(carrier)
        form = self._open_payment_page(order)
        draft = self._prepare(order, form)
        external.side_effect = self._completed_payment
        self.assertEqual(self._submit(draft)["state"], "done")
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()
        self.assertEqual(order.amount_tax, 4.70)
        self.assertEqual(order.amount_delivery, 5)
        self.assertEqual(external.call_args.kwargs["json"]["amount_money"], {"amount": 5670, "currency": "USD"})
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.transaction_ids.payment_id.amount, 56.70)
        self.assertFalse(order.invoice_ids)
        self.assertTrue(order.picking_ids)
        self.assertFalse(order.picking_ids.filtered(lambda picking: picking.state == "done"))
        self.assertEqual(order.order_line.filtered(lambda line: not line.is_delivery).qty_delivered, 0)
        order.action_cancel()
        self.assertEqual(order.state, "cancel")
        self.assertEqual(order.transaction_ids.state, "done")
        external.assert_called_once()

    @patch(square_boundary)
    def test_definitive_decline_allows_a_new_attempt(self, external):
        order = self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))
        external.return_value = self._square_response({"errors": [{
            "code": "CARD_DECLINED", "detail": "private response detail",
        }]}, 400)
        failure = self._submit(draft)
        self.assertEqual(failure["state"], "error")
        self.assertIn("card was declined", failure["state_message"])
        self.assertNotIn("private response detail", failure["state_message"])
        self.assertEqual(self._submit(draft)["state"], "error")
        self.env.invalidate_all()
        self.assertEqual(order.state, "draft")
        self.assertFalse(order.transaction_ids.payment_id)
        external.assert_called_once()
        old_key = external.call_args.kwargs["json"]["idempotency_key"]

        replacement = self._prepare(order, self._open_payment_page(order))
        external.side_effect = self._completed_payment
        self.assertEqual(self._submit(replacement)["state"], "done")
        self.assertNotEqual(external.call_args.kwargs["json"]["idempotency_key"], old_key)
        self.assertEqual(external.call_count, 2)

    @patch(square_boundary)
    def test_lost_square_response_keeps_an_unpaid_order_processing(self, external):
        order = self._make_order()
        form = self._open_payment_page(order)
        draft = self._prepare(order, form)
        external.side_effect = requests.Timeout
        self.assertEqual(self._submit(draft)["state"], "pending")
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "pending")
        self.assertEqual(self._submit(draft)["state"], "pending")
        with self.assertRaises(JsonRpcException):
            self._prepare(order, form)
        self.env.invalidate_all()
        tx = order.transaction_ids
        self.assertEqual(tx.state, "pending")
        self.assertTrue(tx.square_online_dispatch_at)
        self.assertEqual(tx.square_online_operation_key, external.call_args.kwargs["json"]["idempotency_key"])
        self.assertFalse(tx.is_post_processed)
        self.assertEqual(order.state, "draft")
        self.assertFalse(tx.payment_id)
        self.assertFalse(order.invoice_ids)
        external.assert_called_once()

    @patch(square_boundary)
    def test_tampered_or_stale_checkout_never_reaches_square(self, external):
        order = self._make_order()
        form = self._open_payment_page(order)
        for override in (
            {"amount": 4.70}, {"amount": 47.001}, {"amount": 0},
            {"access_token": "wrong-order-token"},
            {"square_online_cart": "a" * 64},
        ):
            with self.subTest(override=override), self.assertRaises(JsonRpcException):
                self._prepare(order, form, **override)
        draft = self._prepare(order, form)
        with self.assertRaises(JsonRpcException):
            self._submit(draft, access_token="a" * 64)
        with self.assertRaises(JsonRpcException):
            self._submit(draft, source_id="4111111111111111")
        self.env.invalidate_all()
        # Update the contents and keep the exact same total. The earlier card challenge is stale.
        order.order_line.name = "Changed line configuration"
        with self.assertRaises(JsonRpcException):
            self._submit(draft)
        with self.assertRaises(JsonRpcException):
            self._prepare(order, form)
        other_session = self.authenticate(None, None)
        with self.assertRaises(JsonRpcException):
            self._submit(draft)
        self.assertNotIn("__payment_monitored_tx_id__", other_session)
        self.env.invalidate_all()
        self.assertEqual(order.state, "draft")
        self.assertFalse(order.transaction_ids.filtered("square_online_dispatch_at"))
        external.assert_not_called()

    def fetch_proxy(self, url):
        if url != "https://sandbox.web.squarecdn.com/v1/square.js":
            return super().fetch_proxy(url)
        return self.make_fetch_proxy_response("""
            window.Square = {
                payments(applicationId, locationId) {
                    if (applicationId !== "sandbox-sq0idp-example" || locationId !== "SQUARE-NY") {
                        throw new Error("Wrong Square SDK configuration");
                    }
                    return {
                        async card() {
                            return {
                                async attach(selector) {
                                    const frame = document.createElement("iframe");
                                    frame.id = "square-sdk-card";
                                    frame.style.width = "100%";
                                    frame.title = "Square secure card";
                                    document.querySelector(selector).append(frame);
                                },
                                async destroy() {},
                                async tokenize(verification) {
                                    if (
                                        verification.amount !== "47.00" || verification.currencyCode !== "USD"
                                        || verification.intent !== "CHARGE"
                                        || !verification.customerInitiated || verification.sellerKeyedIn
                                        || verification.billingContact.email !== "riley@example.test"
                                    ) {
                                        throw new Error("Wrong Square cardholder verification details");
                                    }
                                    return new Promise((resolve) => {
                                        const button = document.createElement("button");
                                        button.id = "square-sdk-authenticate";
                                        button.type = "button";
                                        button.textContent = "Complete bank verification";
                                        button.onclick = () => {
                                            button.remove();
                                            resolve(window.squareCancelVerification
                                                ? {status: "Cancel"}
                                                : {status: "OK", token: "cnon:checkout-test-token"});
                                        };
                                        document.querySelector("[data-square-online-cart]").append(button);
                                    });
                                },
                            };
                        },
                    };
                },
            };
        """)

    @patch(square_boundary)
    def test_mobile_browser_completes_bank_verification_and_submits_the_embedded_card(self, external):
        order = self._make_order()
        external.side_effect = self._completed_payment
        self.browser_size = "390,844"
        self.browser_js(
            "/shop/payment",
            """
            (async () => {
                async function waitFor(selector, test = () => true) {
                    for (let i = 0; i < 100; i++) {
                        const element = Array.from(document.querySelectorAll(selector)).find(test);
                        if (element) {
                            return element;
                        }
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                    throw new Error("Missing checkout element " + selector + " | " +
                        document.querySelector(".modal-dialog")?.textContent);
                }
                const card = await waitFor("#square-sdk-card");
                const rect = card.getBoundingClientRect();
                if (rect.width === 0 || rect.right > document.documentElement.clientWidth) {
                    throw new Error("Square card overflows the mobile checkout");
                }
                const pay = await waitFor('[name="o_payment_submit_button"]:enabled');
                window.squareCancelVerification = true;
                pay.click();
                (await waitFor("#square-sdk-authenticate")).click();
                const failure = await waitFor(".modal-title", title =>
                    title.textContent.includes("Card verification was unsuccessful")
                );
                failure.closest(".modal-dialog").querySelector("button.btn-primary").click();
                window.squareCancelVerification = false;
                (await waitFor('[name="o_payment_submit_button"]:enabled')).click();
                const authenticate = await waitFor("#square-sdk-authenticate");
                const send = XMLHttpRequest.prototype.send;
                XMLHttpRequest.prototype.send = function (body) {
                    if (typeof body === "string" && body.includes("cnon:checkout-test-token")) {
                        this.addEventListener("load", () => {
                            const response = typeof this.response === "string"
                                ? JSON.parse(this.response)
                                : this.response;
                            if (response?.result?.state === "done") {
                                console.log("test successful");
                            } else {
                                console.error("The embedded checkout did not complete");
                            }
                        }, {once: true});
                    }
                    return send.call(this, body);
                };
                authenticate.click();
            })();
            """,
            login=self.portal.login,
            ready="Boolean(document.querySelector('#square-sdk-card'))",
        )
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "done")
        self.env.invalidate_all()
        external.assert_called_once()
        self.assertEqual(order.state, "sale")
        self.assertEqual(order.transaction_ids.payment_id.amount, 47)
        self.assertEqual(set(order.transaction_ids.mapped("state")), {"draft", "done"})

    @patch(square_boundary)
    def test_square_approval_leaves_order_unpaid_until_full_capture(self, external):
        order = self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))

        def approve(*args, **kwargs):
            response = self._completed_payment(*args, **kwargs)
            response.json()["payment"]["status"] = "APPROVED"
            return response

        external.side_effect = approve
        self.assertEqual(self._submit(draft)["state"], "pending")
        self.assertEqual(self.make_jsonrpc_request("/payment/status/poll")["state"], "pending")
        self.env.invalidate_all()
        self.assertEqual(order.state, "draft")
        tx = order.transaction_ids
        self.assertEqual(tx.provider_reference, "SQUARE-PAYMENT-TEST")
        self.assertFalse(tx.payment_id)
        self.assertFalse(tx.is_post_processed)
        external.assert_called_once()

    @patch(square_boundary)
    def test_mismatched_square_capture_keeps_order_unpaid_and_blocks_retry(self, external):
        order = self._make_order()
        draft = self._prepare(order, self._open_payment_page(order))

        def mismatched_capture(*args, **kwargs):
            response = self._completed_payment(*args, **kwargs)
            response.json()["payment"]["amount_money"] = {"amount": 46, "currency": "USD"}
            return response

        external.side_effect = mismatched_capture
        self.assertEqual(self._submit(draft)["state"], "pending")
        self.assertEqual(self._submit(draft)["state"], "pending")
        self.env.invalidate_all()
        self.assertEqual(order.state, "draft")
        self.assertFalse(order.transaction_ids.provider_reference)
        self.assertFalse(order.transaction_ids.payment_id)
        external.assert_called_once()

# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import re

from werkzeug.exceptions import NotFound

from odoo import _
from odoo.exceptions import ValidationError
from odoo.http import Controller, request, route
from odoo.tools import consteq

from odoo.addons.payment.controllers.post_processing import PaymentPostProcessing
from odoo.addons.website_sale.controllers.main import WebsiteSale
from odoo.addons.website_sale.controllers.payment import PaymentPortal
from odoo.addons.website_sale.models.website import CART_SESSION_CACHE_KEY

from ..square_api import square_amount_in_minor_units


_SOURCE_TOKEN = re.compile(r"cnon:[A-Za-z0-9_.:-]{1,187}")


class SquareOnlineShop(WebsiteSale):

    def _get_shop_payment_values(self, order, **kwargs):
        request.update_context(square_online_website_checkout=True)
        return super()._get_shop_payment_values(order, **kwargs)


class SquareOnlinePaymentPortal(PaymentPortal):

    @route()
    def shop_payment_transaction(self, order_id, access_token, square_online_cart=None, **kwargs):
        provider_id = kwargs.get("provider_id")
        if type(provider_id) is int:
            provider = request.env["payment.provider"].sudo().browse(provider_id).exists()
            if provider.code == "square_online":
                amount = kwargs.get("amount")
                if (
                    kwargs.get("flow") != "direct"
                    or type(kwargs.get("payment_method_id")) is not int
                    or kwargs.get("token_id")
                    or kwargs.get("tokenization_requested")
                    or kwargs.get("is_validation")
                    or type(amount) not in (int, float)
                    or not 0 < amount <= 2147483647
                    or not isinstance(square_online_cart, str)
                    or len(square_online_cart) != 64
                ):
                    raise ValidationError(_("Refresh the checkout page to pay with Square."))
                kwargs["landing_route"] = "/shop/payment/validate"
                request.update_context(
                    square_online_cart=square_online_cart, square_online_cart_amount=amount,
                )
        return super().shop_payment_transaction(order_id, access_token, **kwargs)

    def _validate_transaction_for_order(self, transaction, order):
        super()._validate_transaction_for_order(transaction, order)
        if order._square_online_block_conflicting_checkout(transaction):
            raise ValidationError(_("A payment for this order is already being processed."))
        if transaction.provider_code == "square_online":
            user = request.env.user
            if (
                request.session.get(CART_SESSION_CACHE_KEY) != order.id
                or (not user._is_public() and user.partner_id.commercial_partner_id
                    != order.partner_id.commercial_partner_id)
                or not consteq(
                    order._square_online_cart_token(), request.env.context.get("square_online_cart", "")
                )
            ):
                raise ValidationError(_("Your checkout changed. Refresh the checkout page."))
            try:
                requested_minor = square_amount_in_minor_units(
                    request.env.context.get("square_online_cart_amount"), order.currency_id.name,
                )
                order_minor = square_amount_in_minor_units(order.amount_total, order.currency_id.name)
            except ValueError:
                raise ValidationError(_("The cart total is invalid. Refresh the checkout page.")) from None
            if requested_minor != order_minor:
                raise ValidationError(_("The cart total changed. Refresh the checkout page."))
            transaction._square_online_prepare_checkout(order, request.website.id)
            request.session["sale_last_order_id"] = order.id
        # The native checkout route already holds the order lock.
        order._square_online_mark_checkout_change()


class SquareOnlineCheckout(Controller):

    @route("/payment/square_online/submit", type="jsonrpc", auth="public", website=True, methods=["POST"])
    def submit(self, transaction_id, access_token, source_id, **kwargs):
        if (
            type(transaction_id) is not int
            or transaction_id != request.session.get(PaymentPostProcessing.MONITORED_TX_ID_KEY)
            or not isinstance(access_token, str)
            or len(access_token) != 64
        ):
            raise NotFound()
        transaction = request.env["payment.transaction"].sudo().browse(transaction_id).exists()
        if (
            not transaction
            or transaction.provider_code != "square_online"
            or not transaction.square_online_operation_key
            or transaction.square_online_website_id != request.website
            or not consteq(transaction._square_online_submit_token(), access_token)
        ):
            raise NotFound()
        if kwargs or not isinstance(source_id, str) or not _SOURCE_TOKEN.fullmatch(source_id):
            raise ValidationError(_("Check your card details and try again."))
        return transaction._square_online_submit(
            request.website.id, request.session.get(CART_SESSION_CACHE_KEY), source_id,
        )

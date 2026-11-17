import math
import re

from odoo import _
from odoo.exceptions import ValidationError
from odoo.http import request, route

from odoo.addons.payment.controllers.post_processing import PaymentPostProcessing
from odoo.addons.website_sale.controllers.main import WebsiteSale
from odoo.addons.website_sale.controllers.payment import PaymentPortal


class SquareWebsiteSale(WebsiteSale):

    def _get_shop_payment_values(self, order, **kwargs):
        request.update_context(square_online_checkout=True)
        return super()._get_shop_payment_values(order, **kwargs)


class SquarePaymentPortal(PaymentPortal):

    @route()
    def shop_payment_transaction(self, order_id, access_token, square_cart_signature=None, **kwargs):
        amount = kwargs.get('amount')
        if square_cart_signature is not None and (
            type(amount) not in (int, float) or amount <= 0
            or (type(amount) is float and not math.isfinite(amount))
        ):
            raise ValidationError(_("The cart amount is invalid. Please refresh checkout."))
        request.update_context(square_cart_signature=square_cart_signature)
        return super().shop_payment_transaction(order_id, access_token, **kwargs)

    def _validate_transaction_for_order(self, transaction, sale_order):
        super()._validate_transaction_for_order(transaction, sale_order)
        if transaction.provider_code != 'square_online':
            return
        if (
            sale_order.id != request.session.get('sale_order_id')
            or not sale_order._square_order_matches_checkout(request.website, request.env.user)
        ):
            raise ValidationError(_("Square can only pay for your current website cart."))
        transaction._square_prepare_checkout(sale_order, request.env.context.get('square_cart_signature'))

    @route(
        '/payment/square/online/submit', type='json', auth='public', website=True, methods=['POST'],
    )
    def square_submit(self, transaction_id, submission_token, source_id):
        if (
            type(transaction_id) is not int or not isinstance(submission_token, str)
            or not isinstance(source_id, str) or not re.fullmatch(r'cnon:[!-~]{1,1024}', source_id)
        ):
            raise ValidationError(_("Invalid Square card details. Please re-enter your card."))

        status, order_id = request.env['payment.transaction'].browse(transaction_id)._square_submit_checkout(
            submission_token, source_id, website_id=request.website.id, session_id=request.session.sid,
            cart_id=request.session.get('sale_order_id'),
        )
        if order_id:
            PaymentPostProcessing.monitor_transaction(request.env['payment.transaction'].browse(transaction_id))
            request.session['__website_sale_last_tx_id'] = transaction_id
            request.session['sale_last_order_id'] = order_id
        return status

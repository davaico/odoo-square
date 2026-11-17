import math
import re

from odoo import _, api, fields
from odoo.exceptions import ValidationError
from odoo.http import request, route

from odoo.addons.payment import utils as payment_utils
from odoo.addons.payment.controllers.post_processing import PaymentPostProcessing
from odoo.addons.website_sale.controllers.main import WebsiteSale
from odoo.addons.website_sale.controllers.payment import PaymentPortal

from ..square_api import SquareAPIError, square_request


# These errors reject the card operation. A reused token, an idempotency conflict, an
# unrecognized error, or a transport failure do not prove that no funds were collected.
_CARD_REJECTIONS = {
    'ADDRESS_VERIFICATION_FAILURE', 'CARD_DECLINED', 'CARD_DECLINED_CALL_ISSUER',
    'CARD_DECLINED_VERIFICATION_REQUIRED', 'CARD_EXPIRED', 'CARD_NOT_SUPPORTED',
    'CARD_TOKEN_EXPIRED', 'CARDHOLDER_INSUFFICIENT_PERMISSIONS', 'CVV_FAILURE',
    'EXPIRATION_FAILURE', 'GENERIC_DECLINE', 'INSUFFICIENT_FUNDS', 'INVALID_ACCOUNT',
    'INVALID_CARD', 'INVALID_CARD_DATA', 'INVALID_EXPIRATION', 'INVALID_POSTAL_CODE',
    'PAN_FAILURE', 'PAYMENT_LIMIT_EXCEEDED', 'TRANSACTION_LIMIT', 'VOICE_FAILURE',
}


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
            or not self._square_order_matches_website_and_user(sale_order)
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

        # The native payment route has already committed the draft. A dedicated cursor commits
        # the order reservation before any external call, independently of the HTTP request.
        website = request.website
        user = request.env.user
        website_id, company_id = website.id, website.company_id.id
        commercial_partner_id = user.partner_id.commercial_partner_id.id if not user._is_public() else None
        with request.env.registry.cursor() as cr:
            env = api.Environment(cr, request.env.uid, request.env.context)
            tx = env['payment.transaction'].sudo().browse(transaction_id).exists()
            if not tx or not tx.square_order_reference or not payment_utils.check_access_token(
                submission_token, tx.id, tx.square_order_reference, request.session.sid,
            ):
                raise ValidationError(_("This payment attempt is not available. Please refresh checkout."))
            order = tx.sale_order_ids
            if (
                len(order) != 1 or order.website_id.id != website_id or order.company_id.id != company_id
                or (commercial_partner_id and order.partner_id.commercial_partner_id.id != commercial_partner_id)
            ):
                raise ValidationError(_("This payment attempt is not available. Please refresh checkout."))

            order._square_lock_checkout()
            if tx.state != 'draft':
                if tx.state in ('pending', 'done'):
                    self._square_monitor(tx.id, order.id)
                return self._square_status(tx)
            if order.id != request.session.get('sale_order_id'):
                raise ValidationError(_("This cart is no longer current. Please refresh checkout."))

            access_token = tx._square_claim_checkout(order)
            environment = tx.square_environment
            reference = tx.square_order_reference
            location = tx.square_location_ref
            money = tx._square_money()
            order_id = order.id

        payment_order_id = self._square_create_order(environment, access_token, reference, location, money)
        # Keep the Square order ID and dispatch time durable even if the worker stops immediately
        # after sending CreatePayment. A repeated browser request only reads the same attempt.
        with request.env.registry.cursor() as cr:
            tx = self._square_lock_attempt(cr, transaction_id)
            if tx.state != 'pending' or tx.square_dispatched_at:
                return self._square_status(tx)
            if not payment_order_id:
                tx._set_error(_("Could not start the Square payment. No payment was sent. Please try again."))
                return self._square_status(tx)
            tx.write({'square_order_id': payment_order_id, 'square_dispatched_at': fields.Datetime.now()})

        self._square_monitor(transaction_id, order_id)
        try:
            response = square_request(environment, 'POST', '/v2/payments', access_token=access_token, payload={
                'idempotency_key': f'pay-{reference}',
                'reference_id': reference,
                'source_id': source_id,
                'location_id': location,
                'order_id': payment_order_id,
                'amount_money': money,
                'autocomplete': True,
                'accept_partial_authorization': False,
            })
        except SquareAPIError as error:
            if error.status in (400, 402) and error.codes and set(error.codes) <= _CARD_REJECTIONS:
                with request.env.registry.cursor() as cr:
                    tx = self._square_lock_attempt(cr, transaction_id)
                    if tx.state == 'pending' and not tx.provider_reference:
                        tx._set_error(_("Square declined the payment. Check your card or use a different payment method."))
                    return self._square_status(tx)
            return {'state': 'pending'}

        with request.env.registry.cursor() as cr:
            tx = self._square_lock_attempt(cr, transaction_id)
            tx._square_apply_payment(response.get('payment'))
            return self._square_status(tx)

    @staticmethod
    def _square_order_matches_website_and_user(order):
        user = request.env.user
        return (
            order.website_id.id == request.website.id and order.company_id.id == request.website.company_id.id
            and (user._is_public() or order.partner_id.commercial_partner_id == user.partner_id.commercial_partner_id)
        )

    @staticmethod
    def _square_lock_attempt(cr, transaction_id):
        tx = api.Environment(cr, request.env.uid, request.env.context)['payment.transaction'].sudo().browse(transaction_id)
        tx._square_lock_for_status()
        return tx

    @staticmethod
    def _square_create_order(environment, access_token, reference, location, money):
        try:
            response = square_request(environment, 'POST', '/v2/orders', access_token=access_token, payload={
                'idempotency_key': f'order-{reference}',
                'order': {
                    'location_id': location,
                    'reference_id': reference,
                    'line_items': [{
                        'name': 'Odoo website order',
                        'quantity': '1',
                        'base_price_money': money,
                    }],
                },
            })
        except SquareAPIError:
            return None
        order = response.get('order')
        if (
            isinstance(order, dict) and isinstance(order.get('id'), str) and order['id']
            and order.get('location_id') == location and order.get('reference_id') == reference
            and order.get('state') == 'OPEN' and order.get('total_money') == money
        ):
            return order['id']
        return None

    @staticmethod
    def _square_monitor(transaction_id, order_id):
        PaymentPostProcessing.monitor_transaction(request.env['payment.transaction'].browse(transaction_id))
        request.session['__website_sale_last_tx_id'] = transaction_id
        request.session['sale_last_order_id'] = order_id

    @staticmethod
    def _square_status(tx):
        return {'state': tx.state, 'message': tx.state_message if tx.state in ('cancel', 'error') else False}

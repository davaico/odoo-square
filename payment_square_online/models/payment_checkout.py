from odoo import _, api, fields, models
from odoo.exceptions import UserError, ValidationError

from odoo.addons.payment import utils as payment_utils

from ..square_api import SquareAPIError


# These errors reject the card operation. A reused token, an idempotency conflict, an
# unrecognized error, or a transport failure do not prove that no funds were collected.
_CARD_REJECTIONS = {
    'ADDRESS_VERIFICATION_FAILURE', 'ALLOWABLE_PIN_TRIES_EXCEEDED', 'BAD_EXPIRATION',
    'CARD_DECLINED', 'CARD_DECLINED_CALL_ISSUER', 'CARD_EXPIRED', 'CARD_NOT_SUPPORTED',
    'CARD_TOKEN_EXPIRED', 'CARDHOLDER_INSUFFICIENT_PERMISSIONS', 'CVV_FAILURE',
    'CHIP_INSERTION_REQUIRED', 'EXPIRATION_FAILURE', 'GENERIC_DECLINE', 'INSUFFICIENT_FUNDS',
    'INVALID_ACCOUNT', 'INVALID_CARD', 'INVALID_CARD_DATA', 'INVALID_EXPIRATION',
    'INVALID_PIN', 'INVALID_POSTAL_CODE', 'MANUALLY_ENTERED_PAYMENT_NOT_SUPPORTED',
    'PAN_FAILURE', 'TRANSACTION_LIMIT', 'VERIFY_AVS_FAILURE', 'VERIFY_CVV_FAILURE', 'VOICE_FAILURE',
}
_AUTHENTICATION_REJECTIONS = {'CARD_DECLINED_VERIFICATION_REQUIRED', 'VERIFY_BUYER_FAILED'}
_AMOUNT_REJECTIONS = {'AMOUNT_TOO_HIGH', 'AMOUNT_TOO_LOW', 'PAYMENT_AMOUNT_MISMATCH', 'PAYMENT_LIMIT_EXCEEDED'}
_UNAVAILABLE_REJECTIONS = {
    'INSUFFICIENT_PERMISSIONS', 'INVALID_EMAIL_ADDRESS', 'INVALID_FEES', 'INVALID_LOCATION',
    'INVALID_PHONE_NUMBER', 'PAYMENT_SOURCE_NOT_ENABLED_FOR_TARGET', 'UNSUPPORTED_CURRENCY',
}
_DEFINITIVE_REJECTIONS = (
    _CARD_REJECTIONS | _AUTHENTICATION_REJECTIONS | _AMOUNT_REJECTIONS | _UNAVAILABLE_REJECTIONS
)


class PaymentTransaction(models.Model):
    _inherit = 'payment.transaction'

    def _square_submit_checkout(self, submission_token, source_id, *, website_id, session_id, cart_id):
        self.ensure_one()
        # Native checkout has already committed the draft. Reserve the cart in a separate
        # transaction so its payment identity survives the request that sends it to Square.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, self.env.uid, self.env.context)
            tx = self.with_env(env).sudo().exists()
            if not tx or not tx.square_order_reference or not payment_utils.check_access_token(
                submission_token, tx.id, tx.square_order_reference, session_id,
            ):
                raise ValidationError(_("This payment attempt is not available. Please refresh checkout."))
            order = tx.sale_order_ids
            if len(order) != 1 or not order._square_order_matches_checkout(env['website'].browse(website_id), env.user):
                raise ValidationError(_("This payment attempt is not available. Please refresh checkout."))

            order._square_lock_checkout()
            if tx.state != 'draft':
                return tx._square_checkout_status(), order.id if tx.state in ('pending', 'done') else None
            if order.id != cart_id:
                raise ValidationError(_("This cart is no longer current. Please refresh checkout."))

            tx._square_claim_checkout(order)
            order_id = order.id

        # Commit the Square order ID and dispatch time before CreatePayment. Recovery shares the
        # transaction lock, so a reservation abandoned before this marker can be released safely.
        with self.env.registry.cursor() as cr:
            tx = self._square_lock_checkout_attempt(cr)
            if tx.state != 'pending' or tx.square_dispatched_at:
                return tx._square_checkout_status(), None
            payment_order_id = tx._square_create_order()
            if not payment_order_id:
                tx._set_error(_("Could not start the Square payment. No payment was sent. Please try again."))
                return tx._square_checkout_status(), None
            payload = {
                'idempotency_key': f'pay-{tx.square_order_reference}',
                'reference_id': tx.square_order_reference,
                'source_id': source_id,
                'location_id': tx.square_location_ref,
                'order_id': payment_order_id,
                'amount_money': tx._square_money(),
                'autocomplete': True,
                'accept_partial_authorization': False,
            }
            tx.write({'square_order_id': payment_order_id, 'square_dispatched_at': fields.Datetime.now()})

        with self.env.registry.cursor() as cr:
            tx = self._square_lock_checkout_attempt(cr)
            if tx.state == 'pending' and not tx.provider_reference:
                tx._square_create_payment(payload)
            return tx._square_checkout_status(), order_id

    def _square_lock_checkout_attempt(self, cr):
        env = api.Environment(cr, self.env.uid, self.env.context)
        tx = self.with_env(env).sudo()
        tx._square_lock_for_status()
        return tx

    def _square_create_order(self):
        reference = self.square_order_reference
        location = self.square_location_ref
        money = self._square_money()
        try:
            response = self._square_request('POST', '/v2/orders', payload={
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
        except (SquareAPIError, UserError):
            return None
        order = response.get('order')
        if (
            isinstance(order, dict) and isinstance(order.get('id'), str) and order['id']
            and order.get('location_id') == location and order.get('reference_id') == reference
            and order.get('state') == 'OPEN' and order.get('total_money') == money
        ):
            return order['id']
        return None

    def _square_create_payment(self, payload):
        try:
            response = self._square_request('POST', '/v2/payments', payload=payload)
        except UserError:
            self._set_error(_("Could not start the Square payment. No payment was sent. Please try again."))
        except SquareAPIError as error:
            if error.is_authorization_error or (
                error.status == 403 and set(error.codes) == {'CARD_PROCESSING_NOT_ENABLED'}
            ):
                self._set_error(_("Square is unavailable. No payment was collected. Choose another payment method."))
            elif error.status in (400, 402) and error.codes and set(error.codes) <= _DEFINITIVE_REJECTIONS:
                codes = set(error.codes)
                if codes & _UNAVAILABLE_REJECTIONS:
                    message = _("Square is unavailable for this payment. Choose another payment method.")
                elif codes & _AMOUNT_REJECTIONS:
                    message = _("Square could not accept the payment amount. Choose another payment method.")
                elif codes & _AUTHENTICATION_REJECTIONS:
                    message = _("Card authentication is required. Try again and complete your bank's verification.")
                else:
                    message = _("Square declined the payment. Check your card or use a different payment method.")
                self._set_error(message)
        else:
            self._square_apply_payment(response.get('payment'))

    def _square_checkout_status(self):
        return {'state': self.state, 'message': self.state_message if self.state in ('cancel', 'error') else False}

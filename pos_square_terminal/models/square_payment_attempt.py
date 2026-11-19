from copy import deepcopy
from urllib.parse import quote

from odoo import _, fields, models
from odoo.exceptions import UserError

from ..square_api import SquareAPIError
from .square_order import matches_money


class SquarePaymentAttempt(models.Model):
    _name = 'pos.square.payment.attempt'
    _description = 'Square Terminal Payment Attempt'

    square_order_link_id = fields.Many2one('pos.square.order', required=True, ondelete='restrict', index=True)
    order_id = fields.Many2one(related='square_order_link_id.order_id', store=True, index=True)
    company_id = fields.Many2one(related='square_order_link_id.company_id', store=True)
    connection_id = fields.Many2one(related='square_order_link_id.connection_id', store=True)
    environment = fields.Char(related='square_order_link_id.environment', store=True)
    merchant_id = fields.Char(related='square_order_link_id.merchant_id', store=True)
    square_location_id = fields.Char(related='square_order_link_id.square_location_id', store=True)
    currency_code = fields.Char(related='square_order_link_id.currency_code', store=True)
    payment_id = fields.Many2one('pos.payment', required=True, ondelete='restrict', index=True)
    payment_uuid = fields.Char(required=True)
    payment_method_id = fields.Many2one('pos.payment.method', required=True, ondelete='restrict')
    terminal_id = fields.Many2one('pos.square.terminal', required=True, ondelete='restrict')
    square_device_id = fields.Char(required=True)
    application_id = fields.Char(required=True)
    reference = fields.Char(required=True, index=True)
    amount_minor = fields.Integer(required=True)
    checkout_request = fields.Json(required=True)
    state = fields.Selection([
        ('prepared', 'Reserved'),
        ('ready', 'Linked order ready'),
        ('pending', 'Waiting for Square Terminal'),
        ('blocked', 'Square order discrepancy'),
        ('uncertain', 'Payment outcome uncertain'),
        ('verified', 'Payment verified'),
    ], required=True, default='prepared', index=True)
    square_checkout_id = fields.Char(index='btree_not_null', copy=False)
    square_payment_id = fields.Char(index='btree_not_null', copy=False)
    provider_status = fields.Char(copy=False)
    last_checked_at = fields.Datetime(copy=False)

    _sql_constraints = [
        ('reference_unique', 'unique(reference)', 'The Square checkout reference already exists.'),
        ('square_checkout_unique', 'unique(environment, merchant_id, square_checkout_id)',
         'This Square checkout already belongs to another Odoo payment.'),
        ('square_payment_unique', 'unique(environment, merchant_id, square_payment_id)',
         'This Square payment already belongs to another Odoo payment.'),
    ]

    def _create_square_order(self):
        self.ensure_one()
        if self.state != 'prepared':
            return
        try:
            self._check_provider_scope()
            amounts_match = self.square_order_link_id._create_square_order()
        except (SquareAPIError, UserError, ValueError):
            self.state = 'uncertain'
            return
        if not amounts_match:
            self.state = 'blocked'
            return
        request = deepcopy(self.checkout_request)
        request['checkout']['order_id'] = self.square_order_link_id.square_order_id
        self.write({'checkout_request': request, 'state': 'ready'})

    def _check_provider_scope(self):
        self.ensure_one()
        connection = self.connection_id
        if (
            connection.company_id != self.company_id or connection.environment != self.environment
            or connection.merchant_id != self.merchant_id or connection.application_id != self.application_id
        ):
            raise UserError(_("Reconnect the original Square merchant account."))

    def _dispatch(self):
        self.ensure_one()
        if self.state != 'ready':
            return
        try:
            self._check_provider_scope()
            checkout = self.connection_id._request(
                'POST', '/v2/terminals/checkouts', payload=deepcopy(self.checkout_request),
            ).get('checkout')
            self._observe_checkout(checkout)
        except (SquareAPIError, UserError, ValueError):
            self.state = 'uncertain'

    def _refresh(self):
        self.ensure_one()
        if not self.square_checkout_id or self.state == 'verified':
            return
        try:
            self._check_provider_scope()
            path = '/v2/terminals/checkouts/' + quote(self.square_checkout_id, safe='')
            checkout = self.connection_id._request('GET', path).get('checkout')
            self._observe_checkout(checkout)
        except (SquareAPIError, UserError, ValueError):
            self.state = 'uncertain'

    def _observe_checkout(self, checkout):
        self.ensure_one()
        if not isinstance(checkout, dict):
            raise ValueError('Missing Square Terminal checkout')
        checkout_id = checkout.get('id')
        order = self.square_order_link_id
        options = checkout.get('device_options')
        if not (
            isinstance(checkout_id, str) and checkout_id
            and (not self.square_checkout_id or checkout_id == self.square_checkout_id)
            and checkout.get('order_id') == order.square_order_id
            and checkout.get('reference_id') == self.reference
            and checkout.get('location_id') == self.square_location_id
            and checkout.get('app_id', self.application_id) == self.application_id
            and isinstance(options, dict) and options.get('device_id') == self.square_device_id
            and matches_money(checkout.get('amount_money'), self.amount_minor, self.currency_code)
        ):
            raise ValueError('Square Terminal checkout does not match the reserved tender')

        status = checkout.get('status')
        if status not in ('PENDING', 'IN_PROGRESS', 'CANCEL_REQUESTED', 'CANCELED', 'COMPLETED'):
            raise ValueError('Unknown Square Terminal checkout status')
        self.write({
            'square_checkout_id': checkout_id,
            'provider_status': status,
            'last_checked_at': fields.Datetime.now(),
            'state': 'pending' if status in ('PENDING', 'IN_PROGRESS') else 'uncertain',
        })
        if status != 'COMPLETED':
            return

        ids = checkout.get('payment_ids')
        if not (isinstance(ids, list) and len(ids) == 1 and isinstance(ids[0], str) and ids[0]):
            raise ValueError('Square Terminal checkout did not identify one payment')
        self._verify_payment(ids[0])

    def _verify_payment(self, payment_id):
        self.ensure_one()
        path = '/v2/payments/' + quote(payment_id, safe='')
        payment = self.connection_id._request('GET', path).get('payment')
        if not (
            isinstance(payment, dict) and payment.get('id') == payment_id
            and (not self.square_payment_id or self.square_payment_id == payment_id)
            and payment.get('order_id') == self.square_order_link_id.square_order_id
            and payment.get('reference_id') == self.reference
            and payment.get('terminal_checkout_id', self.square_checkout_id) == self.square_checkout_id
            and payment.get('location_id') == self.square_location_id
            and payment.get('source_type') == 'CARD'
            and matches_money(payment.get('amount_money'), self.amount_minor, self.currency_code)
            and matches_money(payment.get('total_money'), self.amount_minor, self.currency_code)
            and ('tip_money' not in payment or matches_money(payment['tip_money'], 0, self.currency_code))
        ):
            raise ValueError('Square payment does not match the reserved tender')

        self.square_payment_id = payment_id
        if payment.get('status') == 'COMPLETED' and (
            'refunded_money' not in payment or matches_money(payment['refunded_money'], 0, self.currency_code)
        ):
            self.state = 'verified'
            self.payment_id._square_set_verified(payment_id)
        elif payment.get('status') in ('APPROVED', 'PENDING'):
            self.state = 'pending'

    def _cashier_status(self):
        self.ensure_one()
        return {
            'state': self.state,
            'payment_id': self.square_payment_id if self.state == 'verified' else False,
        }

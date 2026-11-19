import json
from copy import deepcopy
from datetime import timedelta, timezone
from urllib.parse import quote

from odoo import _, api, fields, models
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
        ('dispatching', 'Dispatch requested'),
        ('pending', 'Waiting for Square Terminal'),
        ('declined', 'Card declined while checkout is still active'),
        ('failed', 'No payment collected'),
        ('blocked', 'Square order discrepancy'),
        ('uncertain', 'Payment outcome uncertain'),
        ('timed_out', 'Square timeout awaiting confirmation'),
        ('verified', 'Payment verified'),
    ], required=True, default='prepared', index=True)
    active_device_key = fields.Char(compute='_compute_active_device_key', store=True)
    square_checkout_id = fields.Char(index='btree_not_null', copy=False)
    square_payment_id = fields.Char(index='btree_not_null', copy=False)
    provider_status = fields.Char(copy=False)
    last_checked_at = fields.Datetime(copy=False)

    _sql_constraints = [
        ('active_device_unique', 'unique(active_device_key)', 'The Square terminal already has an active checkout.'),
        ('reference_unique', 'unique(reference)', 'The Square checkout reference already exists.'),
        ('square_checkout_unique', 'unique(environment, merchant_id, square_checkout_id)',
         'This Square checkout already belongs to another Odoo payment.'),
        ('square_payment_unique', 'unique(environment, merchant_id, square_payment_id)',
         'This Square payment already belongs to another Odoo payment.'),
    ]

    @staticmethod
    def _device_key(environment, merchant_id, device_id):
        return json.dumps((environment, merchant_id, device_id.removeprefix('device:')))

    @api.depends('state', 'environment', 'merchant_id', 'square_device_id')
    def _compute_active_device_key(self):
        for attempt in self:
            if attempt.state in ('blocked', 'failed', 'verified'):
                attempt.active_device_key = False
            else:
                attempt.active_device_key = self._device_key(
                    attempt.environment, attempt.merchant_id, attempt.square_device_id,
                )

    def _create_square_order(self):
        self.ensure_one()
        if self.state != 'prepared':
            return
        try:
            self._check_provider_scope()
            amounts_match = self.square_order_link_id._create_square_order()
        except (SquareAPIError, UserError, ValueError):
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
        if self.state != 'dispatching':
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
        if self.state not in ('dispatching', 'pending', 'declined', 'uncertain', 'timed_out'):
            return
        try:
            self._check_provider_scope()
            if self.square_checkout_id:
                path = '/v2/terminals/checkouts/' + quote(self.square_checkout_id, safe='')
                checkout = self.connection_id._request('GET', path).get('checkout')
                self._observe_checkout(checkout, authoritative=True)
            elif self.square_order_link_id.square_order_id:
                checkout = self._find_original_checkout()
                if checkout is not None:
                    self._observe_checkout(checkout, authoritative=True)
        except (SquareAPIError, UserError, ValueError):
            # A read can race the cashier's dispatch RPC. Only that RPC can
            # mark the first submission attempt as uncertain.
            if self.state != 'dispatching':
                self.state = 'uncertain'

    def _find_original_checkout(self):
        self.ensure_one()
        start = (self.create_date - timedelta(minutes=5)).replace(tzinfo=timezone.utc)
        payload = {'query': {'filter': {
            'device_id': self.square_device_id,
            'created_at': {'start_at': start.isoformat(timespec='seconds')},
        }}, 'limit': 100}
        seen_cursors = set()
        while True:
            result = self.connection_id._request('POST', '/v2/terminals/checkouts/search', payload=payload)
            checkouts = result.get('checkouts', [])
            if not isinstance(checkouts, list):
                raise ValueError('Invalid Square checkout search response')
            for checkout in checkouts:
                if isinstance(checkout, dict) and checkout.get('reference_id') == self.reference:
                    return checkout
            cursor = result.get('cursor')
            if not cursor:
                return None
            if not isinstance(cursor, str) or cursor in seen_cursors:
                raise ValueError('Invalid Square checkout search cursor')
            seen_cursors.add(cursor)
            payload = {**payload, 'cursor': cursor}

    def _observe_checkout(self, checkout, *, authoritative=False):
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
        cancel_reason = checkout.get('cancel_reason')
        state = 'uncertain'
        if status in ('PENDING', 'IN_PROGRESS'):
            state = 'pending'
        elif status == 'CANCELED' and cancel_reason == 'TIMED_OUT':
            # A timeout can occur while the device is still completing payment.
            state = 'timed_out'
        self.write({
            'square_checkout_id': checkout_id,
            'provider_status': status,
            'last_checked_at': fields.Datetime.now(),
            'state': state,
        })
        ids = checkout.get('payment_ids', [])
        if not isinstance(ids, list) or len(ids) > 1 or any(not isinstance(item, str) or not item for item in ids):
            raise ValueError('Invalid Square payment IDs')
        if status == 'COMPLETED' and not ids:
            raise ValueError('Square Terminal checkout did not identify one payment')

        safe_cancellation = authoritative and status == 'CANCELED' and cancel_reason in (
            'BUYER_CANCELED', 'SELLER_CANCELED',
        )
        final = status == 'COMPLETED' or safe_cancellation
        payment_id = ids[0] if ids else self.square_payment_id
        if payment_id:
            self._verify_payment(payment_id, final=final)
        elif safe_cancellation:
            self.state = 'failed'

    def _verify_payment(self, payment_id, *, final):
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

        status = payment.get('status')
        if status == 'COMPLETED' and (
            'refunded_money' not in payment or matches_money(payment['refunded_money'], 0, self.currency_code)
        ):
            self.write({'square_payment_id': payment_id, 'state': 'verified'})
            self.payment_id._square_set_verified(payment_id)
        elif status in ('APPROVED', 'PENDING'):
            self.write({'square_payment_id': payment_id, 'state': 'pending'})
        elif status in ('FAILED', 'CANCELED'):
            if final:
                self.write({'square_payment_id': payment_id, 'state': 'failed'})
            elif self.provider_status in ('PENDING', 'IN_PROGRESS'):
                self.state = 'declined'

    def _cashier_status(self):
        self.ensure_one()
        return {
            'state': self.state,
            'reference': self.reference,
            'payment_id': self.square_payment_id if self.state == 'verified' else False,
        }

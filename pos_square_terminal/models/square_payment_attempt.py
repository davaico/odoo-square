import json
from copy import deepcopy
from datetime import timedelta, timezone
from urllib.parse import quote

from odoo import _, api, fields, models
from odoo.exceptions import UserError

from ..square_api import SquareAPIError
from .square_order import matches_money


class SquarePaymentDiscrepancy(ValueError):
    pass


class SquarePaymentAttempt(models.Model):
    _name = 'pos.square.payment.attempt'
    _description = 'Square Terminal Payment Attempt'
    _rec_name = 'reference'

    square_order_link_id = fields.Many2one('pos.square.order', required=True, ondelete='restrict', index=True)
    order_id = fields.Many2one(related='square_order_link_id.order_id', store=True, index=True)
    company_id = fields.Many2one(related='square_order_link_id.company_id', store=True)
    connection_id = fields.Many2one(related='square_order_link_id.connection_id', store=True)
    environment = fields.Char(related='square_order_link_id.environment', store=True)
    merchant_id = fields.Char(related='square_order_link_id.merchant_id', store=True)
    square_location_id = fields.Char(related='square_order_link_id.square_location_id', store=True)
    currency_code = fields.Char(related='square_order_link_id.currency_code', store=True)
    payment_id = fields.Many2one('pos.payment', ondelete='set null', index=True)
    payment_uuid = fields.Char(required=True)
    payment_method_id = fields.Many2one('pos.payment.method', required=True, ondelete='restrict')
    terminal_id = fields.Many2one('pos.square.terminal', required=True, ondelete='restrict')
    square_device_id = fields.Char(required=True)
    application_id = fields.Char(required=True)
    reference = fields.Char(required=True, index=True)
    amount_minor = fields.Integer(required=True)
    checkout_request = fields.Json(required=True, groups='base.group_system')
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
        ('authorized', 'Authorized by Square'),
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
            if attempt.state in ('blocked', 'failed', 'authorized', 'verified'):
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
        except SquareAPIError as error:
            if (
                error.status == 400 and error.codes == ('PARTIAL_PAYMENT_DELAY_CAPTURE_NOT_SUPPORTED',)
                and self.square_order_link_id.split
            ):
                self.write({'state': 'failed', 'provider_status': error.codes[0]})
            else:
                self.state = 'uncertain'
        except (UserError, ValueError):
            self.state = 'uncertain'

    def _refresh(self, *, recheck_failed=False):
        self.ensure_one()
        if self.square_order_link_id.cancelled or (
            self.state not in self._square_reconcilable_states()
            and not (recheck_failed and self.state == 'failed' and self.square_checkout_id)
        ):
            return True
        try:
            self._check_provider_scope()
            if self.state in ('authorized', 'verified') and self.square_payment_id:
                # Terminal checkouts expire. An authenticated payment ID remains a durable reference.
                status = self._verify_payment(self.square_payment_id, final=True)
                self._record_successful_check()
                link = self.square_order_link_id
                if self.state != 'verified' and link.settlement_request and status == 'COMPLETED':
                    link._settle(self.order_id._square_current_attempts(), read_only=True)
                return status
            if self.square_checkout_id:
                path = '/v2/terminals/checkouts/' + quote(self.square_checkout_id, safe='')
                checkout = self.connection_id._request('GET', path).get('checkout')
                return self._observe_checkout(checkout, authoritative=True)
            if self.square_order_link_id.square_order_id:
                checkout = self._find_original_checkout()
                if checkout is not None:
                    return self._observe_checkout(checkout, authoritative=True)
        except SquareAPIError as error:
            issue = (
                _("Reconnect %s, the original Square connection.", self.connection_id.display_name)
                if error.is_authorization_error else _("Square could not be reached or could not return this record.")
            )
            self._record_failed_check(issue)
        except UserError:
            self._record_failed_check(
                _("Check the authorization of %s, the original Square connection.", self.connection_id.display_name),
            )
        except SquarePaymentDiscrepancy as error:
            self._record_failed_check(str(error), discrepancy=True)
        except ValueError:
            self._record_failed_check(
                _("Square returned details that could not be verified against the original payment."),
                discrepancy=True,
            )
        return False

    def _cancel_checkout(self):
        self.ensure_one()
        if self.state == 'prepared':
            # Resolve an Order create whose response may have been lost with the original key.
            self._create_square_order()
        if self.state == 'ready':
            self.state = 'failed'
            return
        payment_status = self._refresh(recheck_failed=True)
        if (
            self.state in ('pending', 'declined', 'uncertain')
            and self.square_checkout_id
            and self.provider_status in ('PENDING', 'IN_PROGRESS')
        ):
            path = '/v2/terminals/checkouts/' + quote(self.square_checkout_id, safe='') + '/cancel'
            payment_status = self._request_cancellation(path)
        if payment_status == 'APPROVED' and self.provider_status in ('COMPLETED', 'CANCELED'):
            path = '/v2/payments/' + quote(self.square_payment_id, safe='') + '/cancel'
            self._request_cancellation(path)

    def _request_cancellation(self, path):
        try:
            self._check_provider_scope()
            self.connection_id._request('POST', path)
        except (SquareAPIError, UserError):
            pass
        # The cancellation acknowledgment may race a completed card payment.
        return self._refresh()

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
        payment_status = None
        if payment_id:
            payment_status = self._verify_payment(payment_id, final=final)
        elif safe_cancellation:
            self.state = 'failed'
        self._record_successful_check()
        return payment_status or True

    def _read_payment(self, payment_id):
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
        ):
            raise ValueError('Square payment does not match the reserved tender')
        amount = payment.get('amount_money')
        if not matches_money(amount, self.amount_minor, self.currency_code):
            if isinstance(amount, dict) and type(amount.get('amount')) is int and amount.get('currency') == self.currency_code:
                raise SquarePaymentDiscrepancy(_(
                    "Square reported %(observed)s %(currency)s minor units for the payment base. Odoo requested %(requested)s.",
                    observed=amount['amount'], currency=self.currency_code, requested=self.amount_minor,
                ))
            raise SquarePaymentDiscrepancy(_("The Square payment base has an invalid amount or currency."))
        if not (
            matches_money(payment.get('total_money'), self.amount_minor, self.currency_code)
            and ('tip_money' not in payment or matches_money(payment['tip_money'], 0, self.currency_code))
        ):
            raise SquarePaymentDiscrepancy(_("The Square payment total or tip differs from the original payment."))

        status = payment.get('status')
        if status not in ('COMPLETED', 'APPROVED', 'PENDING', 'FAILED', 'CANCELED'):
            raise ValueError('Unknown Square payment status')
        return payment

    def _verify_payment(self, payment_id, *, final):
        self.ensure_one()
        payment = self._read_payment(payment_id)
        status = payment['status']
        if self.state == 'verified':
            if status != 'COMPLETED':
                raise ValueError('Square no longer reports the verified payment as completed')
            self._observe_verified_payment(payment)
        elif status == 'COMPLETED':
            if not self.square_order_link_id.split:
                self._mark_verified(payment)
            else:
                self.write({
                    'square_payment_id': payment_id,
                    'state': 'authorized' if self.square_order_link_id.settlement_request else 'uncertain',
                })
        elif status == 'APPROVED' and final and self.square_order_link_id.split:
            self.write({'square_payment_id': payment_id, 'state': 'authorized'})
        elif status in ('APPROVED', 'PENDING'):
            if self.state == 'authorized':
                raise ValueError('Square no longer reports the payment as approved')
            self.write({'square_payment_id': payment_id, 'state': 'pending'})
        elif status in ('FAILED', 'CANCELED'):
            if final:
                self.write({'square_payment_id': payment_id, 'state': 'failed'})
            elif self.provider_status in ('PENDING', 'IN_PROGRESS'):
                self.state = 'declined'
        return status

    def _mark_verified(self, payment):
        self.ensure_one()
        self.write({'square_payment_id': payment['id'], 'state': 'verified'})
        self.payment_id._square_set_verified(payment['id'])
        self._observe_verified_payment(payment)
        self._record_successful_check()

    def _cashier_state(self):
        self.ensure_one()
        if self.state != 'verified':
            if self.square_order_link_id.cancelled:
                return 'cancelled'
            if self.square_order_link_id.cancel_requested:
                return 'canceling'
            if self.square_order_link_id.settlement_request:
                return 'settling'
            if self.provider_status == 'PARTIAL_PAYMENT_DELAY_CAPTURE_NOT_SUPPORTED':
                return 'unsupported_split'
        return self.state

    def _cashier_status(self):
        self.ensure_one()
        status = self._cashier_tender_status()
        if self.square_order_link_id.split:
            status['tenders'] = {
                attempt.payment_uuid: attempt._cashier_tender_status()
                for attempt in self.order_id._square_current_attempts()
            }
            if self.square_order_link_id.cancelled:
                status['tenders'] = {
                    attempt.payment_uuid: attempt._cashier_tender_status()
                    for attempt in self.order_id._square_attempts()
                    if attempt.square_order_link_id == self.square_order_link_id
                }
        return status

    def _cashier_tender_status(self):
        self.ensure_one()
        return {
            'state': self._cashier_state(),
            'reference': self.reference,
            'payment_id': self.square_payment_id if self.state == 'verified' else False,
        }

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
    amount_minor = fields.Integer(required=True)  # Original Odoo tender, including any tip entered in Odoo.
    authorized_tip_minor = fields.Integer(copy=False)
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
                    link._settle(self.order_id._square_current_attempts(), allow_square_capture=False)
                return status
            checkout = None
            if self.square_checkout_id:
                path = '/v2/terminals/checkouts/' + quote(self.square_checkout_id, safe='')
                try:
                    checkout = self.connection_id._request('GET', path).get('checkout')
                    if checkout is None:
                        raise ValueError('Missing Square Terminal checkout')
                except SquareAPIError as error:
                    if error.status != 404:
                        raise
            elif self.square_order_link_id.square_order_id:
                checkout = self._find_original_checkout()
            if checkout is not None:
                status = self._observe_checkout(checkout, authoritative=True)
                if self.state != 'timed_out':
                    return status
            if self.square_order_link_id.square_order_id:
                return self._recover_original_payment()
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

    def _recover_original_payment(self):
        self.ensure_one()
        link = self.square_order_link_id
        payment = self._read_payment(self.square_payment_id) if self.square_payment_id else None
        if payment and payment['status'] not in ('FAILED', 'CANCELED'):
            status = self._observe_payment(payment, final=False)
            self._record_successful_check()
            return status

        path = '/v2/orders/' + quote(link.square_order_id, safe='')
        order = self.connection_id._request('GET', path).get('order')
        if not (
            isinstance(order, dict) and order.get('id') == link.square_order_id
            and order.get('reference_id') == link.reference and order.get('location_id') == self.square_location_id
            and order.get('state') in ('OPEN', 'COMPLETED', 'CANCELED')
        ):
            raise ValueError('Square order does not match the reserved checkout')
        tenders = order.get('tenders', [])
        if not isinstance(tenders, list):
            raise ValueError('Invalid Square order tenders')
        tender_ids = set()
        matching_payments = []
        for tender in tenders:
            payment_id = (tender.get('payment_id') or tender.get('id')) if isinstance(tender, dict) else None
            if not isinstance(payment_id, str) or not payment_id or payment_id in tender_ids:
                raise ValueError('Invalid Square order tender payment ID')
            tender_ids.add(payment_id)
            if payment or tender.get('type') == 'CASH':
                continue
            candidate = self._read_payment(payment_id, from_order=True)
            if candidate:
                matching_payments.append(candidate)
        if len(matching_payments) > 1:
            raise ValueError('Multiple Square payments match the reserved checkout')
        if matching_payments:
            payment = matching_payments[0]
        # A missing checkout or empty tender list can still precede a late capture.
        # A canceled order is a terminal, unpaid provider state.
        cancellation_confirmed = order['state'] == 'CANCELED' and (
            payment['status'] in ('FAILED', 'CANCELED') if payment else not tender_ids
        )
        status = self._observe_payment(payment, final=cancellation_confirmed) if payment else None
        if cancellation_confirmed:
            if not payment:
                self.state = 'failed'
            link.cancel_requested = True
        self._record_successful_check()
        return status or True

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
            and matches_money(checkout.get('amount_money'), self._requested_base_minor(), self.currency_code)
            and ('tip_money' not in checkout or not order.tip_minor
                 or matches_money(checkout['tip_money'], self._requested_tip_minor(), self.currency_code))
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

    def _requested_base_minor(self):
        return self.checkout_request['checkout']['amount_money']['amount']

    def _requested_tip_minor(self):
        return self.checkout_request['checkout'].get('tip_money', {}).get('amount', 0)

    def _read_payment(self, payment_id, *, from_order=False):
        self.ensure_one()
        path = '/v2/payments/' + quote(payment_id, safe='')
        payment = self.connection_id._request('GET', path).get('payment')
        if not (
            isinstance(payment, dict) and payment.get('id') == payment_id
            and payment.get('order_id') == self.square_order_link_id.square_order_id
            and payment.get('location_id') == self.square_location_id
        ):
            raise ValueError('Square payment does not match the reserved order')
        if from_order and payment.get('reference_id') != self.reference:
            return
        if not (
            (not self.square_payment_id or self.square_payment_id == payment_id)
            and payment.get('reference_id') == self.reference
            and (not self.square_checkout_id
                 or payment.get('terminal_checkout_id', self.square_checkout_id) == self.square_checkout_id)
            and payment.get('source_type') == 'CARD'
        ):
            raise ValueError('Square payment does not match the reserved tender')
        amount = payment.get('amount_money')
        base = self._requested_base_minor()
        if not matches_money(amount, base, self.currency_code):
            if isinstance(amount, dict) and type(amount.get('amount')) is int and amount.get('currency') == self.currency_code:
                raise SquarePaymentDiscrepancy(_(
                    "Square reported %(observed)s %(currency)s minor units for the payment base. Odoo requested %(requested)s.",
                    observed=amount['amount'], currency=self.currency_code, requested=base,
                ))
            raise SquarePaymentDiscrepancy(_("The Square payment base has an invalid amount or currency."))
        status = payment.get('status')
        if status not in ('COMPLETED', 'APPROVED', 'PENDING', 'FAILED', 'CANCELED'):
            raise ValueError('Unknown Square payment status')
        tip_money = payment.get('tip_money', {'amount': 0, 'currency': self.currency_code})
        tip = tip_money.get('amount') if isinstance(tip_money, dict) else None
        allow_tipping = self.checkout_request['checkout']['device_options']['tip_settings']['allow_tipping']
        if not (
            type(tip) is int and 0 <= tip <= 2_147_483_647 - base
            and matches_money(tip_money, tip, self.currency_code)
            and (allow_tipping or tip == self._requested_tip_minor() or (not tip and status in ('FAILED', 'CANCELED')))
            and matches_money(payment.get('total_money'), base + tip, self.currency_code)
            and (not self.has_verified_amounts or tip == self.verified_tip_minor)
            and (self.state != 'authorized' or status not in ('APPROVED', 'COMPLETED')
                 or tip == self.authorized_tip_minor)
        ):
            raise SquarePaymentDiscrepancy(_("The Square payment total or tip differs from the original payment."))
        return payment

    def _device_tip_minor(self):
        self.ensure_one()
        return self.verified_tip_minor if self.state == 'verified' and self.square_order_link_id.device_tip_product_id else 0

    def _verify_payment(self, payment_id, *, final):
        return self._observe_payment(self._read_payment(payment_id), final=final)

    def _observe_payment(self, payment, *, final):
        self.ensure_one()
        payment_id = payment['id']
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
            self.write({
                'square_payment_id': payment_id,
                'state': 'authorized',
                'authorized_tip_minor': payment.get('tip_money', {}).get('amount', 0),
            })
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
        self._observe_verified_payment(payment)
        link = self.square_order_link_id
        amount = (self.amount_minor + self._device_tip_minor()) / (10 ** link.currency_digits)
        self.payment_id._square_set_verified(payment['id'], amount)
        if not link.split:
            self.order_id._square_project_device_tip(link)
        self._record_successful_check()

    def _cashier_state(self):
        self.ensure_one()
        if self.state != 'verified':
            if self.square_order_link_id.cancelled:
                return 'cancelled'
            if self.square_order_link_id.cancel_requested:
                return 'canceling'
            if self.square_order_link_id.settlement_request:
                return 'settlement_blocked' if self.square_order_link_id.settlement_blocked else 'settling'
            if self.square_order_link_id.cash_recording:
                return 'recording_cash'
            if self.provider_status == 'PARTIAL_PAYMENT_DELAY_CAPTURE_NOT_SUPPORTED':
                return 'unsupported_split'
        return self.state

    def _cashier_status(self):
        self.ensure_one()
        status = self._cashier_tender_status()
        tip_line = self.square_order_link_id.device_tip_line_id
        if tip_line:
            status['device_tip_line'] = tip_line.read(
                tip_line._load_pos_data_fields(self.order_id.config_id.id), load=False,
            )[0]
        if self.square_order_link_id.split:
            link = self.square_order_link_id
            status['cash_tenders'] = {
                tender['native_uuid']: {
                    'reserved': not link.cancelled,
                    'payment_id': tender.get('payment_id') if self.state == 'verified' else False,
                }
                for tender in link.cash_tenders or []
            }
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
            'amount': self.payment_id.amount if self.state == 'verified' else None,
        }

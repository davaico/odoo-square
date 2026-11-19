from copy import deepcopy
from decimal import Decimal, InvalidOperation
from urllib.parse import quote
from uuid import NAMESPACE_URL, uuid4, uuid5

from odoo import api, fields, models
from odoo.exceptions import UserError

from ..square_api import SquareAPIError


def matches_money(value, amount, currency):
    return (
        isinstance(value, dict) and type(value.get('amount')) is int
        and value['amount'] == amount and value.get('currency') == currency
    )


def matches_decimal(value, expected):
    try:
        return isinstance(value, str) and Decimal(value) == Decimal(expected)
    except InvalidOperation:
        return False


class SquareOrder(models.Model):
    _name = 'pos.square.order'
    _description = 'Square Order for an Odoo Point of Sale Order'

    # Provider evidence and mutation requests are accessible only from server code.
    order_id = fields.Many2one('pos.order', required=True, ondelete='restrict', index=True)
    order_uuid = fields.Char(required=True)
    company_id = fields.Many2one('res.company', required=True, ondelete='restrict')
    connection_id = fields.Many2one('pos.square.connection', required=True, ondelete='restrict')
    environment = fields.Char(required=True)
    merchant_id = fields.Char(required=True)
    location_id = fields.Many2one('pos.square.location', required=True, ondelete='restrict')
    square_location_id = fields.Char(required=True)
    reference = fields.Char(required=True, index=True)
    currency_code = fields.Char(required=True)
    currency_digits = fields.Integer(required=True)
    total_minor = fields.Integer(required=True)
    tip_minor = fields.Integer()
    create_request = fields.Json(required=True)
    pricing_snapshot = fields.Json(copy=False)
    pricing_issue = fields.Char(copy=False)
    square_order_id = fields.Char(index='btree_not_null', copy=False)
    split = fields.Boolean(copy=False)
    settlement_request = fields.Json(copy=False)
    settlement_blocked = fields.Boolean(copy=False)
    cancel_requested = fields.Boolean(copy=False)
    cancelled = fields.Boolean(copy=False)
    active_order_key = fields.Char(compute='_compute_active_order_key', store=True)

    _sql_constraints = [
        ('order_unique', 'unique(order_id, active_order_key)', 'An Odoo POS order can have only one active Square order.'),
        ('reference_unique', 'unique(reference)', 'The Square order reference already exists.'),
        ('square_order_unique', 'unique(environment, merchant_id, square_order_id)',
         'This Square order already belongs to another Odoo sale.'),
    ]

    @api.depends('cancelled')
    def _compute_active_order_key(self):
        for link in self:
            link.active_order_key = False if link.cancelled else 'active'

    def _preview_square_order(self):
        self.ensure_one()
        payload = {'order': deepcopy(self.create_request['order'])}
        order = self.connection_id._request('POST', '/v2/orders/calculate', payload=payload).get('order')
        if not self._matches_order_totals(order):
            self.pricing_issue = 'preview_discrepancy'
            return False
        return True

    def _create_square_order(self):
        self.ensure_one()
        order = self.connection_id._request('POST', '/v2/orders', payload=deepcopy(self.create_request)).get('order')
        if not (
            isinstance(order, dict) and isinstance(order.get('id'), str) and order['id']
            and order.get('reference_id') == self.reference and order.get('location_id') == self.square_location_id
        ):
            raise ValueError('Square order ownership could not be verified')
        self.square_order_id = order['id']
        if not self._matches_order_totals(order):
            self.pricing_issue = 'created_order_discrepancy'
            return False
        return True

    def _matches_order_totals(self, order, *, state='OPEN'):
        self.ensure_one()
        expected_lines = self.pricing_snapshot['lines'] if self.pricing_snapshot else {}
        if not isinstance(order, dict) or not (
            order.get('state') == state
            and order.get('reference_id') == self.reference
            and order.get('location_id') == self.square_location_id
            and matches_money(order.get('total_money'), self.total_minor, self.currency_code)
            and self._matches_optional_money(order, 'total_tip_money', 0)
            and self._matches_optional_money(order, 'total_service_charge_money', 0)
            and self._matches_optional_money(order, 'total_tax_money', sum(
                line.get('total_tax_money', 0) for line in expected_lines.values()
            ))
            and self._matches_optional_money(order, 'total_discount_money', sum(
                line.get('total_discount_money', 0) for line in expected_lines.values()
            ))
        ):
            return False
        if not self.pricing_snapshot:
            return True
        request_order = self.create_request['order']
        request_lines = request_order['line_items']
        response_lines = order.get('line_items')
        if not isinstance(response_lines, list) or len(response_lines) != len(request_lines):
            return False
        for field in ('taxes', 'discounts'):
            actual = order.get(field, [])
            requested = request_order.get(field, [])
            if not isinstance(actual, list) or len(actual) != len(requested):
                return False
            for adjustment in requested:
                if not any(self._matches_adjustment(entry, adjustment) for entry in actual):
                    return False
        by_uid = {line['uid']: line for line in request_lines}
        for line in response_lines:
            if not isinstance(line, dict) or not isinstance(line.get('uid'), str):
                return False
            uid = line['uid']
            requested = by_uid.pop(uid, None)
            if not requested or not (
                line.get('catalog_object_id') == requested.get('catalog_object_id')
                and ('catalog_version' not in requested or line.get('catalog_version') == requested['catalog_version'])
                and ('name' not in requested or line.get('name') == requested['name'])
                and matches_decimal(line.get('quantity'), requested['quantity'])
                and self._matches_quantity_unit(line, requested.get('quantity_unit') or expected_lines[uid].get('quantity_unit'))
                and matches_money(line.get('base_price_money'), requested['base_price_money']['amount'], self.currency_code)
                and matches_money(line.get('total_money'), expected_lines[uid]['total_money'], self.currency_code)
                and all(self._matches_optional_money(line, field, expected_lines[uid].get(field, 0)) for field in (
                    'total_tax_money', 'total_discount_money', 'total_service_charge_money',
                ))
            ):
                return False
            for field, uid_field in (('applied_taxes', 'tax_uid'), ('applied_discounts', 'discount_uid')):
                actual = line.get(field, [])
                if (
                    not isinstance(actual, list) or any(not isinstance(entry, dict) for entry in actual)
                    or sorted(str(entry.get(uid_field)) for entry in actual)
                    != sorted(entry[uid_field] for entry in requested.get(field, []))
                ):
                    return False
        return not by_uid

    def _matches_optional_money(self, record, field, amount):
        return (not amount and field not in record) or matches_money(record.get(field), amount, self.currency_code)

    def _matches_adjustment(self, entry, requested):
        return isinstance(entry, dict) and all(
            matches_decimal(entry.get(field), value) if field == 'percentage' else entry.get(field) == value
            for field, value in requested.items()
        )

    def _matches_quantity_unit(self, line, requested):
        if not requested:
            return True
        unit = line.get('quantity_unit')
        if not isinstance(unit, dict) or type(unit.get('precision')) is not int:
            return False
        if 'catalog_object_id' in requested:
            return unit.get('catalog_object_id') == requested['catalog_object_id'] and unit['precision'] == requested['precision']
        measurement = unit.get('measurement_unit')
        return (
            unit['precision'] == requested['precision'] and isinstance(measurement, dict)
            and measurement.get('custom_unit') == requested['measurement_unit']['custom_unit']
        )

    def _prepare_settlement(self, attempts):
        self.ensure_one()
        path = '/v2/orders/' + quote(self.square_order_id, safe='')
        try:
            for attempt in attempts:
                attempt._check_provider_scope()
            order = self._read_settlement_order(path)
            version = order.get('version')
            payment_ids = attempts.mapped('square_payment_id')
            if (
                order['state'] == 'OPEN' and type(version) is int and version > 0
                and all(payment_ids) and len(set(payment_ids)) == len(attempts)
                and self._matches_settlement_tenders(order, attempts, complete=False)
            ):
                # The cashier calls settlement in a separate RPC after this intent commits.
                self.settlement_request = {
                    'idempotency_key': str(uuid4()),
                    'order_version': version,
                    'payment_ids': payment_ids,
                }
        except (SquareAPIError, UserError, ValueError):
            return

    def _settle(self, attempts, *, read_only=False):
        self.ensure_one()
        request = self.settlement_request
        if not request or request['payment_ids'] != attempts.mapped('square_payment_id'):
            return
        path = '/v2/orders/' + quote(self.square_order_id, safe='')
        try:
            self.order_id._square_check_reserved_amounts(attempts)
            order, payments = self._read_settlement(path, attempts)
            if (
                not read_only and not self.cancel_requested
                and order['state'] == 'OPEN' and order.get('version') == request['order_version']
                and self._matches_settlement_tenders(order, attempts, complete=False)
                and all(payment['status'] == 'APPROVED' for payment in payments)
            ):
                try:
                    self.connection_id._request('POST', path + '/pay', payload=deepcopy(request))
                except (SquareAPIError, UserError):
                    # A missed response can still mean Square captured every tender.
                    pass
                order, payments = self._read_settlement(path, attempts)
        except (SquareAPIError, UserError, ValueError):
            return
        statuses = {payment['status'] for payment in payments}
        if (
            order['state'] == 'COMPLETED' and statuses == {'COMPLETED'}
            and self._matches_settlement_tenders(order, attempts, complete=True)
        ):
            with self.env.cr.savepoint():
                for attempt, payment in zip(attempts, payments):
                    if attempt.state != 'verified':
                        attempt._mark_verified(payment)
        released = {'CANCELED', 'FAILED'}
        self.settlement_blocked = (
            order['state'] in ('OPEN', 'CANCELED')
            and bool(statuses & released) and statuses <= released | {'APPROVED'}
            and self._matches_settlement_tenders(order, attempts, complete=False)
        )
        return self.settlement_blocked

    def _read_settlement(self, path, attempts):
        for attempt in attempts:
            attempt._check_provider_scope()
        order = self._read_settlement_order(path)
        payments = [attempt._read_payment(attempt.square_payment_id) for attempt in attempts]
        return order, payments

    def _read_settlement_order(self, path):
        order = self.connection_id._request('GET', path).get('order')
        if (
            not isinstance(order, dict) or order.get('id') != self.square_order_id
            or order.get('state') not in ('OPEN', 'COMPLETED', 'CANCELED')
            or not self._matches_order_totals(order, state=order['state'])
        ):
            raise ValueError('Square order does not match the original split sale')
        return order

    def _matches_settlement_tenders(self, order, attempts, *, complete):
        tenders = order.get('tenders', [])
        if not isinstance(tenders, list):
            return False
        expected = {attempt.square_payment_id: attempt.amount_minor for attempt in attempts}
        for tender in tenders:
            if not isinstance(tender, dict):
                return False
            payment_id = tender.get('payment_id') or tender.get('id')
            if not isinstance(payment_id, str) or payment_id not in expected:
                return False
            if (
                tender.get('type') != 'CARD'
                or tender.get('location_id', self.square_location_id) != self.square_location_id
                or not matches_money(tender.get('amount_money'), expected.pop(payment_id), self.currency_code)
            ):
                return False
        return not complete or not expected

    def _cancel_unpaid_order(self, released_payment_ids, *, request):
        self.ensure_one()
        if not self.square_order_id:
            self.cancelled = True
            return

        path = '/v2/orders/' + quote(self.square_order_id, safe='')
        try:
            order = self._read_unpaid_order(path, released_payment_ids)
            if request and order and order['state'] == 'OPEN':
                version = order.get('version')
                if type(version) is not int or version < 1:
                    return
                try:
                    self.connection_id._request('PUT', path, payload={
                        'idempotency_key': str(uuid5(NAMESPACE_URL, f'{self.reference}/cancel/{version}')),
                        'order': {'location_id': self.square_location_id, 'version': version, 'state': 'CANCELED'},
                    })
                except (SquareAPIError, UserError):
                    pass
                order = self._read_unpaid_order(path, released_payment_ids)
            if order and order['state'] == 'CANCELED':
                self.cancelled = True
        except (SquareAPIError, UserError):
            # An ambiguous update remains reserved until a later authoritative read.
            return

    def _read_unpaid_order(self, path, released_payment_ids):
        order = self.connection_id._request('GET', path).get('order')
        if not (
            isinstance(order, dict) and order.get('id') == self.square_order_id
            and order.get('location_id') == self.square_location_id and order.get('reference_id') == self.reference
            and order.get('state') in ('OPEN', 'CANCELED')
        ):
            return
        tenders = order.get('tenders', [])
        if isinstance(tenders, list) and all(
            isinstance(tender, dict) and (tender.get('payment_id') or tender.get('id')) in released_payment_ids
            for tender in tenders
        ):
            return order

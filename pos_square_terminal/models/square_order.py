from copy import deepcopy

from odoo import fields, models


def matches_money(value, amount, currency):
    return (
        isinstance(value, dict) and type(value.get('amount')) is int
        and value['amount'] == amount and value.get('currency') == currency
    )


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
    create_request = fields.Json(required=True)
    square_order_id = fields.Char(index='btree_not_null', copy=False)

    _sql_constraints = [
        ('order_unique', 'unique(order_id)', 'An Odoo POS order can have only one Square order.'),
        ('reference_unique', 'unique(reference)', 'The Square order reference already exists.'),
        ('square_order_unique', 'unique(environment, merchant_id, square_order_id)',
         'This Square order already belongs to another Odoo sale.'),
    ]

    def _create_square_order(self):
        self.ensure_one()
        order = self.connection_id._request('POST', '/v2/orders', payload=deepcopy(self.create_request)).get('order')
        if not (
            isinstance(order, dict) and isinstance(order.get('id'), str) and order['id']
            and order.get('reference_id') == self.reference and order.get('location_id') == self.square_location_id
        ):
            raise ValueError('Square order ownership could not be verified')
        self.square_order_id = order['id']
        return self._matches_order_totals(order)

    def _matches_order_totals(self, order):
        self.ensure_one()
        return (
            order.get('state') == 'OPEN'
            and matches_money(order.get('total_money'), self.total_minor, self.currency_code)
            and all(
                field not in order or matches_money(order[field], 0, self.currency_code)
                for field in ('total_tax_money', 'total_discount_money', 'total_tip_money', 'total_service_charge_money')
            )
        )

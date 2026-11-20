from odoo import SUPERUSER_ID, api, models
from odoo.osv import expression


class SquareImportOwnership(models.Model):
    _inherit = 'pos.square.order'

    @api.model
    def _square_terminal_import_scope_exists(self, company_id, environment):
        return self._square_terminal_find_import_resource([
            ('company_id', '=', company_id), ('environment', '=', environment),
        ])

    @api.model
    def _square_terminal_matches_import_resource(self, company_id, environment, kind, resource, merchant_id=None):
        resource_id = resource.get('id')
        location_id = resource.get('location_id')
        if not resource_id or not location_id:
            return False

        scope = [
            ('company_id', '=', company_id), ('environment', '=', environment),
            ('square_location_id', '=', location_id),
        ]
        if merchant_id:
            scope.append(('merchant_id', '=', merchant_id))
        # A share order can reach the importer before its create response
        # reaches Odoo. The attempt reference was committed before dispatch.
        share_id = resource.get('order_id') if kind == 'payment' else resource_id
        share_keys = [[('share_order_id', '=', share_id)]] if share_id else []
        if resource.get('reference_id'):
            share_keys.append([
                ('reference', '=', resource['reference_id']), ('share_order_id', '=', False),
                ('square_order_link_id.immediate_split', '=', True),
            ])
        if share_keys and self._square_terminal_find_import_resource(
            expression.AND([scope, expression.OR(share_keys)]), model='pos.square.payment.attempt',
        ):
            return True
        if kind == 'payment':
            if self._square_terminal_find_import_resource(
                scope + [('square_payment_id', '=', resource_id)], model='pos.square.payment.attempt',
            ):
                return True
            # Square can report a payment or refund before the POS learns its
            # payment ID. The authenticated payment still identifies its order.
            order_id = resource.get('order_id')
            return bool(order_id and self._square_terminal_find_import_resource(
                scope + [('square_order_id', '=', order_id)],
            ))
        references = [[('square_order_id', '=', resource_id)]]
        if resource.get('reference_id'):
            references.append([
                ('reference', '=', resource['reference_id']), ('square_order_id', '=', False),
            ])
        return self._square_terminal_find_import_resource(expression.AND([scope, expression.OR(references)]))

    def _square_terminal_find_import_resource(self, domain, *, model='pos.square.order'):
        # The POS commits its reservation before CreateOrder. The importer may
        # have begun its transaction before that commit and retrieved the new order later.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            return bool(env[model].search_count(domain, limit=1))

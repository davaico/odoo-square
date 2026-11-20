# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import SUPERUSER_ID, api, models
from odoo.fields import Domain


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
        return self._square_terminal_find_import_resource(Domain(scope) & Domain.OR(references))

    def _square_terminal_find_import_resource(self, domain, *, model='pos.square.order'):
        # The POS commits its reservation before CreateOrder. The importer may
        # have begun its transaction before that commit and retrieved the new order later.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            return bool(env[model].search_count(domain, limit=1))

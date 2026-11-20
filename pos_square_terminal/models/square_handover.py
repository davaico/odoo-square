from datetime import timedelta
from urllib.parse import quote
from uuid import NAMESPACE_URL, uuid5

from psycopg2.errors import DeadlockDetected, SerializationFailure

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import UserError

from ..square_api import SquareAPIError


ATTRIBUTE_KEY = 'odoo-pos-in-store-handover'
STRING_SCHEMA = 'https://developer-production-s.squarecdn.com/schemas/v1/common.json#squareup.common.String'
DEFINITION_PATH = '/v2/orders/custom-attribute-definitions'


class SquareOrder(models.Model):
    _inherit = 'pos.square.order'

    handover_at = fields.Datetime(copy=False)
    handover_state = fields.Selection([
        ('pending', 'Pending Square handover'),
        ('sent', 'Recorded in Square'),
    ], copy=False)
    handover_error = fields.Char(copy=False)
    next_handover_at = fields.Datetime(index='btree_not_null', copy=False)

    def _square_queue_handover(self):
        now = fields.Datetime.now()
        self.filtered(lambda link: link.square_order_id and not link.handover_at).write({
            'handover_at': now, 'handover_state': 'pending', 'next_handover_at': now,
        })

    @api.model
    def _cron_send_handover(self):
        due = [('handover_state', '=', 'pending'), ('next_handover_at', '<=', fields.Datetime.now())]
        for link_id in self.search(due, order='next_handover_at, id', limit=20).ids:
            try:
                with self.env.registry.cursor() as cr:
                    cr.execute(
                        'SELECT id FROM pos_square_order WHERE id = %s FOR NO KEY UPDATE SKIP LOCKED',
                        [link_id],
                    )
                    if not cr.fetchone():
                        continue
                    link = api.Environment(cr, SUPERUSER_ID, {})[self._name].browse(link_id)
                    if link.filtered_domain(due):
                        link._send_square_handover()
            except (SerializationFailure, DeadlockDetected):
                continue

    def _send_square_handover(self):
        self.ensure_one()
        try:
            self._record_square_handover()
        except SquareAPIError:
            error = _("Square could not record the counter handover. Odoo will retry.")
        except UserError as issue:
            error = str(issue)
        else:
            self.write({'handover_state': 'sent', 'handover_error': False, 'next_handover_at': False})
            return

        self.write({
            'handover_error': error,
            'next_handover_at': fields.Datetime.now() + timedelta(minutes=5),
        })

    def _record_square_handover(self):
        self.ensure_one()
        attempts = self.env['pos.square.payment.attempt'].search([
            ('square_order_link_id', '=', self.id), ('state', '=', 'verified'),
        ])
        if not attempts:
            raise UserError(_("The original Square payment is not verified."))
        for attempt in attempts:
            attempt._check_provider_scope()
            if self.immediate_split:
                self._record_order_handover(attempt.share_order_id, attempt.reference)
        if not self.immediate_split:
            self._record_order_handover(self.square_order_id, self.reference)

    def _record_order_handover(self, order_id, reference):
        path = '/v2/orders/' + quote(order_id, safe='')
        order = self.connection_id._request('GET', path).get('order')
        if not isinstance(order, dict) or (
            order.get('id') != order_id or order.get('location_id') != self.square_location_id
            or order.get('reference_id') != reference
        ):
            raise UserError(_("Square returned a different order than the original counter sale."))
        if order.get('state') != 'COMPLETED' or order.get('fulfillments'):
            raise UserError(_("Square has not confirmed completion of the original counter order."))

        path += '/custom-attributes/' + ATTRIBUTE_KEY
        value = 'In-store handover at ' + self.handover_at.isoformat(timespec='seconds') + 'Z'
        self._ensure_handover_definition()
        attribute = self._read_optional_handover_resource(path, 'custom_attribute')
        if attribute is None:
            attribute = self.connection_id._request('POST', path, payload={
                'idempotency_key': str(uuid5(NAMESPACE_URL, f'odoo-square:handover:{reference}')),
                'custom_attribute': {'value': value},
            }).get('custom_attribute')
        if not isinstance(attribute, dict) or attribute.get('key') != ATTRIBUTE_KEY or attribute.get('value') != value:
            raise UserError(_("Square has not confirmed the handover value for the original counter order."))

    def _read_optional_handover_resource(self, path, field):
        try:
            result = self.connection_id._request('GET', path)
        except SquareAPIError as error:
            if error.status == 404 and 'NOT_FOUND' in error.codes:
                return None
            raise
        resource = result.get(field)
        if not isinstance(resource, dict):
            raise UserError(_("Square returned an invalid custom attribute response."))
        return resource

    def _ensure_handover_definition(self):
        definition = self._read_optional_handover_resource(
            DEFINITION_PATH + '/' + ATTRIBUTE_KEY, 'custom_attribute_definition',
        )
        if definition is None:
            identity = f'{self.environment}:{self.merchant_id}:{self.connection_id.application_id}:{ATTRIBUTE_KEY}'
            definition = self.connection_id._request('POST', DEFINITION_PATH, payload={
                'idempotency_key': str(uuid5(NAMESPACE_URL, f'odoo-square:handover-definition:{identity}')),
                'custom_attribute_definition': {
                    'key': ATTRIBUTE_KEY,
                    'name': 'Odoo POS in-store handover',
                    'description': 'When the goods were handed over at the Odoo Point of Sale counter.',
                    'visibility': 'VISIBILITY_READ_ONLY',
                    'schema': {'$ref': STRING_SCHEMA},
                },
            }).get('custom_attribute_definition')
        if not isinstance(definition, dict) or (
            definition.get('key') != ATTRIBUTE_KEY or definition.get('schema') != {'$ref': STRING_SCHEMA}
            or definition.get('visibility') != 'VISIBILITY_READ_ONLY'
        ):
            raise UserError(_("The Square handover attribute definition differs from the Odoo definition."))


class SquarePaymentAttempt(models.Model):
    _inherit = 'pos.square.payment.attempt'

    handover_at = fields.Datetime(related='square_order_link_id.handover_at')
    handover_state = fields.Selection(related='square_order_link_id.handover_state')
    handover_error = fields.Char(related='square_order_link_id.handover_error')

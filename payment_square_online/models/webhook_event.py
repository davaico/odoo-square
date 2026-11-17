import logging
from datetime import timedelta

from psycopg2.errors import DeadlockDetected, SerializationFailure, UniqueViolation

from odoo import SUPERUSER_ID, api, fields, models
from odoo.osv import expression


_logger = logging.getLogger(__name__)


class SquareWebhookEvent(models.Model):
    _name = 'payment.square.webhook.event'
    _description = 'Verified Square Online Webhook'

    # Private inbox. Store identifiers only, never the signature or payment object.
    connection_id = fields.Many2one('payment.square.connection', required=True, ondelete='cascade')
    event_id = fields.Char(required=True)
    event_type = fields.Char(required=True)
    merchant_id = fields.Char(required=True)
    environment = fields.Selection([('sandbox', 'Sandbox'), ('production', 'Production')], required=True)
    resource_id = fields.Char(required=True)
    resource_reference = fields.Char()
    next_check_at = fields.Datetime(required=True, default=fields.Datetime.now, index=True)
    processed_at = fields.Datetime()

    _sql_constraints = [
        ('square_event_unique', 'UNIQUE(connection_id, event_id)', 'A Square event may only be queued once.'),
    ]

    @api.model
    def _square_webhook_event_types(self):
        return {'payment.created': 'payment', 'payment.updated': 'payment'}

    @api.model
    def _square_receive_event(self, connection, payload):
        event_type = payload.get('type')
        if not isinstance(event_type, str):
            return 400
        object_type = self._square_webhook_event_types().get(event_type)
        if not object_type:
            return 204
        event_id = payload.get('event_id')
        data = payload.get('data')
        if (
            not self._square_valid_event_identifier(event_id)
            or not isinstance(data, dict) or data.get('type') != object_type
        ):
            return 400
        resource_id = data.get('id')
        event_object = data.get('object')
        resource = event_object.get(object_type) if isinstance(event_object, dict) else None
        if (
            not self._square_valid_event_identifier(resource_id)
            or not isinstance(resource, dict) or resource.get('id') != resource_id
        ):
            return 400
        resource_reference = resource.get('reference_id')
        if self.search_count([('connection_id', '=', connection.id), ('event_id', '=', event_id)], limit=1):
            return 204
        try:
            with self.env.cr.savepoint():
                self.create({
                    'connection_id': connection.id,
                    'event_id': event_id,
                    'event_type': event_type,
                    'merchant_id': connection.merchant_id,
                    'environment': connection.provider_id.square_environment,
                    'resource_id': resource_id,
                    'resource_reference': self._square_valid_event_identifier(resource_reference),
                })
        except UniqueViolation as error:
            if error.diag.constraint_name != 'payment_square_webhook_event_square_event_unique':
                raise
            return 204
        self.env.ref('payment_square_online.ir_cron_square_online_webhooks')._trigger()
        return 204

    @staticmethod
    def _square_valid_event_identifier(value):
        return value if isinstance(value, str) and 0 < len(value) <= 191 and value.isascii() and value.isprintable() else False

    def _square_process_event(self):
        self.ensure_one()
        connection = self.connection_id
        if (
            connection.merchant_id != self.merchant_id or connection.provider_id.code != 'square_online'
            or connection.provider_id.square_environment != self.environment
        ):
            return True
        return self._square_process_webhook_resource()

    def _square_process_webhook_resource(self):
        scope = [
            ('provider_code', '=', 'square_online'), ('operation', '=', 'online_direct'),
            ('square_connection_id', '=', self.connection_id.id), ('square_merchant_id', '=', self.merchant_id),
            ('square_environment', '=', self.environment), ('square_dispatched_at', '!=', False),
        ]
        identity = [('provider_reference', '=', self.resource_id)]
        if self.resource_reference:
            identity = expression.OR([identity, [('square_order_reference', '=', self.resource_reference)]])
        tx = self.env['payment.transaction'].search(expression.AND([scope, identity]), limit=2)
        if len(tx) == 1:
            return tx._square_refresh_status(payment_id=self.resource_id)
        # Square sends all of the merchant's events to the application's subscription. Late
        # visibility is retryable for our own references. Other payments belong to other flows.
        return not (self.resource_reference or '').startswith('odoo-web-')

    @api.model
    def _cron_square_webhooks(self):
        now = fields.Datetime.now()
        self.search([('create_date', '<', now - timedelta(days=7))], limit=500).unlink()
        events = self.search([('processed_at', '=', False), ('next_check_at', '<=', now)],
                             order='next_check_at, id', limit=5)
        for event_id in events.ids:
            try:
                with self.env.registry.cursor() as cr:
                    event = self.with_env(api.Environment(cr, SUPERUSER_ID, {})).browse(event_id)
                    cr.execute('''
                        SELECT id FROM payment_square_webhook_event
                        WHERE id = %s AND processed_at IS NULL FOR NO KEY UPDATE SKIP LOCKED
                    ''', [event_id])
                    if not cr.fetchone():
                        continue
                    if event._square_process_event():
                        event.processed_at = fields.Datetime.now()
                    else:
                        event.next_check_at = fields.Datetime.now() + timedelta(minutes=5)
            except (SerializationFailure, DeadlockDetected):
                continue
            except Exception:
                _logger.exception('Could not process verified Square webhook %s', event_id)

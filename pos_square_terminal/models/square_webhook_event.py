import logging
from datetime import timedelta
from urllib.parse import quote

from psycopg2.errors import DeadlockDetected, SerializationFailure, UniqueViolation

from odoo import SUPERUSER_ID, api, fields, models
from odoo.osv import expression


_logger = logging.getLogger(__name__)

_EVENT_OBJECTS = {
    'terminal.checkout.created': 'checkout',
    'terminal.checkout.updated': 'checkout',
    'payment.created': 'payment',
    'payment.updated': 'payment',
    'refund.created': 'refund',
    'refund.updated': 'refund',
}


class SquareWebhookEvent(models.Model):
    _name = 'pos.square.webhook.event'
    _description = 'Verified Square Terminal Notification'

    connection_id = fields.Many2one('pos.square.connection', required=True, ondelete='cascade')
    merchant_id = fields.Char(required=True)
    application_id = fields.Char(required=True)
    environment = fields.Char(required=True)
    event_id = fields.Char(required=True)
    event_type = fields.Char(required=True)
    resource_id = fields.Char(required=True)
    resource_reference = fields.Char()
    next_check_at = fields.Datetime(required=True, default=fields.Datetime.now, index=True)
    processed_at = fields.Datetime()

    _unique_event = models.Constraint(
        'UNIQUE(connection_id, event_id)', 'This Square notification was already received.',
    )

    @staticmethod
    def _identifier(value):
        return value if isinstance(value, str) and 0 < len(value) <= 255 and value.isascii() and value.isprintable() else False

    @api.model
    def _receive_event(self, connection, payload):
        event_type = payload.get('type')
        if not isinstance(event_type, str):
            return 400
        object_type = _EVENT_OBJECTS.get(event_type)
        if not object_type:
            return 204
        event_id = self._identifier(payload.get('event_id'))
        data = payload.get('data')
        if not event_id or not isinstance(data, dict) or data.get('type') != object_type:
            return 400
        resource_id = self._identifier(data.get('id'))
        objects = data.get('object')
        resource = objects.get(object_type) if isinstance(objects, dict) else None
        if not resource_id or not isinstance(resource, dict) or resource.get('id') != resource_id:
            return 400

        reference_field = 'payment_id' if object_type == 'refund' else 'reference_id'
        if self.search_count([('connection_id', '=', connection.id), ('event_id', '=', event_id)], limit=1):
            return 204
        try:
            with self.env.cr.savepoint():
                self.create({
                    'connection_id': connection.id,
                    'merchant_id': connection.merchant_id,
                    'application_id': connection.application_id,
                    'environment': connection.environment,
                    'event_id': event_id,
                    'event_type': event_type,
                    'resource_id': resource_id,
                    'resource_reference': self._identifier(resource.get(reference_field)),
                })
        except UniqueViolation as error:
            if error.diag.constraint_name != 'pos_square_webhook_event_unique_event':
                raise
            return 204
        self.env.ref('pos_square_terminal.ir_cron_square_terminal_webhooks')._trigger()
        return 204

    def _attempt_scope(self):
        return [
            ('connection_id', '=', self.connection_id.id), ('environment', '=', self.environment),
            ('merchant_id', '=', self.merchant_id), ('application_id', '=', self.application_id),
        ]

    def _process_event(self):
        self.ensure_one()
        connection = self.connection_id
        if (
            connection.merchant_id != self.merchant_id or connection.environment != self.environment
            or connection.application_id != self.application_id
        ):
            return True
        if self.event_type.startswith('refund.'):
            return self._process_refund_event()
        field = 'square_checkout_id' if self.event_type.startswith('terminal.checkout.') else 'square_payment_id'
        identity = [(field, '=', self.resource_id)]
        if self.resource_reference:
            identity = expression.OR([identity, [('reference', '=', self.resource_reference)]])
        attempt = self.env['pos.square.payment.attempt'].search(
            expression.AND([self._attempt_scope(), identity]), limit=2,
        )
        if len(attempt) != 1 or attempt.state not in attempt._square_reconcilable_states():
            # Genuine Square sales and unrelated application events need no Terminal work.
            return True
        attempt._lock_reconciliation()
        return attempt._refresh()

    def _process_refund_event(self):
        refund = self.env['pos.square.payment.refund']._read_square_refund(self.connection_id, self.resource_id)
        payment_id = self._identifier(refund.get('payment_id'))
        if not payment_id or refund.get('unlinked') or refund['status'] in ('FAILED', 'REJECTED'):
            return True

        attempts = self.env['pos.square.payment.attempt']
        scope = self._attempt_scope()
        attempt = attempts.search(scope + [('square_payment_id', '=', payment_id)], limit=1)
        if not attempt:
            # The original payment ID may not have been observed yet. Use the
            # authenticated payment to locate the attempt, then verify its checkout.
            payment = self.connection_id._request('GET', '/v2/payments/' + quote(payment_id, safe='')).get('payment')
            if not isinstance(payment, dict) or payment.get('id') != payment_id:
                return False
            reference = self._identifier(payment.get('reference_id'))
            order_id = self._identifier(payment.get('order_id'))
            location_id = self._identifier(payment.get('location_id'))
            if not (reference and order_id and location_id):
                return True
            attempt = attempts.search(scope + [
                ('reference', '=', reference), ('square_order_id', '=', order_id),
                ('square_location_id', '=', location_id),
            ], limit=1)
        if not attempt or attempt.state not in attempt._square_reconcilable_states():
            return True
        attempt._lock_reconciliation()
        if attempt.state != 'verified':
            attempt._refresh()
            if attempt.state != 'verified':
                return False
        if attempt.square_payment_id != payment_id:
            return True
        try:
            recorded = attempt._record_square_refund(refund)
        except ValueError:
            attempt.refund_reconciliation_issue = attempt._square_refund_verification_issue()
            return False
        issue = attempt._square_refund_total_issue()
        if issue:
            attempt.refund_reconciliation_issue = issue
        return recorded

    @api.model
    def _cron_process_webhooks(self):
        now = fields.Datetime.now()
        self.search([('processed_at', '<', now - timedelta(days=7))], limit=500).unlink()
        due = [('processed_at', '=', False), ('next_check_at', '<=', now)]
        for event_id in self.search(due, order='next_check_at, id', limit=20).ids:
            try:
                with self.env.registry.cursor() as cr:
                    event = api.Environment(cr, SUPERUSER_ID, {})[self._name].browse(event_id)
                    cr.execute(
                        'SELECT id FROM pos_square_webhook_event WHERE id = %s FOR NO KEY UPDATE SKIP LOCKED',
                        [event_id],
                    )
                    if not cr.fetchone() or not event.filtered_domain(due):
                        continue
                    event.next_check_at = fields.Datetime.now() + timedelta(minutes=5)
                    try:
                        with cr.savepoint():
                            if event._process_event():
                                event.processed_at = fields.Datetime.now()
                    except (SerializationFailure, DeadlockDetected):
                        raise
                    except Exception:
                        _logger.exception('Could not process verified Square Terminal notification %s', event_id)
            except (SerializationFailure, DeadlockDetected):
                continue

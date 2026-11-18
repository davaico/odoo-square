import logging
from collections import defaultdict
from datetime import timedelta

from psycopg2.errors import DeadlockDetected, SerializationFailure

from odoo import SUPERUSER_ID, api, fields, models
from odoo.exceptions import UserError

from ..square_api import SquareAPIError


_logger = logging.getLogger(__name__)


class SquareConnection(models.Model):
    _inherit = 'payment.square.connection'

    refund_scan_at = fields.Datetime()
    refund_scan_next_check = fields.Datetime(index=True)

    def _square_refund_source_domain(self):
        self.ensure_one()
        return [
            ('provider_id', '=', self.provider_id.id), ('provider_code', '=', 'square_online'),
            ('square_connection_id', '=', self.id), ('square_environment', '=', self.provider_id.square_environment),
            ('square_application_id', '=', self.provider_id.square_application_id),
            ('square_merchant_id', '=', self.merchant_id), ('operation', '=', 'online_direct'),
            ('state', '=', 'done'), ('provider_reference', '!=', False),
            ('square_dispatched_at', '!=', False),
        ]

    def _square_read_refund_scan(self, source, now):
        # Square's list can lag. Keep a time overlap and advance only after complete pagination.
        # Filtering by payment happens locally so the provider's previous locations remain covered.
        begin = max(self.refund_scan_at or source.square_dispatched_at, now - timedelta(days=365))
        params = {'begin_time': (begin - timedelta(minutes=15)).isoformat() + 'Z', 'end_time': now.isoformat() + 'Z'}
        refunds_by_payment = defaultdict(list)
        cursors = set()
        while True:
            response = source._square_request('GET', '/v2/refunds', params=params)
            refunds = response.get('refunds', [])
            if not isinstance(refunds, list):
                return None
            for refund in refunds:
                payment_id = refund.get('payment_id') if isinstance(refund, dict) else None
                if isinstance(payment_id, str) and payment_id:
                    refunds_by_payment[payment_id].append(refund)
            cursor = response.get('cursor')
            if cursor is None or cursor == '':
                return refunds_by_payment
            if not isinstance(cursor, str) or cursor in cursors:
                return None
            cursors.add(cursor)
            params = {**params, 'cursor': cursor}

    @api.model
    def _cron_square_discover_refunds(self):
        now = fields.Datetime.now()
        domain = [
            ('provider_id.code', '=', 'square_online'), ('merchant_id', '!=', False),
            ('access_token', '!=', False), ('reauthorization_required', '=', False),
            '|', ('refund_scan_next_check', '=', False), ('refund_scan_next_check', '<=', now),
        ]
        for connection_id in self.search(domain, order='refund_scan_next_check ASC NULLS FIRST, id', limit=5).ids:
            try:
                with self.env.registry.cursor() as cr:
                    connection = self.with_env(api.Environment(cr, SUPERUSER_ID, {})).browse(connection_id)
                    if not connection._lock() or not connection.filtered_domain(domain):
                        continue
                    connection.refund_scan_next_check = now + timedelta(minutes=5)
                    scope = connection._square_refund_source_domain()
                    source = connection.env['payment.transaction'].search(scope, order='square_dispatched_at, id', limit=1)
                    if not source:
                        continue
                    try:
                        refunds = connection._square_read_refund_scan(source, now)
                    except (UserError, SquareAPIError):
                        continue

                if refunds is None:
                    continue
                # The Square reads are complete. Release the OAuth connection lock before taking
                # source locks so a scan and a status refresh can run independently.
                with self.env.registry.cursor() as cr:
                    env = api.Environment(cr, SUPERUSER_ID, {})
                    for source in env['payment.transaction'].search([
                        *scope, ('provider_reference', 'in', list(refunds)),
                    ], order='id'):
                        source._square_lock_for_status()
                        if not source.payment_id:
                            # Checkout may be captured before native post-processing creates its
                            # payment. Retain discovery before advancing the merchant-wide cursor.
                            source.square_linked_refunds_pending = True
                            continue
                        for refund in refunds[source.provider_reference]:
                            source._square_ingest_refund(refund)
                    env[self._name].browse(connection_id).refund_scan_at = now
            except (SerializationFailure, DeadlockDetected):
                continue
            except Exception:
                _logger.exception('Could not discover Square refunds for connection %s', connection_id)

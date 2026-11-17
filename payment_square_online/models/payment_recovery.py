import logging
from datetime import timedelta
from urllib.parse import quote

from psycopg2 import OperationalError
from psycopg2.errors import DeadlockDetected, SerializationFailure

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import AccessError, UserError

from ..square_api import SquareAPIError, square_request


_logger = logging.getLogger(__name__)
_DISPATCH_RESERVATION_TIMEOUT = timedelta(minutes=5)


class PaymentTransaction(models.Model):
    _inherit = 'payment.transaction'

    square_recovery_message = fields.Char(string='Square Status Check', readonly=True, copy=False)
    square_recovery_next_check = fields.Datetime(readonly=True, copy=False, index=True)
    square_paid_cancelled_order = fields.Boolean(compute='_compute_square_paid_cancelled_order')

    @api.depends('provider_code', 'operation', 'state', 'sale_order_ids.state')
    def _compute_square_paid_cancelled_order(self):
        for tx in self:
            tx.square_paid_cancelled_order = (
                tx.provider_code == 'square_online' and tx.operation == 'online_direct' and tx.state == 'done'
                and any(order.state == 'cancel' for order in tx.sudo().sale_order_ids)
            )

    def action_square_refresh_status(self):
        self.ensure_one()
        if not (
            self.env.user.has_group('account.group_account_manager')
            or self.env.user.has_group('base.group_system')
        ):
            raise AccessError(_("Only accounting managers and administrators can refresh Square payments."))
        self.check_access('read')
        if self.company_id not in self.env.companies:
            raise AccessError(_("This payment belongs to a different company."))
        if self.provider_code != 'square_online' or not self.square_reserved_at:
            raise UserError(_("This transaction has no Square payment to refresh."))

        self.sudo()._square_refresh_status()
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def _square_lock_for_status(self):
        self.ensure_one()
        self.env.cr.execute('SELECT id FROM payment_transaction WHERE id = %s FOR NO KEY UPDATE', [self.id])

    def _square_recovery_credentials(self):
        self.ensure_one()
        connection = self.square_connection_id
        provider = self.provider_id
        if (
            not connection or connection.provider_id != provider or connection.merchant_id != self.square_merchant_id
            or provider.square_environment != self.square_environment
            or provider.square_application_id != self.square_application_id
            or not connection.access_token
            or not connection.expires_at or connection.expires_at <= fields.Datetime.now()
        ):
            raise UserError(_("Reconnect the original Square account to recover this transaction."))
        return connection._unseal(connection.access_token)

    def _square_refresh_status(self, *, payment_id=None):
        """Read and apply an existing operation under the same lock as checkout dispatch."""
        self.ensure_one()
        self._square_lock_for_status()
        if self.operation != 'online_direct':
            return False
        if not self.square_dispatched_at:
            if (
                self.state == 'pending' and self.square_reserved_at
                and self.square_reserved_at <= fields.Datetime.now() - _DISPATCH_RESERVATION_TIMEOUT
            ):
                # CreatePayment is only sent after checkout commits its dispatch time under this
                # lock. Once cancelled here, a resumed checkout worker cannot send the payment.
                self._set_canceled(state_message=_("Square payment was interrupted before it was sent. You can try again."))
                self._square_post_process_recovered_payment()
                return True
            return False

        try:
            verified = self._square_refresh_payment(self._square_recovery_credentials(), payment_id=payment_id)
        except UserError as error:
            self.square_recovery_message = error.args[0]
            return False
        except SquareAPIError as error:
            self.square_recovery_message = (
                _("Reconnect the original Square account to recover this transaction.")
                if error.status in (401, 403)
                else _("Odoo could not check the Square payment. Try refreshing its status again later.")
            )
            return False

        if not verified:
            self.square_recovery_message = _("Square did not return a payment matching this attempt. Odoo will check again.")
            return False

        self.square_recovery_message = False
        self._square_post_process_recovered_payment()
        return True

    def _square_refresh_payment(self, access_token, *, payment_id=None):
        payment_id = payment_id or self.provider_reference
        if payment_id:
            if self.provider_reference and self.provider_reference != payment_id:
                return False
            response = square_request(
                self.square_environment, 'GET', f'/v2/payments/{quote(payment_id, safe="")}',
                access_token=access_token,
            )
            payment = response.get('payment')
            return isinstance(payment, dict) and payment.get('id') == payment_id and self._square_apply_payment(payment)

        # Square cannot filter by Odoo's reference. Scan the original location with a clock-skew
        # margin, then let the same verifier used for checkout confirm the exact attempt and money.
        earliest = self.square_dispatched_at - timedelta(minutes=5)
        params = {'location_id': self.square_location_ref, 'begin_time': earliest.isoformat() + 'Z', 'sort_order': 'ASC'}
        cursors = set()
        while True:
            response = square_request(
                self.square_environment, 'GET', '/v2/payments', access_token=access_token, params=params,
            )
            payments = response.get('payments', [])
            if not isinstance(payments, list):
                return False
            if any(self._square_apply_payment(payment) for payment in payments):
                return True
            cursor = response.get('cursor')
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                return False
            cursors.add(cursor)
            params = {**params, 'cursor': cursor}

    def _square_post_process_recovered_payment(self):
        if self.state not in ('done', 'error', 'cancel') or self.is_post_processed:
            return
        try:
            with self.env.cr.savepoint():
                self.with_company(self.company_id)._post_process()
        except OperationalError:
            raise
        except Exception:
            # Keep the verified payment even if an Odoo accounting error needs attention.
            _logger.exception('Could not post-process recovered Square transaction %s', self.id)
            self.square_recovery_message = _("Odoo recorded the Square result but could not finish processing this transaction.")

    def _post_process(self):
        # Customer polling, Odoo's cron and Square recovery can all reach native post-processing.
        # The transaction row serializes the business effects across those entry points.
        square_transactions = self.filtered(lambda tx: tx.provider_code == 'square_online')
        super(PaymentTransaction, self - square_transactions)._post_process()
        for tx in square_transactions.sorted('id'):
            tx._square_lock_for_status()
            if not tx.is_post_processed:
                super(PaymentTransaction, tx)._post_process()

    @api.model
    def _square_payment_recovery_domain(self):
        return [
            ('provider_code', '=', 'square_online'), ('operation', '=', 'online_direct'),
            ('square_reserved_at', '!=', False),
            '|', ('state', '=', 'pending'), '&', ('state', '=', 'done'), ('is_post_processed', '=', False),
        ]

    @api.model
    def _cron_square_reconcile(self):
        now = fields.Datetime.now()
        candidates = self.search([
            *self._square_payment_recovery_domain(),
            '|', ('square_recovery_next_check', '=', False), ('square_recovery_next_check', '<=', now),
        ], order='square_recovery_next_check ASC NULLS FIRST, id', limit=5)
        for tx_id in candidates.ids:
            try:
                with self.env.registry.cursor() as cr:
                    tx = self.with_env(api.Environment(cr, SUPERUSER_ID, {})).browse(tx_id)
                    tx._square_lock_for_status()
                    if not tx.exists() or (tx.square_recovery_next_check and tx.square_recovery_next_check > now):
                        continue
                    tx.square_recovery_next_check = now + timedelta(minutes=5)
                    try:
                        with cr.savepoint():
                            tx._square_reconcile_payment()
                    except OperationalError:
                        raise
                    except Exception:
                        _logger.exception('Could not reconcile Square transaction %s', tx_id)
            except (SerializationFailure, DeadlockDetected):
                continue

    def _square_reconcile_payment(self):
        if self.state == 'pending':
            self._square_refresh_status()
        elif self.state == 'done' and not self.is_post_processed:
            self.square_recovery_message = False
            self._square_post_process_recovered_payment()

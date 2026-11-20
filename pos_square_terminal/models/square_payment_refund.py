from urllib.parse import quote

from psycopg2.errors import UniqueViolation

from odoo import _, api, fields, models
from odoo.exceptions import UserError

from ..square_api import SquareAPIError


class SquarePaymentRefund(models.Model):
    _name = 'pos.square.payment.refund'
    _description = 'Square Terminal Refund'
    _rec_name = 'square_refund_id'

    attempt_id = fields.Many2one('pos.square.payment.attempt', required=True, ondelete='restrict', index=True)
    confirmed_attempt_id = fields.Many2one(
        'pos.square.payment.attempt', compute='_compute_confirmed_attempt', store=True, index=True,
    )
    company_id = fields.Many2one(related='attempt_id.company_id', store=True)
    environment = fields.Char(required=True)
    merchant_id = fields.Char(required=True)
    currency_id = fields.Many2one(related='attempt_id.currency_id')
    square_refund_id = fields.Char(string='Square Refund Reference', required=True)
    status = fields.Selection(
        [('PENDING', 'Pending'), ('COMPLETED', 'Completed'), ('REJECTED', 'Rejected'), ('FAILED', 'Failed')],
        string='Square Refund Status', required=True, default='COMPLETED',
    )
    amount_minor = fields.Integer(required=True)
    amount = fields.Monetary(compute='_compute_amount', compute_sudo=True)

    _sql_constraints = [
        ('square_refund_unique', 'unique(environment, merchant_id, square_refund_id)',
         'This Square refund has already been recorded.'),
    ]

    @api.depends('attempt_id', 'status')
    def _compute_confirmed_attempt(self):
        for refund in self:
            refund.confirmed_attempt_id = refund.attempt_id if refund.status == 'COMPLETED' else False

    @api.depends('amount_minor', 'attempt_id.square_order_link_id.currency_digits')
    def _compute_amount(self):
        for refund in self:
            refund.amount = refund.amount_minor / (10 ** refund.attempt_id.square_order_link_id.currency_digits)

    @api.model
    def _read_square_refund(self, connection, refund_id):
        refund = connection._request('GET', '/v2/refunds/' + quote(refund_id, safe='')).get('refund')
        if (
            not isinstance(refund, dict) or refund.get('id') != refund_id
            or refund.get('status') not in ('PENDING', 'COMPLETED', 'REJECTED', 'FAILED')
        ):
            raise ValueError('Square did not identify the requested refund')
        return refund


class SquarePaymentAttempt(models.Model):
    _inherit = 'pos.square.payment.attempt'

    square_refund_ids = fields.One2many('pos.square.payment.refund', 'attempt_id', string='Square Refunds')
    refund_ids = fields.One2many(
        'pos.square.payment.refund', 'confirmed_attempt_id', string='Confirmed Square Refunds',
    )
    refunded_amount = fields.Monetary(
        string='Confirmed Refund Total', compute='_compute_refunded_amount', compute_sudo=True,
    )
    refund_reconciliation_issue = fields.Char(string='Refund Check Issue', copy=False)

    @api.depends('refund_ids.amount_minor', 'square_order_link_id.currency_digits')
    def _compute_refunded_amount(self):
        for attempt in self:
            attempt.refunded_amount = sum(attempt.refund_ids.mapped('amount_minor')) / (
                10 ** attempt.square_order_link_id.currency_digits
            )

    def _observe_verified_payment(self, payment):
        super()._observe_verified_payment(payment)
        refunds = self.env['pos.square.payment.refund']
        refund_ids = payment.get('refund_ids', [])
        issue = False
        if not isinstance(refund_ids, list) or any(
            not isinstance(refund_id, str) or not refund_id for refund_id in refund_ids
        ):
            issue = _("Square did not return valid refund references for this payment.")
        else:
            known = set(self.refund_ids.mapped('square_refund_id'))
            for refund_id in dict.fromkeys(refund_ids):
                if refund_id in known:
                    continue
                try:
                    self._record_square_refund(refunds._read_square_refund(self.connection_id, refund_id))
                except UserError:
                    issue = _("Check the authorization of %s, the original Square connection.",
                              self.connection_id.display_name)
                    break
                except SquareAPIError as error:
                    if error.is_authorization_error:
                        issue = _("Reconnect %s, the original Square connection.", self.connection_id.display_name)
                        break
                    issue = _("Square could not return all refunds for this payment.")
                except ValueError:
                    issue = self._square_refund_verification_issue()
        self.refund_reconciliation_issue = issue or self._square_refund_total_issue(payment)

    def _is_fully_refunded(self):
        self.ensure_one()
        captured = self.verified_base_minor + self.verified_tip_minor
        return (
            self.has_verified_amounts and captured > 0 and not self.refund_reconciliation_issue
            and sum(self.refund_ids.mapped('amount_minor')) == captured
        )

    def _square_refund_verification_issue(self):
        return _("Square returned a refund that could not be verified against the original payment.")

    def _square_refund_amount_minor(self, money):
        if (
            isinstance(money, dict) and money.get('currency') == self.currency_code
            and type(money.get('amount')) is int
        ):
            return money['amount']
        return None

    def _square_refund_total_issue(self, payment=None):
        confirmed_minor = sum(self.refund_ids.mapped('amount_minor'))
        captured_minor = self.verified_base_minor + self.verified_tip_minor
        if confirmed_minor > captured_minor:
            return _("Confirmed Square refunds exceed the original captured amount.")
        if payment is not None and 'refunded_money' in payment:
            reported_minor = self._square_refund_amount_minor(payment['refunded_money'])
            if reported_minor is None or reported_minor < 0:
                return _("Square's refund total has an invalid amount or currency.")
            if reported_minor > captured_minor:
                return _("Square's reported refunds exceed the original captured amount.")
            pending_refunds = self.square_refund_ids.filtered_domain([
                ('status', '=', 'PENDING'), ('square_refund_id', 'in', payment.get('refund_ids', [])),
            ])
            pending_minor = sum(pending_refunds.mapped('amount_minor'))
            if reported_minor not in (confirmed_minor, confirmed_minor + pending_minor):
                return _("Square's refund total differs from the confirmed refund details.")
        return False

    def _record_square_refund(self, refund):
        self.ensure_one()
        if (
            self.state not in ('captured', 'verified') or refund.get('unlinked')
            or refund.get('payment_id') != self.square_payment_id
            or refund.get('location_id', self.square_location_id) != self.square_location_id
        ):
            raise ValueError('Square refund does not match the original payment')
        status = refund['status']
        amount_minor = self._square_refund_amount_minor(refund.get('amount_money'))
        if amount_minor is None or amount_minor <= 0:
            raise ValueError('Invalid Square refund amount or currency')
        refunds = self.env['pos.square.payment.refund']
        existing = refunds.search([
            ('environment', '=', self.environment), ('merchant_id', '=', self.merchant_id),
            ('square_refund_id', '=', refund['id']),
        ], limit=1)
        if existing:
            if existing.attempt_id != self or existing.amount_minor != amount_minor:
                raise ValueError('Square refund differs from the previously recorded refund')
            # A stale pending response must not regress a refund observed after it.
            if existing.status not in ('COMPLETED', status) and status != 'PENDING':
                existing.status = status
            return existing.status != 'PENDING'
        try:
            with self.env.cr.savepoint():
                refunds.create({
                    'attempt_id': self.id,
                    'environment': self.environment,
                    'merchant_id': self.merchant_id,
                    'square_refund_id': refund['id'],
                    'amount_minor': amount_minor,
                    'status': status,
                })
        except UniqueViolation as error:
            if error.diag.constraint_name != 'pos_square_payment_refund_square_refund_unique':
                raise
            raise ValueError('Square refund already belongs to another payment') from None
        return status != 'PENDING'

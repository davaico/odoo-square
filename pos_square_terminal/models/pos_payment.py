from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError

from .pos_order import PAID_STATES, checkout_create_parents, same_value


class PosPayment(models.Model):
    _inherit = 'pos.payment'

    square_attempt_state = fields.Char(compute='_compute_square_attempt_state')
    square_attempt_reference = fields.Char(compute='_compute_square_attempt_state')
    square_cash_reserved = fields.Boolean(compute='_compute_square_attempt_state')

    _square_evidence_fields = {
        'payment_status', 'transaction_id', 'card_type', 'card_brand', 'card_no', 'cardholder_name',
        'payment_ref_no', 'payment_method_authcode', 'payment_method_issuer_bank',
        'payment_method_payment_mode', 'ticket',
    }

    def _compute_square_attempt_state(self):
        attempts = self.pos_order_id._square_attempts()
        attempts_by_payment = {attempt.payment_id.id: attempt for attempt in attempts}
        reserved_cash_ids = set(attempts.square_order_link_id.filtered(lambda link: not link.cancelled)._cash_payment_lines().ids)
        for payment in self:
            attempt = attempts_by_payment.get(payment.id)
            payment.square_attempt_state = attempt._cashier_state() if attempt else False
            payment.square_attempt_reference = attempt.reference if attempt else False
            payment.square_cash_reserved = payment.id in reserved_cash_ids

    @api.model_create_multi
    def create(self, vals_list):
        vals_list, orders = checkout_create_parents(self, vals_list, 'pos_order_id')
        orders._square_lock_checkout(touch=True)
        payments = super().create(vals_list)
        for payment in payments:
            order = payment.pos_order_id
            attempts = order._square_current_attempts()
            if attempts:
                order._square_check_new_share(payment, attempts)
        for payment in payments.filtered(lambda line: line.payment_method_id.use_payment_terminal == 'square_terminal'):
            if (
                payment.pos_order_id.state in PAID_STATES
                or payment.payment_status not in (False, 'pending', 'waiting')
                or any(payment[field] for field in payment._square_evidence_fields - {'payment_status'})
            ):
                raise AccessError(_("Square verifies the payment status and provider references."))
        return payments

    def write(self, vals):
        identity_fields = {'uuid', 'pos_order_id', 'payment_method_id', 'amount', 'is_change'}
        changes = identity_fields.intersection(vals)
        evidence = self._square_evidence_fields.intersection(vals)
        if not (changes or evidence):
            return super().write(vals)

        self.check_access('write')
        target_order = self.env['pos.order'].browse(vals.get('pos_order_id') or [])
        orders = self.pos_order_id | target_order
        orders._square_lock_checkout(touch=True)
        reserved = orders._square_reserved_attempts()
        reserved_order_ids = set(reserved.order_id.ids)
        reserved_payment_ids = set((reserved.payment_id | reserved.square_order_link_id._cash_payment_lines()).ids)
        for payment in self:
            payment_vals = dict(vals)
            new_method = self.env['pos.payment.method'].browse(vals.get('payment_method_id') or [])
            order = payment.pos_order_id
            if reserved_order_ids.intersection({order.id, target_order.id}):
                fixed = changes if payment.id in reserved_payment_ids else changes - {'amount'}
                if any(not same_value(payment, field, payment_vals[field]) for field in fixed):
                    raise UserError(_("The original tender is reserved by a Square payment."))
                for field in evidence:
                    # Native draft synchronization can replay the pre-verification values.
                    if not same_value(payment, field, payment_vals[field]) and (
                        (field == 'payment_status' and payment_vals[field] not in (False, 'pending', 'waiting'))
                        or (field != 'payment_status' and payment_vals[field])
                    ):
                        raise AccessError(_("Square verifies the payment status and provider references."))
                    payment_vals.pop(field)
            elif 'square_terminal' in (payment.payment_method_id.use_payment_terminal, new_method.use_payment_terminal):
                fields_to_check = (
                    self._square_evidence_fields
                    if new_method.use_payment_terminal == 'square_terminal' else evidence
                )
                if (target_order or payment.pos_order_id).state in PAID_STATES or any(
                    payment_vals.get(field, payment[field]) not in (
                        (False, 'pending', 'waiting') if field == 'payment_status' else (False,)
                    )
                    for field in fields_to_check
                ):
                    raise AccessError(_("Square verifies the payment status and provider references."))
            super(PosPayment, payment).write(payment_vals)
            if 'amount' in payment_vals and order.id in reserved_order_ids and payment.id not in reserved_payment_ids:
                order._square_check_new_share(payment, order._square_current_attempts())
        return True

    def _square_set_verified(self, payment_id, amount=None, *, cash=False):
        self.ensure_one()
        values = {'transaction_id': payment_id}
        if not cash:
            values.update(payment_status='done', amount=amount)
        super(PosPayment, self.sudo()).write(values)
        self.invalidate_recordset(['square_attempt_state', 'square_attempt_reference', 'square_cash_reserved'])

    def unlink(self):
        self.check_access('unlink')
        orders = self.pos_order_id
        orders._square_lock_checkout(touch=True)
        attempts = orders._square_reserved_attempts()
        if (attempts.payment_id | attempts.square_order_link_id._cash_payment_lines()) & self:
            raise UserError(_("The original tender is reserved by a Square payment."))
        return super().unlink()

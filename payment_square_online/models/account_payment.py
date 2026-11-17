from odoo import api, models


class AccountPayment(models.Model):
    _inherit = 'account.payment'

    @api.depends('payment_transaction_id.child_transaction_ids.state',
                 'payment_transaction_id.child_transaction_ids.square_amount_minor',
                 'payment_transaction_id.square_refund_recheck_minor')
    @api.depends_context('uid')
    def _compute_amount_available_for_refund(self):
        square_payments = self.filtered(
            lambda payment: payment.sudo().payment_transaction_id.provider_code == 'square_online',
        )
        super(AccountPayment, self - square_payments)._compute_amount_available_for_refund()
        can_refund = self.env['payment.transaction']._square_can_refund()
        for payment in square_payments:
            tx = payment.sudo().payment_transaction_id
            minor = tx._square_refundable_minor() if can_refund else 0
            payment.amount_available_for_refund = tx._square_refund_amount(minor)

    def action_refund_wizard(self):
        self.ensure_one()
        self.check_access('read')
        tx = self.payment_transaction_id
        if tx.provider_code == 'square_online':
            tx._square_check_refund_access()
        return super().action_refund_wizard()

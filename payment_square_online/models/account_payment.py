# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import api, models

from ..square_api import MINOR_UNIT_DIGITS


class AccountPayment(models.Model):
    _inherit = "account.payment"

    @api.depends("payment_transaction_id.square_online_refund_revision", "payment_transaction_id.state")
    @api.depends_context("uid")
    def _compute_amount_available_for_refund(self):
        square_payments = self.filtered(lambda p: p.payment_transaction_id.sudo().provider_code == "square_online")
        super(AccountPayment, self - square_payments)._compute_amount_available_for_refund()
        allowed = (
            self.env.user.has_group("account.group_account_manager")
            or self.env.user.has_group("base.group_system")
        )
        for payment in square_payments:
            tx = payment.payment_transaction_id.sudo()
            digits = MINOR_UNIT_DIGITS.get(tx.square_online_currency_code, 0)
            payment.amount_available_for_refund = tx._square_online_refundable_minor() / 10 ** digits if allowed else 0

    def action_refund_wizard(self):
        self.ensure_one()
        tx = self.payment_transaction_id
        if tx.sudo().provider_code == "square_online":
            self.check_access("read")
            tx._square_online_check_refund_access()
        return super().action_refund_wizard()


class PaymentRefundWizard(models.TransientModel):
    _inherit = "payment.refund.wizard"

    @api.depends("amount_available_for_refund", "transaction_id.child_transaction_ids.state")
    def _compute_refunded_amount(self):
        square_wizards = self.filtered(lambda wizard: wizard.transaction_id.sudo().provider_code == "square_online")
        super(PaymentRefundWizard, self - square_wizards)._compute_refunded_amount()
        for wizard in square_wizards:
            completed = wizard.transaction_id.sudo().child_transaction_ids.filtered(
                lambda tx: tx.operation == "refund" and tx.state == "done"
            )
            wizard.refunded_amount = -sum(completed.mapped("amount"))

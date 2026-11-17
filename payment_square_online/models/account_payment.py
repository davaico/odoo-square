# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import _, api, fields, models
from odoo.exceptions import UserError

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

    square_online_refund_submitted = fields.Boolean(
        readonly=True, copy=False, groups="base.group_no_one",
    )

    @api.model_create_multi
    def create(self, vals_list):
        # Monetary fields round on write. Check the operator's original input
        # before it can become a different amount in the native wizard.
        defaults = self.default_get(["payment_id", "amount_to_refund"])
        for vals in vals_list:
            if "amount_to_refund" in vals or "amount_to_refund" in defaults:
                payment = self.env["account.payment"].browse(vals.get("payment_id", defaults.get("payment_id")))
                self._square_online_check_input(payment, vals.get("amount_to_refund", defaults.get("amount_to_refund")))
        return super().create(vals_list)

    def write(self, vals):
        if "amount_to_refund" in vals:
            payments = (
                self.env["account.payment"].browse(vals["payment_id"])
                if "payment_id" in vals else self.payment_id
            )
            self._square_online_check_input(payments, vals["amount_to_refund"])
        return super().write(vals)

    def _square_online_check_input(self, payments, amount):
        for tx in payments.payment_transaction_id.sudo():
            if tx.provider_code == "square_online":
                tx._square_online_validate_refund_amount(amount)

    def action_refund(self):
        self.ensure_one()
        tx = self.transaction_id
        if tx.sudo().provider_code != "square_online":
            return super().action_refund()
        self.check_access("write")
        tx._square_online_check_refund_access()
        # Use the reservation lock so two submits of this wizard cannot both
        # pass the marker check. The native transaction action updates the parent.
        tx.sudo()._square_online_lock_payment()
        if self.sudo().square_online_refund_submitted:
            raise UserError(_("This Square refund was submitted. Check its status in Square Payment History."))
        result = super().action_refund()
        self.sudo().square_online_refund_submitted = True
        return result

    @api.depends("amount_available_for_refund", "transaction_id.child_transaction_ids.state")
    def _compute_refunded_amount(self):
        square_wizards = self.filtered(lambda wizard: wizard.transaction_id.sudo().provider_code == "square_online")
        super(PaymentRefundWizard, self - square_wizards)._compute_refunded_amount()
        for wizard in square_wizards:
            completed = wizard.transaction_id.sudo().child_transaction_ids.filtered(
                lambda tx: tx.operation == "refund" and tx.state == "done"
            )
            digits = MINOR_UNIT_DIGITS.get(wizard.transaction_id.sudo().square_online_currency_code, 0)
            wizard.refunded_amount = sum(completed.mapped("square_online_amount_minor")) / 10 ** digits

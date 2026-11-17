from odoo import _, api, models


class PaymentRefundWizard(models.TransientModel):
    _inherit = 'payment.refund.wizard'

    @api.depends('amount_available_for_refund')
    def _compute_refunded_amount(self):
        square_wizards = self.filtered(lambda wizard: wizard.transaction_id.provider_code == 'square_online')
        super(PaymentRefundWizard, self - square_wizards)._compute_refunded_amount()
        for wizard in square_wizards:
            done = wizard.transaction_id.child_transaction_ids.filtered(
                lambda tx: tx.operation == 'refund' and tx.state == 'done',
            )
            wizard.refunded_amount = -sum(done.mapped('amount'))

    def action_refund(self):
        self.check_access('read')
        self.payment_id.check_access('read')
        square_wizards = self.filtered(lambda wizard: wizard.transaction_id.provider_code == 'square_online')
        square_wizards.transaction_id._square_check_refund_access()
        result = super(PaymentRefundWizard, self - square_wizards).action_refund()
        for wizard in square_wizards:
            refund = wizard.transaction_id._send_refund_request(
                amount_to_refund=wizard.amount_to_refund, wizard_id=wizard.id,
            )
            result = {
                'name': _('Square Refund'),
                'type': 'ir.actions.act_window',
                'res_model': 'payment.transaction',
                'res_id': refund.id,
                'view_mode': 'form',
                'views': [(False, 'form')],
                'target': 'current',
            }
        return result

from odoo import _, api, models


class PaymentRefundWizard(models.TransientModel):
    _inherit = 'payment.refund.wizard'

    @api.model_create_multi
    def create(self, values_list):
        # Check the submitted value before the native Monetary field rounds it.
        defaults = self.default_get(['payment_id', 'amount_to_refund'])
        for values in values_list:
            if 'amount_to_refund' in values or 'amount_to_refund' in defaults:
                payment = self.env['account.payment'].browse(values.get('payment_id', defaults.get('payment_id')))
                tx = payment.payment_transaction_id
                if tx.provider_code == 'square_online':
                    tx._square_refund_minor(values.get('amount_to_refund', defaults.get('amount_to_refund')))
        return super().create(values_list)

    def write(self, values):
        if 'amount_to_refund' in values:
            payments = (
                self.env['account.payment'].browse(values['payment_id'])
                if 'payment_id' in values else self.payment_id
            )
            for tx in payments.payment_transaction_id.filtered(lambda tx: tx.provider_code == 'square_online'):
                tx._square_refund_minor(values['amount_to_refund'])
        return super().write(values)

    @api.depends('amount_available_for_refund', 'transaction_id.child_transaction_ids.state',
                 'transaction_id.child_transaction_ids.square_amount_minor')
    def _compute_refunded_amount(self):
        square_wizards = self.filtered(lambda wizard: wizard.transaction_id.provider_code == 'square_online')
        super(PaymentRefundWizard, self - square_wizards)._compute_refunded_amount()
        for wizard in square_wizards:
            done = wizard.transaction_id.child_transaction_ids.filtered(
                lambda tx: tx.operation == 'refund' and tx.state == 'done',
            )
            wizard.refunded_amount = wizard.transaction_id._square_refund_amount(sum(done.mapped('square_amount_minor')))

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

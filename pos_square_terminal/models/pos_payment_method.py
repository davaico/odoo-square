from odoo import _, api, fields, models
from odoo.exceptions import ValidationError


class PosPaymentMethod(models.Model):
    _inherit = 'pos.payment.method'

    square_terminal_id = fields.Many2one(
        'pos.square.terminal', ondelete='restrict', check_company=True, copy=False,
        groups='point_of_sale.group_pos_manager',
    )
    square_terminal_unavailable_reason = fields.Char(compute='_compute_square_terminal_unavailable_reason')

    def _get_payment_terminal_selection(self):
        return super()._get_payment_terminal_selection() + [('square_terminal', 'Square Terminal')]

    @api.onchange('use_payment_terminal')
    def _onchange_use_payment_terminal(self):
        super()._onchange_use_payment_terminal()
        for method in self:
            if method.use_payment_terminal != 'square_terminal':
                method.square_terminal_id = False

    @api.constrains('square_terminal_id', 'company_id')
    def _check_square_terminal_company(self):
        for method in self.sudo():
            if method.square_terminal_id and method.square_terminal_id.company_id != method.company_id:
                raise ValidationError(_("Select a Square Terminal from this company."))

    @api.depends_context('square_pos_config')
    @api.depends(
        'use_payment_terminal', 'payment_method_type', 'square_terminal_id.unavailable_reason',
        'journal_id.type', 'journal_id.active', 'journal_id.currency_id', 'company_id.currency_id',
        'company_id', 'split_transactions', 'config_ids.currency_id',
    )
    def _compute_square_terminal_unavailable_reason(self):
        config_id = self.env.context.get('square_pos_config')
        config = self.env['pos.config'].browse(config_id).exists() if type(config_id) is int else None
        for method in self:
            method.square_terminal_unavailable_reason = method._square_readiness_error(config)

    def _square_readiness_error(self, config=None):
        """Check the register configuration before initiating a new Square payment."""
        self.ensure_one()
        if self.use_payment_terminal != 'square_terminal':
            return False
        if self.payment_method_type != 'terminal' or self.journal_id.type != 'bank' or not self.journal_id.active:
            return _("Select an active bank journal for Square Terminal.")
        if self.split_transactions:
            return _("Turn off Identify Customer for this Square payment method.")
        terminal = self.sudo().square_terminal_id
        if not terminal:
            return _("Select a paired Square Terminal.")
        if terminal.company_id.id != self.company_id.id or (config and config.company_id != self.company_id):
            return _("The Square Terminal and register must belong to the same company.")
        reason = terminal._readiness_error()
        if reason:
            return reason
        currencies = (config or self.config_ids).currency_id or self.company_id.currency_id
        currencies |= self.journal_id.currency_id
        if any(currency.name != terminal.location_id.currency_code for currency in currencies):
            return _("The Square location currency (%s) must match the register and bank journal.", terminal.location_id.currency_code)
        return False

    @api.model
    def _load_pos_data_fields(self, config_id):
        return super()._load_pos_data_fields(config_id) + ['square_terminal_unavailable_reason']

    @api.model
    def _load_pos_data(self, data):
        config_id = data['pos.config']['data'][0]['id']
        return super(PosPaymentMethod, self.with_context(square_pos_config=config_id))._load_pos_data(data)

# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import api, fields, models


class PosConfig(models.Model):
    _inherit = 'pos.config'

    square_terminal_tipping = fields.Boolean(
        string='Square Terminal Tips',
        help='Prompt each cardholder for a tip on the Square Terminal.',
    )


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    pos_square_terminal_tipping = fields.Boolean(related='pos_config_id.square_terminal_tipping', readonly=False)

    @api.depends('pos_iface_tipproduct', 'pos_square_terminal_tipping', 'pos_config_id.tip_product_id')
    def _compute_pos_tip_product_id(self):
        super()._compute_pos_tip_product_id()
        for settings in self.filtered('pos_square_terminal_tipping'):
            settings.pos_tip_product_id = settings.pos_config_id.tip_product_id

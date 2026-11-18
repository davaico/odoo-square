from odoo import fields, models


class SquareLocation(models.Model):
    _name = 'payment.square.location'
    _description = 'Square Online Payment Location'
    _order = 'name, id'

    provider_id = fields.Many2one('payment.provider', required=True, ondelete='cascade')
    company_id = fields.Many2one(related='provider_id.company_id', store=True, index=True)
    square_id = fields.Char(required=True)
    name = fields.Char(required=True)
    country_code = fields.Char(required=True)
    currency_code = fields.Char(required=True)
    is_available = fields.Boolean(default=True)

    _sql_constraints = [
        ('square_location_unique', 'unique(provider_id, square_id)', 'The Square location already exists for this provider.'),
    ]

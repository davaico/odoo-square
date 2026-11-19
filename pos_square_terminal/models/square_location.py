from odoo import _, api, fields, models

from ..const import SQUARE_LOCATION_CURRENCIES


class SquareLocation(models.Model):
    _name = 'pos.square.location'
    _description = 'Square Terminal Location'
    _order = 'name, id'

    connection_id = fields.Many2one('pos.square.connection', required=True, ondelete='cascade')
    company_id = fields.Many2one(related='connection_id.company_id', store=True, index=True)
    square_id = fields.Char(required=True)
    name = fields.Char(required=True)
    country_code = fields.Char()
    currency_code = fields.Char()
    is_available = fields.Boolean()
    unavailable_reason = fields.Char()

    _sql_constraints = [
        ('connection_square_id', 'unique(connection_id, square_id)', 'This Square location is already on this connection.'),
    ]

    @api.model
    def _square_location_values(self, response, merchant_id):
        locations = response.get('locations', [])
        if not isinstance(locations, list):
            raise ValueError('Invalid Square locations response')
        result = {}
        for location in locations:
            if not isinstance(location, dict) or location.get('merchant_id') != merchant_id:
                raise ValueError('Square location belongs to a different merchant')
            square_id, country, currency = (location.get(key) for key in ('id', 'country', 'currency'))
            if not isinstance(square_id, str) or not square_id or square_id in result:
                raise ValueError('Invalid Square location ID')
            if not all(value is None or isinstance(value, str) for value in (country, currency)):
                raise ValueError('Invalid Square location currency or country')
            capabilities = location.get('capabilities')
            reason = False
            if location.get('status') != 'ACTIVE':
                reason = _("This Square location is inactive.")
            elif not country or not currency or SQUARE_LOCATION_CURRENCIES.get(country) != currency:
                reason = _("Square Terminal with linked orders is unavailable for this location's country or currency.")
            elif not isinstance(capabilities, list) or 'CREDIT_CARD_PROCESSING' not in capabilities:
                reason = _("This Square location cannot process credit cards.")
            result[square_id] = {
                'name': location.get('name') or square_id, 'country_code': country, 'currency_code': currency,
                'is_available': not reason, 'unavailable_reason': reason,
            }
        return result

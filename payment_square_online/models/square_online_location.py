# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import api, fields, models


class SquareOnlineLocation(models.Model):
    _name = "square.online.location"
    _description = "Square Online Location"
    _order = "name, id"
    _check_company_auto = True

    name = fields.Char(required=True, readonly=True)
    connection_id = fields.Many2one(
        "square.online.connection", required=True, ondelete="cascade", readonly=True, index=True,
    )
    company_id = fields.Many2one(related="connection_id.company_id", store=True, index=True)
    square_id = fields.Char(string="Square Location ID", required=True, readonly=True)
    country_code = fields.Char(string="Square Seller Country", required=True, readonly=True)
    currency_code = fields.Char(string="Processing Currency", required=True, readonly=True)
    available_for_payment = fields.Boolean(readonly=True)

    _connection_location_unique = models.Constraint(
        "UNIQUE(connection_id, square_id)", "This location already belongs to this connection."
    )

    @api.depends("name", "country_code", "currency_code")
    def _compute_display_name(self):
        for location in self:
            location.display_name = (
                f"{location.name} ({location.country_code} · {location.currency_code})"
            )

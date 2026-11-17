# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from odoo import models, fields, api
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)


class SquareLocationMapping(models.Model):
    _name = "square.location.mapping"
    _description = "Square Location to Warehouse Mapping"
    _rec_name = "square_location_name"

    config_id = fields.Many2one(
        "square.config",
        string="Square Configuration",
        required=True,
        ondelete="cascade",
    )

    square_location_id = fields.Char(
        string="Square Location ID",
        required=True,
        help="The unique identifier for the Square location",
    )

    square_location_name = fields.Char(
        string="Square Location Name",
        required=True,
        help="The display name for the Square location",
    )

    # Computed field to show available Square locations
    available_square_locations = fields.Json(
        string="Available Square Locations",
        compute="_compute_available_square_locations",
        help="List of available Square locations for selection",
    )

    warehouse_id = fields.Many2one(
        "stock.warehouse",
        string="Odoo Warehouse",
        required=True,
        help="The Odoo warehouse to map to this Square location",
    )

    sales_team_id = fields.Many2one(
        "crm.team",
        string="Sales Team",
        help="Sales Team for orders and invoices from this Square location. "
        "Leave empty to use the default on Square configuration.",
    )

    active = fields.Boolean(default=True)

    _unique_location_per_config = models.Constraint(
        "UNIQUE(config_id, square_location_id)",
        "Each Square location can only be mapped once per configuration",
    )

    @api.constrains("config_id", "warehouse_id")
    def _check_warehouse_mapping(self):
        for mapping in self:
            if mapping.warehouse_id.company_id != mapping.config_id.company_id:
                raise ValidationError("The Square location and warehouse must belong to the same company.")
            if self.search_count([
                ("config_id", "=", mapping.config_id.id),
                ("warehouse_id", "=", mapping.warehouse_id.id),
                ("id", "!=", mapping.id),
            ], limit=1):
                raise ValidationError(
                    f"Warehouse '{mapping.warehouse_id.name}' is already mapped to "
                    "another Square location in this configuration."
                )

    @api.depends("square_location_name", "warehouse_id.name")
    def _compute_display_name(self):
        for record in self:
            record.display_name = f"{record.square_location_name} → {record.warehouse_id.name}"

    @api.depends("config_id")
    def _compute_available_square_locations(self):
        """Compute available Square locations for selection"""
        for record in self:
            if record.config_id:
                locations = self._get_square_locations(record.config_id)
                record.available_square_locations = locations
            else:
                record.available_square_locations = []

    @api.model
    def _get_square_locations(self, config):
        """Get available Square locations from API"""
        try:
            square_api = self.env["square.api.client"].with_company(config.company_id)
            locations = square_api.get_locations()

            if not locations:
                return []

            # Format for selection: [{"id": "location_id", "name": "Location Name"}]
            formatted_locations = []
            for location in locations:
                formatted_locations.append(
                    {
                        "id": location.get("id"),
                        "name": location.get("name", "Unknown Location"),
                    }
                )

            return formatted_locations

        except Exception as e:
            _logger.error(f"Error fetching Square locations: {str(e)}")
            return []

    @api.onchange("square_location_id")
    def _onchange_square_location_id(self):
        """Update location name when location ID changes"""
        if self.square_location_id and self.config_id:
            locations = self._get_square_locations(self.config_id)
            for location in locations:
                if location["id"] == self.square_location_id:
                    self.square_location_name = location["name"]
                    break
        elif not self.square_location_id:
            self.square_location_name = False

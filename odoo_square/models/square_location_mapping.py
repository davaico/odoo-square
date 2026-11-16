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

    @api.model_create_multi
    def create(self, vals_list):
        """Override create to validate warehouse selection"""
        records = super().create(vals_list)

        # Validate each record
        for record in records:
            # Validate that the selected warehouse is not already mapped in this config
            existing_mapping = self.search(
                [
                    ("config_id", "=", record.config_id.id),
                    ("warehouse_id", "=", record.warehouse_id.id),
                    ("id", "!=", record.id),
                ]
            )

            if existing_mapping:
                raise ValidationError(
                    f"Warehouse '{record.warehouse_id.name}' is already mapped to another Square location in this configuration."
                )

            _logger.info(
                f"Created mapping: Square location '{record.square_location_name}' -> "
                f"Odoo warehouse '{record.warehouse_id.name}'"
            )

        return records

    def write(self, vals):
        """Override write to validate warehouse selection"""
        result = super().write(vals)

        if "warehouse_id" in vals:
            # Validate that the selected warehouse is not already mapped in this config
            existing_mapping = self.search(
                [
                    ("config_id", "=", self.config_id.id),
                    ("warehouse_id", "=", self.warehouse_id.id),
                    ("id", "!=", self.id),
                ]
            )

            if existing_mapping:
                raise ValidationError(
                    f"Warehouse '{self.warehouse_id.name}' is already mapped to another Square location in this configuration."
                )

        return result

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
            square_api = self.env["square.api.client"]
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

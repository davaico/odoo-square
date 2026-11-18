# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import models, fields, api
from odoo.exceptions import AccessError, ValidationError


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

    _sql_constraints = [
        (
            "unique_location_per_config",
            "unique(config_id, square_location_id)",
            "Each Square location can only be mapped once per configuration",
        ),
    ]

    @api.constrains("config_id", "warehouse_id", "sales_team_id", "square_location_id", "active")
    def _check_mapping_scope(self):
        for mapping in self:
            mapping.config_id.check_access("write")
            company = mapping.config_id.company_id
            if not company or mapping.warehouse_id.company_id != company:
                raise ValidationError("The warehouse must belong to the Square configuration's company.")
            if mapping.sales_team_id.company_id and mapping.sales_team_id.company_id != company:
                raise ValidationError("The sales team must belong to the Square configuration's company.")
            location_id = mapping.square_location_id
            if not location_id or not location_id.strip() or location_id != location_id.strip():
                raise ValidationError("Enter a valid Square location ID.")
            if mapping.active and self.with_context(active_test=True).search_count([
                ("config_id", "=", mapping.config_id.id),
                ("warehouse_id", "=", mapping.warehouse_id.id),
                ("id", "!=", mapping.id),
            ], limit=1):
                raise ValidationError("A warehouse can only have one active mapping per Square configuration.")

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
        # The form can call onchange on a virtual record before any ACL is checked.
        if not self.env.is_system():
            raise AccessError("Only Square administrators can read available Square locations.")
        if config._origin:
            config._origin.check_access("read")
        locations = self.env["square.api.client"]._get_locations(square_config=config)
        return [
            {"id": location["id"], "name": location.get("name", "Unknown Location")}
            for location in locations
            if isinstance(location, dict) and location.get("id")
        ]

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

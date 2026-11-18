# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from odoo import api, fields, models
from odoo.exceptions import ValidationError

from .square_order_processor import SquareOrderCompletionDeferred

_logger = logging.getLogger(__name__)


class SquareConfig(models.Model):
    _name = "square.config"
    _description = "Square Configuration"
    _rec_name = "name"

    name = fields.Char(string="Name", default="Square Configuration", required=True)

    company_id = fields.Many2one(
        "res.company", required=True, default=lambda self: self.env.company,
    )

    # Square API Configuration
    square_application_id = fields.Char(
        string="Square Application ID",
        help="Your Square Application ID from the Square Developer Dashboard",
        required=True,
    )

    square_access_token = fields.Char(
        string="Square Access Token",
        groups="base.group_system,odoo_square.group_square_pos_service",
        help="Your Square Access Token for API authentication",
        required=True,
    )

    square_environment = fields.Selection(
        [("sandbox", "Sandbox"), ("production", "Production")],
        string="Environment",
        default="sandbox",
        required=True,
        help="Choose the Square environment to connect to",
    )

    square_webhook_signature_key = fields.Char(
        string="Webhook Signature Key",
        groups="base.group_system,odoo_square.group_square_pos_service",
        help="Webhook signature key for secure webhook verification",
    )

    # Location to Warehouse Mapping
    location_mapping_ids = fields.One2many(
        "square.location.mapping",
        "config_id",
        string="Square Location Mappings",
        help="Map each Square location to an Odoo warehouse and optional Sales Team",
    )

    default_sales_team_id = fields.Many2one(
        "crm.team",
        string="Default Sales Team",
        help="Used when a location mapping has no Sales Team, or for unknown locations "
        "(same fallback order as warehouse).",
    )

    # Payment Journal Configuration
    payment_journal_id = fields.Many2one(
        "account.journal",
        string="Payment Journal",
        help="Select the journal where Square payments will be recorded",
        context={"create": True},
    )

    # Connection Status
    connection_status = fields.Selection(
        [
            ("not_configured", "Not Configured"),
            ("configured", "Configured"),
            ("connected", "Connected"),
            ("error", "Connection Error"),
        ],
        string="Connection Status",
        default="not_configured",
        readonly=True,
    )

    active = fields.Boolean(default=True)

    module_version_display = fields.Char(
        string="Integration module version",
        compute="_compute_module_version_display",
        help="Installed version of the Odoo Square integration module (from Apps).",
    )

    @api.depends("name")
    def _compute_module_version_display(self):
        module = self.env["ir.module.module"].sudo().search(
            [("name", "=", "odoo_square")], limit=1
        )
        raw = (module.latest_version or "").strip()
        display = self._odoo_version_to_semver(raw) if raw else ""
        for rec in self:
            rec.module_version_display = display or "Unknown (upgrade module in Apps)"

    @staticmethod
    def _odoo_version_to_semver(version):
        """Map manifest form 17.0.1.0.1 to semantic module part 1.0.1."""
        parts = version.split(".")
        if len(parts) >= 5 and all(p.isdigit() for p in parts[:5]):
            return ".".join(parts[2:5])
        return version

    def _valid_location_mappings(self):
        self.ensure_one()
        return self.location_mapping_ids.filtered(
            lambda mapping: mapping.active and mapping.warehouse_id.company_id == self.company_id
        )

    def get_warehouse_for_location(self, square_location_id):
        """Get the configured warehouse for a specific Square location"""
        self.ensure_one()

        # Find mapping for this location
        mapping = self.with_context(active_test=False).location_mapping_ids.filtered(
            lambda m: m.square_location_id == square_location_id
        )

        if mapping:
            if not mapping.active:
                raise SquareOrderCompletionDeferred("The warehouse mapping for this Square location is archived.")
            if mapping.warehouse_id.sudo().company_id != self.company_id:
                raise SquareOrderCompletionDeferred("The Square location is mapped to a warehouse in another company.")
            _logger.info(
                f"Using mapped warehouse '{mapping.warehouse_id.name}' for Square location '{square_location_id}'"
            )
            return mapping.warehouse_id

        # Fallback: try to find any mapping (useful for single-location setups)
        mappings = self._valid_location_mappings()
        if mappings:
            first_mapping = mappings[0]
            _logger.warning(
                f"No specific mapping found for Square location '{square_location_id}', "
                f"using first available warehouse: {first_mapping.warehouse_id.name}"
            )
            return first_mapping.warehouse_id

        # Last resort: use first available warehouse
        warehouse = self.env["stock.warehouse"].search([("company_id", "=", self.company_id.id)], limit=1)
        if warehouse:
            _logger.warning(
                f"No warehouse mappings configured for Square, using default warehouse: {warehouse.name}"
            )
            return warehouse
        return None

    def _sales_team_matches_company(self, team, company):
        if not team:
            return False
        if not company:
            return True
        if not team.company_id:
            return True
        return team.company_id.id == company.id

    def get_sales_team_for_location(self, square_location_id, company=None):
        """Sales Team for a Square location: mapping → first mapping → default."""
        self.ensure_one()
        company = company or self.env.company

        def pick(team):
            return team if self._sales_team_matches_company(team, company) else False

        mapping = self.location_mapping_ids.filtered(
            lambda m: m.square_location_id == square_location_id
        )[:1]

        if mapping and mapping.sales_team_id:
            team = pick(mapping.sales_team_id)
            if team:
                _logger.info(
                    "Square: sales team '%s' for location '%s'",
                    team.name,
                    square_location_id,
                )
                return team

        if self.location_mapping_ids:
            first = self.location_mapping_ids[0]
            if first.sales_team_id:
                team = pick(first.sales_team_id)
                if team:
                    _logger.warning(
                        "Square: no team for location '%s'; using first mapping team '%s'",
                        square_location_id,
                        team.name,
                    )
                    return team

        if self.default_sales_team_id:
            team = pick(self.default_sales_team_id)
            if team:
                _logger.warning(
                    "Square: using default sales team '%s' for location '%s'",
                    team.name,
                    square_location_id,
                )
                return team

        return self.env["crm.team"].browse()

    def get_configured_warehouse(self):
        """Get the configured warehouse for Square operations (legacy method)"""
        self.ensure_one()

        # For backward compatibility, return first mapped warehouse or default
        mappings = self._valid_location_mappings()
        if mappings:
            first_mapping = mappings[0]
            _logger.info(
                f"Using first mapped warehouse: {first_mapping.warehouse_id.name}"
            )
            return first_mapping.warehouse_id

        # Fallback to first warehouse if none configured
        warehouse = self.env["stock.warehouse"].search([("company_id", "=", self.company_id.id)], limit=1)
        if warehouse:
            _logger.warning(
                f"No warehouse mappings configured for Square, using default warehouse: {warehouse.name}"
            )
            return warehouse
        return None

    def get_payment_journal(self):
        """Get the configured payment journal for Square payments"""
        self.ensure_one()
        if self.payment_journal_id.company_id == self.company_id:
            _logger.info(
                f"Using configured payment journal: {self.payment_journal_id.name}"
            )
            return self.payment_journal_id
        else:
            # Fallback to first available bank journal if none configured
            bank_journal = self.env["account.journal"].search(
                [("type", "=", "bank"), ("company_id", "=", self.company_id.id)], limit=1
            )
            if bank_journal:
                _logger.warning(
                    f"No payment journal configured for Square, using default bank journal: {bank_journal.name}"
                )
                return bank_journal
            return None

    @api.model_create_multi
    def create(self, vals_list):
        # Ensure only one configuration exists
        if len(vals_list) > 1 or self.sudo().with_context(active_test=False).search_count([]):
            raise ValidationError(
                "Only one Square configuration is allowed. Please modify the existing configuration."
            )
        configs = super().create(vals_list)
        configs._allow_bot_in_company()
        return configs

    def write(self, vals):
        result = super().write(vals)
        if "company_id" in vals:
            self._allow_bot_in_company()
        return result

    def _allow_bot_in_company(self):
        bot = self.env.ref("odoo_square.user_square_bot")
        companies = self.company_id - bot.company_ids
        if companies:
            bot.sudo().write({"company_ids": [fields.Command.link(company.id) for company in companies]})

    def test_square_connection(self):
        """Test the connection to Square API"""
        self.ensure_one()
        self.check_access("write")

        try:
            square_api = self.env["square.api.client"].with_company(self.company_id)
            result = square_api.test_connection()

            if result["success"]:
                self.connection_status = "connected"
                return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params": {
                        "title": "Square Connection Test",
                        "message": result["message"],
                        "type": "success",
                        "sticky": False,
                    },
                }
            else:
                self.connection_status = "error"
                return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params": {
                        "title": "Square Connection Test Failed",
                        "message": result["message"],
                        "type": "danger",
                        "sticky": True,
                    },
                }

        except Exception as e:
            self.connection_status = "error"
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Square Connection Test Error",
                    "message": f"Connection test failed: {str(e)}",
                    "type": "danger",
                    "sticky": True,
                },
            }

    def sync_square_locations(self):
        """Sync Square locations and create mappings for unmapped locations"""
        self.ensure_one()
        self.check_access("write")

        locations = self.env["square.api.client"].with_company(self.company_id).get_locations()
        if not locations:
            _logger.warning("No Square locations found to sync")
            return False

        existing_ids = set(
            self.with_context(active_test=False).location_mapping_ids.mapped("square_location_id")
        )
        mappings = []
        for location in locations:
            location_id = location["id"]
            if location_id in existing_ids:
                continue
            name = location.get("name") or "Unknown Location"
            warehouse = self._suggest_warehouse_for_location(name)
            mappings.append({
                "config_id": self.id,
                "square_location_id": location_id,
                "square_location_name": name,
                "warehouse_id": warehouse.id,
            })
            existing_ids.add(location_id)

        if mappings:
            with self.env.cr.savepoint():
                self.env["square.location.mapping"].create(mappings)
            _logger.info("Created %s new location mappings", len(mappings))
        return True

    def _suggest_warehouse_for_location(self, location_name):
        """Suggest a warehouse for a Square location based on name similarity"""
        company_domain = [("company_id", "=", self.company_id.id)]

        warehouse = self.env["stock.warehouse"]
        full_match = warehouse.search(company_domain + [("name", "ilike", location_name)], limit=1)
        if full_match:
            return full_match

        # Try location name in warehouse name
        location_words = location_name.lower().split()
        for word in location_words:
            if len(word) > 3:  # Only check meaningful words
                word_match = warehouse.search(
                    company_domain + [("name", "ilike", word)], limit=1
                )
                if word_match:
                    return word_match

        # Fallback to first available warehouse
        return warehouse.search(company_domain, limit=1)

    def action_open_manual_resync_wizard(self):
        """Open the manual resync wizard for missing orders"""
        self.ensure_one()

        self.check_access("write")

        # Create or get the wizard
        wizard = self.env["square.manual.resync.wizard"].create(
            {
                "config_id": self.id,
            }
        )

        # Return wizard form view
        return {
            "type": "ir.actions.act_window",
            "res_model": "square.manual.resync.wizard",
            "res_id": wizard.id,
            "view_mode": "form",
            "view_type": "form",
            "target": "new",
        }

# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from odoo import Command, api, fields, models
from odoo.exceptions import AccessError, ValidationError

from .square_order_processor import SquareOrderProcessingDeferred

_logger = logging.getLogger(__name__)


class SquareConfig(models.Model):
    _name = "square.config"
    _description = "Square Configuration"
    _rec_name = "name"
    _check_company_auto = True

    name = fields.Char(string="Name", default="Square Configuration", required=True)

    company_id = fields.Many2one(
        "res.company", default=lambda self: self.env.company, required=True,
        readonly=True, index=True,
    )

    # Square API Configuration
    square_application_id = fields.Char(
        string="Square Application ID",
        help="Your Square Application ID from the Square Developer Dashboard",
        required=True,
    )

    square_access_token = fields.Char(
        string="Square Access Token",
        help="Your Square Access Token for API authentication",
        groups="base.group_system",
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
        help="Webhook signature key for secure webhook verification",
        groups="base.group_system",
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
        check_company=True,
        help="Used when a location mapping has no Sales Team, or for unknown locations "
        "(same fallback order as warehouse).",
    )

    # Payment Journal Configuration
    payment_journal_id = fields.Many2one(
        "account.journal",
        string="Payment Journal",
        check_company=True,
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

    def _company_location_mappings(self):
        self.ensure_one()
        return self.sudo().location_mapping_ids.filtered(
            lambda mapping: mapping.warehouse_id.company_id == self.company_id
        )

    def _default_warehouse(self):
        self.ensure_one()
        return self.env["stock.warehouse"].search(
            [("company_id", "=", self.company_id.id)], limit=1
        )

    def _get_warehouse_for_location(self, square_location_id):
        """Get the configured warehouse for a specific Square location"""
        self.ensure_one()

        # Refuse an explicit cross-company mapping retained from an earlier
        # installation; falling back would fulfill at an arbitrary warehouse.
        mapping = self.sudo().with_context(active_test=False).location_mapping_ids.filtered(
            lambda m: m.square_location_id == square_location_id
        )

        if mapping:
            if not mapping.active:
                raise SquareOrderProcessingDeferred("The warehouse mapping for this Square location is archived.")
            if mapping.warehouse_id.company_id != self.company_id:
                raise SquareOrderProcessingDeferred("The Square location is mapped to a warehouse in another company.")
            _logger.info(
                f"Using mapped warehouse '{mapping.warehouse_id.name}' for Square location '{square_location_id}'"
            )
            return mapping.warehouse_id

        # Fallback: try to find any mapping (useful for single-location setups)
        mappings = self._company_location_mappings()
        if mappings:
            first_mapping = mappings[0]
            _logger.warning(
                f"No specific mapping found for Square location '{square_location_id}', "
                f"using first available warehouse: {first_mapping.warehouse_id.name}"
            )
            return first_mapping.warehouse_id

        # Last resort: use first available warehouse
        warehouse = self._default_warehouse()
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

    def _get_sales_team_for_location(self, square_location_id, company=None):
        """Sales Team for a Square location: mapping → first mapping → default."""
        self.ensure_one()
        company = company or self.env.company

        def pick(team):
            return team if self._sales_team_matches_company(team, company) else False

        mapping = self._company_location_mappings().filtered(
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

        mappings = self._company_location_mappings()
        if mappings:
            first = mappings[0]
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

    def _get_configured_warehouse(self):
        """Get the configured warehouse for Square operations (legacy method)"""
        self.ensure_one()

        # For backward compatibility, return first mapped warehouse or default
        mappings = self._company_location_mappings()
        if mappings:
            first_mapping = mappings[0]
            _logger.info(
                f"Using first mapped warehouse: {first_mapping.warehouse_id.name}"
            )
            return first_mapping.warehouse_id

        # Fallback to first warehouse if none configured
        warehouse = self._default_warehouse()
        if warehouse:
            _logger.warning(
                f"No warehouse mappings configured for Square, using default warehouse: {warehouse.name}"
            )
            return warehouse
        return None

    def _get_payment_journal(self):
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
        if len(vals_list) != 1 or self.sudo().with_context(active_test=False).search_count([]):
            raise ValidationError(
                "Only one Square configuration is allowed. Please modify the existing configuration."
            )
        config = super().create(vals_list)
        config._authorize_company_for_bot()
        return config

    def _authorize_company_for_bot(self):
        self.ensure_one()
        bot = self.env.ref("odoo_square.user_square_bot").sudo()
        if self.company_id not in bot.company_ids:
            bot.write({"company_ids": [Command.link(self.company_id.id)]})

    def write(self, vals):
        self.check_access("write")
        if "company_id" in vals and any(config.company_id.id != vals["company_id"] for config in self):
            raise ValidationError("The company of an existing Square configuration cannot be changed.")
        routing_fields = {"square_access_token", "square_environment", "square_webhook_signature_key", "active"}
        changed = self.filtered(lambda config: any(
            config.sudo()[key] != vals[key] for key in routing_fields.intersection(vals)
        ))
        result = super().write(vals)
        if changed:
            self.env["square.webhook.queue"].sudo().search([
                ("config_id", "in", changed.ids), ("state", "in", ("pending", "processing")),
            ]).write({
                "state": "failed", "order_data": "{}", "signature_verified": False,
                "error_message": "Square credentials or configuration changed; waiting for signed delivery.",
            })
        return result

    def _check_square_operator(self):
        self.ensure_one()
        if not self.env.user.has_group("base.group_system"):
            raise AccessError("Only administrators can operate the Square integration.")
        self.check_access("read")
        if not self.active or self.company_id != self.env.company:
            raise AccessError("Switch to the active Square configuration's company.")

    def test_square_connection(self):
        """Test the connection to Square API"""
        self._check_square_operator()

        try:
            square_api = self.env["square.api.client"]
            result = square_api._test_connection()

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
        self._check_square_operator()

        locations = self.env["square.api.client"]._get_locations()
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

        # First, try exact name match
        exact_match = self.env["stock.warehouse"].search(
            company_domain + [("name", "ilike", location_name)], limit=1
        )

        if exact_match:
            return exact_match

        # Try partial name match
        partial_matches = self.env["stock.warehouse"].search(
            company_domain + [("name", "ilike", f"%{location_name}%")]
        )

        if partial_matches:
            return partial_matches[0]

        # Try location name in warehouse name
        location_words = location_name.lower().split()
        for word in location_words:
            if len(word) > 3:  # Only check meaningful words
                word_match = self.env["stock.warehouse"].search(
                    company_domain + [("name", "ilike", f"%{word}%")], limit=1
                )
                if word_match:
                    return word_match

        # Fallback to first available warehouse
        return self._default_warehouse()

    def action_open_manual_resync_wizard(self):
        """Open the manual resync wizard for missing orders"""
        self._check_square_operator()

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
            "target": "new",
        }

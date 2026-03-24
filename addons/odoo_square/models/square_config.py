# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from odoo import models, fields, api
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)


class SquareConfig(models.Model):
    _name = "square.config"
    _description = "Square Configuration"
    _rec_name = "name"

    name = fields.Char(string="Name", default="Square Configuration", required=True)

    # Square API Configuration
    square_application_id = fields.Char(
        string="Square Application ID",
        help="Your Square Application ID from the Square Developer Dashboard",
        required=True,
    )

    square_access_token = fields.Char(
        string="Square Access Token",
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
        help="Webhook signature key for secure webhook verification",
    )

    # Location to Warehouse Mapping
    location_mapping_ids = fields.One2many(
        "square.location.mapping",
        "config_id",
        string="Square Location Mappings",
        help="Map each Square location to an Odoo warehouse",
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

    def get_warehouse_for_location(self, square_location_id):
        """Get the configured warehouse for a specific Square location"""
        self.ensure_one()

        # Find mapping for this location
        mapping = self.location_mapping_ids.filtered(
            lambda m: m.square_location_id == square_location_id
        )

        if mapping:
            _logger.info(
                f"Using mapped warehouse '{mapping.warehouse_id.name}' for Square location '{square_location_id}'"
            )
            return mapping.warehouse_id

        # Fallback: try to find any mapping (useful for single-location setups)
        if self.location_mapping_ids:
            first_mapping = self.location_mapping_ids[0]
            _logger.warning(
                f"No specific mapping found for Square location '{square_location_id}', "
                f"using first available warehouse: {first_mapping.warehouse_id.name}"
            )
            return first_mapping.warehouse_id

        # Last resort: use first available warehouse
        warehouse = self.env["stock.warehouse"].search([], limit=1)
        if warehouse:
            _logger.warning(
                f"No warehouse mappings configured for Square, using default warehouse: {warehouse.name}"
            )
            return warehouse
        return None

    def get_configured_warehouse(self):
        """Get the configured warehouse for Square operations (legacy method)"""
        self.ensure_one()

        # For backward compatibility, return first mapped warehouse or default
        if self.location_mapping_ids:
            first_mapping = self.location_mapping_ids[0]
            _logger.info(
                f"Using first mapped warehouse: {first_mapping.warehouse_id.name}"
            )
            return first_mapping.warehouse_id

        # Fallback to first warehouse if none configured
        warehouse = self.env["stock.warehouse"].search([], limit=1)
        if warehouse:
            _logger.warning(
                f"No warehouse mappings configured for Square, using default warehouse: {warehouse.name}"
            )
            return warehouse
        return None

    def get_payment_journal(self):
        """Get the configured payment journal for Square payments"""
        self.ensure_one()
        if self.payment_journal_id:
            _logger.info(
                f"Using configured payment journal: {self.payment_journal_id.name}"
            )
            return self.payment_journal_id
        else:
            # Fallback to first available bank journal if none configured
            bank_journal = self.env["account.journal"].search(
                [("type", "=", "bank")], limit=1
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
        if self.search_count([]) > 0:
            raise ValidationError(
                "Only one Square configuration is allowed. Please modify the existing configuration."
            )
        return super().create(vals_list)

    def test_square_connection(self):
        """Test the connection to Square API"""
        self.ensure_one()

        try:
            # Use the Square API client to test connection
            square_api = self.env["square.api.client"]
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

        try:
            # Get Square API client
            square_api = self.env["square.api.client"]
            locations = square_api.get_locations()

            if not locations:
                _logger.warning("No Square locations found to sync")
                return False

            # Get existing mappings
            existing_location_ids = set(
                self.location_mapping_ids.mapped("square_location_id")
            )

            # Create mappings for new locations
            new_mappings = []
            for location in locations:
                location_id = location.get("id")
                location_name = location.get("name", "Unknown Location")

                if location_id not in existing_location_ids:
                    # Try to find a matching warehouse by name similarity or use first available
                    suggested_warehouse = self._suggest_warehouse_for_location(
                        location_name
                    )

                    new_mappings.append(
                        {
                            "square_location_id": location_id,
                            "square_location_name": location_name,
                            "warehouse_id": (
                                suggested_warehouse.id if suggested_warehouse else False
                            ),
                        }
                    )

            # Create the new mappings
            if new_mappings:
                for mapping_vals in new_mappings:
                    self.env["square.location.mapping"].create(
                        {"config_id": self.id, **mapping_vals}
                    )

                _logger.info(f"Created {len(new_mappings)} new location mappings")

            return True

        except Exception as e:
            _logger.error(f"Error syncing Square locations: {str(e)}")
            return False

    def _suggest_warehouse_for_location(self, location_name):
        """Suggest a warehouse for a Square location based on name similarity"""
        # First, try exact name match
        exact_match = self.env["stock.warehouse"].search(
            [("name", "ilike", location_name)], limit=1
        )

        if exact_match:
            return exact_match

        # Try partial name match
        partial_matches = self.env["stock.warehouse"].search(
            [("name", "ilike", f"%{location_name}%")]
        )

        if partial_matches:
            return partial_matches[0]

        # Try location name in warehouse name
        location_words = location_name.lower().split()
        for word in location_words:
            if len(word) > 3:  # Only check meaningful words
                word_match = self.env["stock.warehouse"].search(
                    [("name", "ilike", f"%{word}%")], limit=1
                )
                if word_match:
                    return word_match

        # Fallback to first available warehouse
        return self.env["stock.warehouse"].search([], limit=1)

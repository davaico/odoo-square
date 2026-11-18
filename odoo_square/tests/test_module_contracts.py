# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json
from unittest.mock import patch

from psycopg2.errors import UniqueViolation

from odoo.exceptions import AccessError, ValidationError
from odoo.modules.module import Manifest
from odoo.tests import Form, TransactionCase
from odoo.tests.common import tagged

from odoo.addons.odoo_square.models.square_order_processor import SquareOrderCompletionDeferred

@tagged("post_install", "-at_install", "TestSquareModuleContracts")
class TestSquareModuleContracts(TransactionCase):
    """Low-level contracts that keep the addon installable and operable."""

    def test_manifest_metadata_matches_odoo_addon_conventions(self):
        manifest = Manifest.for_addon("odoo_square")

        self.assertEqual(manifest["license"], "AGPL-3")
        self.assertEqual(manifest["version"], "19.0.1.4.2")
        self.assertTrue(manifest["installable"])
        self.assertTrue(manifest["application"])
        self.assertIn("requests", manifest["external_dependencies"]["python"])
        self.assertIn("sale_stock", manifest["depends"])

        data_files = manifest["data"]
        self.assertEqual(data_files[:2], ["security/square_security.xml", "security/ir.model.access.csv"])
        self.assertIn("views/square_config_views.xml", data_files)
        self.assertIn("views/square_webhook_queue_views.xml", data_files)

    @patch(
        "odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_locations",
        return_value=[],
    )
    def test_save_square_configuration_form(self, _get_locations):
        warehouse = self.env["stock.warehouse"].search(
            [("company_id", "=", self.env.company.id)], limit=1
        )
        second_warehouse = self.env["stock.warehouse"].create(
            {"name": "Square Warehouse", "code": "SQ19"}
        )
        journal = self.env["account.journal"].create(
            {"name": "Square Payments", "code": "SQPAY", "type": "bank"}
        )
        teams = self.env["crm.team"].create(
            [{"name": "Square Default"}, {"name": "Square Location"}]
        )
        config_form = Form(
            self.env["square.config"].with_user(self.env.ref("base.user_admin")),
            view="odoo_square.view_square_config_form",
        )
        config_form.square_environment = "sandbox"
        config_form.square_application_id = "square_app_test"
        config_form.square_access_token = "square_token_test"
        config_form.square_webhook_signature_key = "square_key_test"
        config_form.payment_journal_id = journal
        config_form.default_sales_team_id = teams[0]
        with config_form.location_mapping_ids.new() as mapping_form:
            mapping_form.square_location_id = "LOCATION_MAIN"
            mapping_form.square_location_name = "Main Square"
            mapping_form.warehouse_id = warehouse
        with config_form.location_mapping_ids.new() as mapping_form:
            mapping_form.square_location_id = "LOCATION_SECOND"
            mapping_form.square_location_name = "Second Square"
            mapping_form.warehouse_id = second_warehouse
            mapping_form.sales_team_id = teams[1]
        config = config_form.save()

        self.assertEqual(config.square_environment, "sandbox")
        self.assertEqual(config.square_application_id, "square_app_test")
        self.assertEqual(config.square_access_token, "square_token_test")
        self.assertEqual(config.square_webhook_signature_key, "square_key_test")
        self.assertEqual(config.module_version_display, "1.4.2")
        self.assertEqual(config.get_payment_journal().id, journal.id)
        self.assertEqual(
            config.get_warehouse_for_location("LOCATION_SECOND").id, second_warehouse.id
        )
        self.assertEqual(
            config.get_sales_team_for_location("LOCATION_SECOND").id, teams[1].id
        )
        self.assertEqual(
            config.get_sales_team_for_location("LOCATION_MAIN").id, teams[0].id
        )
        self.assertIn(
            "Second Square → Square Warehouse", config.location_mapping_ids.mapped("display_name")
        )

        bot_config = config.with_user(self.env.ref("odoo_square.user_square_bot"))
        self.assertEqual(bot_config.get_payment_journal().id, journal.id)
        with self.assertRaises(AccessError):
            bot_config.write({"square_access_token": "unauthorized_change"})

    def test_sync_locations_keeps_archived_mappings_and_matches_own_warehouse(self):
        config = self.env["square.config"].create({
            "square_application_id": "square-app",
            "square_access_token": "square-token",
        })
        warehouse = self.env["stock.warehouse"].search([("company_id", "=", config.company_id.id)], limit=1)
        archived = self.env["square.location.mapping"].create({
            "config_id": config.id,
            "square_location_id": "OLD",
            "square_location_name": "Old Store",
            "warehouse_id": warehouse.id,
            "active": False,
        })
        foreign = self.env["res.company"].create({"name": "Other Square Company"})
        foreign_warehouse = self.env["stock.warehouse"].search([("company_id", "=", foreign.id)], limit=1)
        foreign_warehouse.name = "Lakeshore"
        warehouse.name = "Downtown"
        config = config.with_context(allowed_company_ids=[config.company_id.id, foreign.id])
        locations = [{"id": "OLD", "name": "Old Store"}, {"id": "NEW", "name": "Lakeshore Downtown"}]
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_locations", return_value=locations,
        ):
            self.assertTrue(config.sync_square_locations())

        self.assertFalse(archived.active)
        self.assertEqual(config.location_mapping_ids.square_location_id, "NEW")
        self.assertEqual(config.location_mapping_ids.warehouse_id, warehouse)

        self.env["stock.warehouse"].create({"name": "Uptown Store", "code": "SQUP"})
        conflicting = [{"id": "UPTOWN", "name": "Uptown Store"}, {"id": "DUPLICATE", "name": warehouse.name}]
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient.get_locations", return_value=conflicting,
        ), self.assertRaises(ValidationError):
            config.sync_square_locations()
        self.assertEqual(config.location_mapping_ids.square_location_id, "NEW")

        # Existing databases may contain a mapping saved before the company constraint.
        mapping = config.location_mapping_ids
        mapping.flush_recordset(["warehouse_id"])
        self.env.cr.execute(
            "UPDATE square_location_mapping SET warehouse_id = %s WHERE id = %s", (foreign_warehouse.id, mapping.id),
        )
        mapping.invalidate_recordset(["warehouse_id"])
        bot = self.env.ref("odoo_square.user_square_bot")
        with self.assertRaisesRegex(SquareOrderCompletionDeferred, "another company"):
            self.env["sale.order"].with_user(bot)._get_warehouse_for_square_location("NEW")

    def test_square_location_cannot_be_mapped_to_two_warehouses(self):
        config = self.env["square.config"].create(
            {
                "square_application_id": "square_app_test",
                "square_access_token": "square_token_test",
            }
        )
        warehouses = self.env["stock.warehouse"].create(
            [
                {"name": "First Warehouse", "code": "SQ1"},
                {"name": "Second Warehouse", "code": "SQ2"},
            ]
        )
        mapping = {
            "config_id": config.id,
            "square_location_id": "LOCATION_MAIN",
            "square_location_name": "Main Square",
        }
        self.env["square.location.mapping"].create(
            dict(mapping, warehouse_id=warehouses[0].id)
        )
        with self.assertRaises(UniqueViolation):
            self.env["square.location.mapping"].create(
                dict(mapping, warehouse_id=warehouses[1].id)
            )

    def test_config_version_formatter_keeps_odoo_and_semver_forms_clear(self):
        formatter = self.env["square.config"]._odoo_version_to_semver

        self.assertEqual(formatter("19.0.1.4.0"), "1.4.0")
        self.assertEqual(formatter("18.0.2.0.4"), "2.0.4")
        self.assertEqual(formatter("1.3.0"), "1.3.0")
        self.assertEqual(formatter("dev"), "dev")

    def test_integration_log_display_name_includes_event_title_and_order(self):
        log = self.env["square.integration.log"].create(
            {
                "event_type": "order_created",
                "status": "success",
                "square_order_id": "SQ-ORDER-001",
                "title": "Order imported",
            }
        )

        self.assertEqual(
            log.display_name,
            "[Order Created] Order imported (SQ-ORDER-001)",
        )

    def test_webhook_queue_generates_synthetic_event_id_when_missing(self):
        queue = self.env["square.webhook.queue"]._queue_event(
            webhook_event_id=None,
            event_type="order.updated",
            order_data={
                "id": "SQ-ORDER-002",
                "state": "OPEN",
                "version": 5,
            },
            square_order_id="SQ-ORDER-002",
        )

        self.assertTrue(
            queue.webhook_event_id.startswith(
                "synthetic-order.updated-SQ-ORDER-002-"
            )
        )
        self.assertEqual(queue.event_type, "order.updated")
        self.assertEqual(queue.square_order_id, "SQ-ORDER-002")
        self.assertEqual(queue.order_version, 5)
        self.assertTrue(queue.depends_on_order)
        self.assertEqual(json.loads(queue.order_data)["state"], "OPEN")

    def test_webhook_queue_reuses_existing_event_id(self):
        queue_model = self.env["square.webhook.queue"]
        first = queue_model._queue_event(
            webhook_event_id="evt_duplicate_contract",
            event_type="order.updated",
            order_data={"id": "SQ-ORDER-003", "version": 1},
            square_order_id="SQ-ORDER-003",
        )
        first.write({"state": "failed"})

        second = queue_model._queue_event(
            webhook_event_id="evt_duplicate_contract",
            event_type="order.updated",
            order_data={"id": "SQ-ORDER-003", "version": 2},
            square_order_id="SQ-ORDER-003",
        )

        self.assertEqual(first.id, second.id)
        self.assertEqual(second.state, "pending")

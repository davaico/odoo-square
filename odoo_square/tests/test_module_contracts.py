# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json
from unittest.mock import patch

from odoo import release
from odoo.exceptions import AccessError
from odoo.modules.module import get_manifest
from odoo.tests import Form, TransactionCase
from odoo.tests.common import new_test_user, tagged

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient


@tagged("post_install", "-at_install", "TestSquareModuleContracts")
class TestSquareModuleContracts(TransactionCase):
    """Low-level contracts that keep the addon installable and operable."""

    def test_manifest_metadata_matches_odoo_addon_conventions(self):
        manifest = get_manifest("odoo_square")

        self.assertEqual(manifest["license"], "AGPL-3")
        self.assertEqual(release.series, "18.0")
        self.assertEqual(manifest["version"], "18.0.1.4.0")
        self.assertTrue(manifest["installable"])
        self.assertTrue(manifest["application"])
        self.assertIn("sale_stock", manifest["depends"])
        self.assertIn("requests", manifest["external_dependencies"]["python"])

        data_files = manifest["data"]
        self.assertEqual(data_files[0], "security/ir.model.access.csv")
        self.assertIn("views/square_config_views.xml", data_files)
        self.assertIn("views/square_webhook_queue_views.xml", data_files)

    @patch.object(
        SquareApiClient,
        "get_locations",
        return_value=[{"id": "DOWNTOWN", "name": "Downtown"}],
    )
    def test_admin_configures_square_from_form(self, _get_locations):
        admin = self.env.ref("base.user_admin")
        config_model = self.env["square.config"].with_user(admin)
        journal = self.env["account.journal"].create(
            {"name": "Square Payments", "type": "bank", "code": "SQCFG"}
        )
        warehouse = self.env["stock.warehouse"].search(
            [("company_id", "=", self.env.company.id)], limit=1
        )
        default_team, location_team = self.env["crm.team"].create(
            [{"name": "Square Sales"}, {"name": "Downtown Sales"}]
        )
        config_view = "odoo_square.view_square_config_form"

        with Form(config_model, view=config_view) as config_form:
            config_form.square_environment = "production"
            config_form.square_application_id = "square-app"
            config_form.square_access_token = "square-token"
            config_form.square_webhook_signature_key = "square-signature"
            config_form.payment_journal_id = journal
            config_form.default_sales_team_id = default_team
            with config_form.location_mapping_ids.new() as mapping_form:
                mapping_form.square_location_id = "DOWNTOWN"
                mapping_form.warehouse_id = warehouse
                mapping_form.sales_team_id = location_team

        config = config_form.record
        saved = Form(config, view=config_view)
        self.assertEqual(saved.square_environment, "production")
        self.assertEqual(saved.square_application_id, "square-app")
        self.assertEqual(saved.square_access_token, "square-token")
        self.assertEqual(saved.square_webhook_signature_key, "square-signature")
        self.assertEqual(saved.payment_journal_id, journal)
        self.assertEqual(saved.default_sales_team_id, default_team)
        self.assertEqual(saved.module_version_display, "1.4.0")
        self.assertEqual(len(saved.location_mapping_ids), 1)
        mapping = config.location_mapping_ids
        self.assertEqual(mapping.square_location_id, "DOWNTOWN")
        self.assertEqual(mapping.square_location_name, "Downtown")
        self.assertEqual(mapping.warehouse_id, warehouse)
        self.assertEqual(mapping.display_name, f"Downtown → {warehouse.name}")
        self.assertEqual(mapping.sales_team_id, location_team)
        self.assertEqual(config.get_warehouse_for_location("DOWNTOWN"), warehouse)
        self.assertEqual(config.get_sales_team_for_location("DOWNTOWN"), location_team)

    def test_salesperson_can_read_but_cannot_edit_square_configuration(self):
        config = self.env["square.config"].create(
            {
                "square_application_id": "square-app",
                "square_access_token": "square-token",
            }
        )
        salesperson = new_test_user(
            self.env, login="square-salesperson", groups="sales_team.group_sale_salesman"
        )
        own_config = config.with_user(salesperson)
        self.assertEqual(own_config.square_application_id, "square-app")
        with self.assertRaises(AccessError):
            own_config.square_application_id = "changed-app"

    def test_config_version_formatter_keeps_odoo_and_semver_forms_clear(self):
        formatter = self.env["square.config"]._odoo_version_to_semver

        self.assertEqual(formatter("17.0.1.4.0"), "1.4.0")
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
        queue = self.env["square.webhook.queue"].queue_event(
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
        first = queue_model.queue_event(
            webhook_event_id="evt_duplicate_contract",
            event_type="order.updated",
            order_data={"id": "SQ-ORDER-003", "version": 1},
            square_order_id="SQ-ORDER-003",
        )
        first.write({"state": "failed"})

        second = queue_model.queue_event(
            webhook_event_id="evt_duplicate_contract",
            event_type="order.updated",
            order_data={"id": "SQ-ORDER-003", "version": 2},
            square_order_id="SQ-ORDER-003",
        )

        self.assertEqual(first.id, second.id)
        self.assertEqual(second.state, "pending")

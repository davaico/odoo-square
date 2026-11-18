# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json
from unittest.mock import patch

from odoo import Command, release
from odoo.exceptions import AccessError, ValidationError
from odoo.modules.module import get_manifest
from odoo.tests import Form, TransactionCase
from odoo.tests.common import new_test_user, tagged

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient

from odoo.addons.odoo_square.models.square_order_processor import SquareOrderProcessingDeferred

@tagged("post_install", "-at_install", "TestSquareModuleContracts")
class TestSquareModuleContracts(TransactionCase):
    """Low-level contracts that keep the addon installable and operable."""

    def test_manifest_metadata_matches_odoo_addon_conventions(self):
        manifest = get_manifest("odoo_square")

        self.assertEqual(manifest["license"], "AGPL-3")
        self.assertEqual(release.series, "18.0")
        self.assertEqual(manifest["version"], "18.0.1.4.2")
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
        "_get_locations",
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
        self.assertEqual(saved.module_version_display, "1.4.2")
        self.assertEqual(len(saved.location_mapping_ids), 1)
        mapping = config.location_mapping_ids
        self.assertEqual(mapping.square_location_id, "DOWNTOWN")
        self.assertEqual(mapping.square_location_name, "Downtown")
        self.assertEqual(mapping.warehouse_id, warehouse)
        self.assertEqual(mapping.display_name, f"Downtown → {warehouse.name}")
        self.assertEqual(mapping.sales_team_id, location_team)
        self.assertEqual(config._get_warehouse_for_location("DOWNTOWN"), warehouse)
        self.assertEqual(config._get_sales_team_for_location("DOWNTOWN"), location_team)

    def test_sync_locations_keeps_archived_mappings(self):
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
        locations = [{"id": "OLD", "name": "Old Store"}, {"id": "NEW", "name": "New Store"}]
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_locations", return_value=locations,
        ):
            self.assertTrue(config.sync_square_locations())

        self.assertFalse(archived.active)
        self.assertEqual(config.location_mapping_ids.square_location_id, "NEW")
        self.assertEqual(config.location_mapping_ids.warehouse_id, warehouse)

        self.env["stock.warehouse"].create({"name": "Uptown Store", "code": "SQUP"})
        conflicting = [{"id": "UPTOWN", "name": "Uptown Store"}, {"id": "DUPLICATE", "name": warehouse.name}]
        with patch(
            "odoo.addons.odoo_square.models.square_api_client.SquareApiClient._get_locations", return_value=conflicting,
        ), self.assertRaises(ValidationError):
            config.sync_square_locations()
        self.assertEqual(config.location_mapping_ids.square_location_id, "NEW")

        foreign = self.env["res.company"].create({"name": "Other Square Company"})
        foreign_warehouse = self.env["stock.warehouse"].search([("company_id", "=", foreign.id)], limit=1)
        # Existing databases may contain a mapping saved before the company constraint.
        mapping = config.location_mapping_ids
        mapping.flush_recordset(["warehouse_id"])
        self.env.cr.execute(
            "UPDATE square_location_mapping SET warehouse_id = %s WHERE id = %s", (foreign_warehouse.id, mapping.id),
        )
        mapping.invalidate_recordset(["warehouse_id"])
        bot = self.env.ref("odoo_square.user_square_bot")
        with self.assertRaisesRegex(SquareOrderProcessingDeferred, "another company"):
            self.env["sale.order"].with_user(bot)._get_warehouse_for_square_location("NEW")

    def test_salesperson_can_read_but_cannot_edit_square_configuration(self):
        config = self.env["square.config"].create(
            {
                "square_application_id": "square-app",
                "square_access_token": "square-token",
                "square_webhook_signature_key": "fixture-signing-key",
            }
        )
        salesperson = new_test_user(
            self.env, login="square-salesperson", groups="sales_team.group_sale_salesman"
        )
        own_config = config.with_user(salesperson)
        self.assertEqual(own_config.square_application_id, "square-app")
        with self.assertRaises(AccessError):
            own_config.square_application_id = "changed-app"
        secret_fields = ["square_access_token", "square_webhook_signature_key"]
        for field in secret_fields:
            self.assertNotIn(field, own_config.fields_get())
            with self.assertRaises(AccessError):
                own_config.read([field])
        self.assertEqual(
            config.with_user(self.env.ref("base.user_admin")).read(secret_fields)[0]["square_access_token"],
            "square-token",
        )

        bot = self.env.ref("odoo_square.user_square_bot")
        with patch("odoo.addons.odoo_square.models.square_api_client.requests.get") as get:
            get.return_value.json.return_value = {"locations": []}
            self.env["square.api.client"].with_user(bot)._get_locations()
            self.assertEqual(get.call_args.kwargs["headers"]["Authorization"], "Bearer square-token")

    def test_service_records_respect_operator_and_company(self):
        config = self.env["square.config"].create({
            "square_application_id": "square-app", "square_access_token": "square-token",
        })
        employee = new_test_user(self.env, login="square-employee", groups="base.group_user")
        sales = new_test_user(self.env, login="square-sales", groups="sales_team.group_sale_salesman")
        other = self.env["res.company"].create({"name": "Other company"})
        outsider = new_test_user(
            self.env, login="square-other-admin", groups="base.group_system",
            company_id=other.id, company_ids=[Command.set(other.ids)],
        )
        admin = self.env.ref("base.user_admin")
        order = self.env["sale.order"].create({"partner_id": sales.partner_id.id, "user_id": sales.id})
        order.with_user(sales).write({"client_order_ref": "ordinary sale edit"})
        with self.assertRaises(AccessError):
            order.with_user(sales).write({"square_payment_id": "false-link"})
        with self.assertRaises(AccessError):
            self.env["sale.order"].with_user(sales).with_context(
                default_square_order_id="forged",
            ).create({"partner_id": sales.partner_id.id})

        refund = self.env["square.refund"].create({
            "square_refund_id": "fixture-refund",
            "square_order_id": "fixture-order",
            "sale_order_id": order.id,
            "currency_id": self.env.ref("base.EUR").id,
            "status": "completed",
            "refund_amount": 10,
        })
        queue = self.env["square.webhook.queue"]._queue_event(
            "fixture-event", "order.updated", {"id": "fixture-order"}, "fixture-order", config,
        )
        log = self.env["square.integration.log"]._log_square_event(
            event_type="order_updated", title="Status only", technical_details="private fixture",
        )
        self.assertFalse(log.technical_details)
        for record in (refund, queue, log):
            for denied in (employee, sales, outsider):
                with self.subTest(model=record._name, user=denied.login), self.assertRaises(AccessError):
                    record.with_user(denied).read(["id"])
            self.assertEqual(record.with_user(admin).read(["id"])[0]["id"], record.id)

        self.assertEqual(order.with_user(sales).total_refunded_amount, 10)
        with self.assertRaises(AccessError):
            refund.with_user(sales).write({"processed_at": "2026-01-01 00:00:00"})
        with self.assertRaises(AccessError):
            queue.with_user(sales).write({"signature_verified": True, "order_data": "{}"})
        with self.assertRaises(AccessError):
            log.with_user(sales).write({"status": "success"})
        with self.assertRaises(AccessError):
            queue.with_user(sales).process_pending_events()

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

# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json

from odoo.modules.module import load_information_from_description_file
from odoo.tests import TransactionCase
from odoo.tests.common import tagged


@tagged("post_install", "-at_install", "TestSquareModuleContracts")
class TestSquareModuleContracts(TransactionCase):
    """Low-level contracts that keep the addon installable and operable."""

    def test_manifest_metadata_matches_odoo_addon_conventions(self):
        manifest = load_information_from_description_file("odoo_square")

        self.assertEqual(manifest["license"], "AGPL-3")
        self.assertEqual(manifest["version"], "17.0.1.3.0")
        self.assertTrue(manifest["installable"])
        self.assertTrue(manifest["application"])
        self.assertIn("requests", manifest["external_dependencies"]["python"])

        data_files = manifest["data"]
        self.assertEqual(data_files[0], "security/ir.model.access.csv")
        self.assertIn("views/square_config_views.xml", data_files)
        self.assertIn("views/square_webhook_queue_views.xml", data_files)

    def test_config_version_formatter_keeps_odoo_and_semver_forms_clear(self):
        formatter = self.env["square.config"]._odoo_version_to_semver

        self.assertEqual(formatter("17.0.1.3.0"), "1.3.0")
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

# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from unittest.mock import patch

from odoo import Command, fields
from odoo.tests import Form, TransactionCase, tagged
from odoo.tests.common import new_test_user

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient


@tagged("post_install", "-at_install")
class TestStockSync(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.warehouse = cls.env["stock.warehouse"].create(
            {"name": "Square Stock Warehouse", "code": "SQST"}
        )
        cls.other_mapped_warehouse = cls.env["stock.warehouse"].create(
            {"name": "Other Square Warehouse", "code": "SQOT"}
        )
        cls.unmapped_warehouse = cls.env["stock.warehouse"].create(
            {"name": "Unmapped Warehouse", "code": "UNMP"}
        )
        cls.product = cls.env["product.product"].create(
            {
                "name": "Square Stock Product",
                "default_code": "SYNC-SKU",
                "is_storable": True,
            }
        )
        cls.env["square.config"].create(
            {
                "square_application_id": "test-app",
                "square_access_token": "test-token",
                "square_environment": "sandbox",
                "location_mapping_ids": [
                    Command.create(
                        {
                            "square_location_id": "SQ-STOCK-LOCATION",
                            "square_location_name": "Square Stock Store",
                            "warehouse_id": cls.warehouse.id,
                        }
                    ),
                    Command.create(
                        {
                            "square_location_id": "OTHER-STOCK-LOCATION",
                            "square_location_name": "Other Square Store",
                            "warehouse_id": cls.other_mapped_warehouse.id,
                        }
                    ),
                ],
            }
        )

    def _square_response(self, endpoint, method="GET", data=None):
        if endpoint == "/v2/locations":
            return {"locations": [
                {"id": "SQ-STOCK-LOCATION", "status": "ACTIVE"},
                {"id": "OTHER-STOCK-LOCATION", "status": "ACTIVE"},
            ]}
        if endpoint == "/v2/catalog/search" and method == "POST":
            return {"objects": [{"id": "SYNC-VARIATION"}]}
        if endpoint == "/v2/inventory/changes/batch-create" and method == "POST":
            return {"counts": []}
        self.fail(f"Unexpected Square request: {method} {endpoint}")

    def _assert_physical_counts(self, square_requests, expected_location_quantities):
        searches = []
        counts = []
        for request in square_requests.call_args_list:
            endpoint = request.args[0]
            if endpoint == "/v2/locations":
                continue
            self.assertEqual(request.kwargs["method"], "POST")
            data = request.kwargs["data"]
            if endpoint == "/v2/catalog/search":
                searches.append(data["query"]["exact_query"])
            else:
                self.assertEqual(endpoint, "/v2/inventory/changes/batch-create")
                self.assertFalse(data["ignore_unchanged_counts"])
                for change in data["changes"]:
                    self.assertEqual(change["type"], "PHYSICAL_COUNT")
                    count = change["physical_count"]
                    self.assertEqual(count["catalog_object_id"], "SYNC-VARIATION")
                    self.assertEqual(count["state"], "IN_STOCK")
                    counts.append((count["location_id"], count["quantity"]))

        self.assertTrue(searches, "Square must be queried for the product's SKU")
        for search in searches:
            self.assertEqual(search, {"attribute_name": "sku", "attribute_value": "SYNC-SKU"})
        self.assertCountEqual(counts, expected_location_quantities)
        keys = [call.kwargs["data"]["idempotency_key"] for call in square_requests.call_args_list
                if call.args[0] == "/v2/inventory/changes/batch-create"]
        self.assertEqual(len(set(keys)), len(counts))

    def _create_receipt(self, warehouse):
        picking_type = warehouse.in_type_id
        return self.env["stock.picking"].create(
            {
                "picking_type_id": picking_type.id,
                "location_id": picking_type.default_location_src_id.id,
                "location_dest_id": warehouse.lot_stock_id.id,
                "move_ids": [
                    Command.create(
                        {
                            "product_id": self.product.id,
                            "product_uom_qty": 4,
                            "location_id": picking_type.default_location_src_id.id,
                            "location_dest_id": warehouse.lot_stock_id.id,
                        }
                    )
                ],
            }
        )

    def _create_quant(self, warehouse, quantity):
        return self.env["stock.quant"].create(
            {
                "product_id": self.product.id,
                "location_id": warehouse.lot_stock_id.id,
                "quantity": quantity,
            }
        )

    def _send_pending_counts(self):
        action = self.env.ref("odoo_square.ir_cron_square_stock_counts").ir_actions_server_id
        action.run()  # Resolve the variation.
        action.run()  # Dispatch the persisted count.

    def test_validated_receipt_updates_mapped_square_inventory(self):
        receipt = self._create_receipt(self.warehouse)
        stock_user = new_test_user(self.env, login="square_stock_operator", groups="stock.group_stock_user")

        with patch.object(
            SquareApiClient, "_make_api_request", side_effect=self._square_response
        ) as square_requests:
            receipt.with_user(stock_user).button_validate()
            square_requests.assert_not_called()
            self._send_pending_counts()

        self.assertEqual(receipt.state, "done")
        self.assertEqual(
            self.product.with_context(warehouse_id=self.warehouse.id).qty_available, 4
        )
        self._assert_physical_counts(square_requests, {("SQ-STOCK-LOCATION", "4")})

        admin = self.env.ref("base.user_admin")
        sync = self.env["square.stock.sync"].with_user(admin).search([], limit=1)
        self.assertTrue(sync)
        sync_form = Form(sync, view="odoo_square.view_square_stock_sync_form")
        self.assertTrue(sync_form.auto_sync_enabled)
        self.assertGreater(sync_form.successful_syncs, 0)
        logs = self.env["square.integration.log"].with_user(admin).search(
            [("event_type", "=", "stock_sync"), ("status", "=", "success")]
        )
        self.assertTrue(logs.filtered(lambda log: "SYNC-SKU" in log.description))

    def test_inventory_adjustment_updates_only_its_warehouse_with_the_exact_count(self):
        quant = self._create_quant(self.other_mapped_warehouse, 5).with_context(
            inventory_mode=True
        )
        main_quant = self._create_quant(self.warehouse, 12)

        for quantity in (8, 0):
            with self.subTest(quantity=quantity), patch.object(
                SquareApiClient, "_make_api_request", side_effect=self._square_response
            ) as square_requests:
                quant.inventory_quantity = quantity
                quant.action_apply_inventory()
                self._send_pending_counts()

                self.assertEqual(quant.quantity, quantity)
                self.assertEqual(main_quant.quantity, 12)
                self._assert_physical_counts(
                    square_requests, {("OTHER-STOCK-LOCATION", str(quantity))}
                )

    def test_validated_receipt_in_unmapped_warehouse_does_not_update_square(self):
        quant = self._create_quant(self.unmapped_warehouse, 2)
        receipt = self._create_receipt(self.unmapped_warehouse)

        with patch.object(SquareApiClient, "_make_api_request") as square_requests:
            receipt.button_validate()
            self._send_pending_counts()

        self.assertEqual(receipt.state, "done")
        self.assertEqual(quant.quantity, 6)
        square_requests.assert_not_called()

    def test_transfer_updates_both_locations_once_with_fractional_counts(self):
        self._create_quant(self.warehouse, 7.25)
        self._create_quant(self.other_mapped_warehouse, 3)
        receipt = self.env["stock.picking"].create({
            "picking_type_id": self.warehouse.int_type_id.id,
            "location_id": self.warehouse.lot_stock_id.id,
            "location_dest_id": self.other_mapped_warehouse.lot_stock_id.id,
            "origin": "Square display stand transfer",
            "move_ids": [Command.create({"product_id": self.product.id, "product_uom_qty": 4})],
        })
        receipt.action_confirm()
        receipt.action_assign()
        receipt.move_ids.picked = True
        with patch.object(SquareApiClient, "_make_api_request", side_effect=self._square_response) as requests:
            receipt.button_validate()
            self._send_pending_counts()
        self.assertEqual(receipt.state, "done")
        self._assert_physical_counts(requests, [("SQ-STOCK-LOCATION", "3.25"), ("OTHER-STOCK-LOCATION", "7")])

    def test_ambiguous_square_sku_does_not_update_either_variation(self):
        def duplicate_sku(endpoint, **kwargs):
            if endpoint == "/v2/catalog/search":
                return {"objects": [{"id": "A"}, {"id": "B"}]}
            return self._square_response(endpoint, **kwargs)

        receipt = self._create_receipt(self.warehouse)
        with patch.object(SquareApiClient, "_make_api_request", side_effect=duplicate_sku) as requests:
            receipt.button_validate()
            self._send_pending_counts()
        self.assertFalse(any(call.args[0] == "/v2/inventory/changes/batch-create"
                             for call in requests.call_args_list))
        self.assertEqual(self.env["square.stock.sync"].search([], limit=1).failed_syncs, 1)

    def test_rolled_back_stock_never_reaches_square_and_failed_counts_retry(self):
        quant = self._create_quant(self.warehouse, 8)
        jobs = self.env["square.stock.count"]
        with self.assertRaises(RuntimeError), self.cr.savepoint():
            quant.quantity = 6
            self.assertTrue(jobs.search([]))
            raise RuntimeError("Roll back the warehouse operation")
        self.assertFalse(jobs.search([]))
        with patch.object(SquareApiClient, "_make_api_request", return_value={"locations": []}) as unavailable:
            self._send_pending_counts()
            unavailable.assert_not_called()
            quant.quantity = 2.5
            self._send_pending_counts()
        pending = jobs.search([])
        self.assertEqual(len(pending), 1)
        self.assertGreater(pending.next_attempt, fields.Datetime.now())
        pending.next_attempt = fields.Datetime.now()
        with patch.object(SquareApiClient, "_make_api_request", side_effect=self._square_response) as requests:
            self._send_pending_counts()
            self._assert_physical_counts(requests, [("SQ-STOCK-LOCATION", "2.5")])
        self.assertFalse(jobs.search([]))


    def test_lost_physical_count_response_replays_the_original_observation(self):
        quant = self._create_quant(self.warehouse, 8)
        jobs = self.env["square.stock.count"]
        for quantity in range(21, 46):
            quant.quantity = quantity
        self.assertEqual(jobs.search_count([]), 1)
        attempts = []

        def lose_response(endpoint, **kwargs):
            if endpoint == "/v2/inventory/changes/batch-create":
                attempts.append(kwargs["data"])
                if len(attempts) == 1:
                    raise TimeoutError("Response lost after Square accepted the count")
            return self._square_response(endpoint, **kwargs)

        with patch.object(SquareApiClient, "_make_api_request", side_effect=lose_response):
            self._send_pending_counts()
            job = jobs.search([])
            self.assertTrue(job.catalog_object_id)
            job.next_attempt = fields.Datetime.now()
            self._send_pending_counts()
        self.assertFalse(jobs.search([]))
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0], attempts[1])
        self.assertEqual(attempts[0]["changes"][0]["physical_count"]["quantity"], "45")

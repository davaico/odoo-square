# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from unittest.mock import patch

from odoo import Command
from odoo.tests import Form, TransactionCase, tagged

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
        self.assertEqual(set(counts), expected_location_quantities)

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

    def test_validated_receipt_updates_mapped_square_inventory(self):
        receipt = self._create_receipt(self.warehouse)

        with patch.object(
            SquareApiClient, "_make_api_request", side_effect=self._square_response
        ) as square_requests:
            receipt.button_validate()

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

        self.assertEqual(receipt.state, "done")
        self.assertEqual(quant.quantity, 6)
        square_requests.assert_not_called()

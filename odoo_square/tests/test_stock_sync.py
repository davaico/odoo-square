from unittest.mock import patch

from odoo import Command
from odoo.tests import TransactionCase, tagged

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient


@tagged("post_install", "-at_install")
class TestStockSync(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.warehouse = cls.env["stock.warehouse"].create(
            {"name": "Square Stock Warehouse", "code": "SQST"}
        )
        cls.product = cls.env["product.product"].create(
            {"name": "Square Stock Product", "default_code": "SYNC-SKU", "is_storable": True}
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
                    )
                ],
            }
        )

    def _square_response(self, endpoint, method="GET", data=None):
        if endpoint == "/v2/catalog/search" and method == "POST":
            return {"objects": [{"id": "SYNC-VARIATION"}]}
        if endpoint == "/v2/inventory/changes/batch-create" and method == "POST":
            return {"counts": []}
        self.fail(f"Unexpected Square request: {method} {endpoint}")

    def _assert_physical_count(self, square_requests, expected_quantity):
        searches = []
        counts = []
        for request in square_requests.call_args_list:
            endpoint = request.args[0]
            data = request.kwargs["data"]
            if endpoint == "/v2/catalog/search":
                searches.append(data["query"]["exact_query"])
            else:
                self.assertEqual(endpoint, "/v2/inventory/changes/batch-create")
                self.assertFalse(data["ignore_unchanged_counts"])
                for change in data["changes"]:
                    self.assertEqual(change["type"], "PHYSICAL_COUNT")
                    count = change["physical_count"]
                    counts.append(
                        (count["catalog_object_id"], count["location_id"], count["state"], count["quantity"])
                    )

        self.assertTrue(searches, "Square must be queried for the product's SKU")
        self.assertTrue(counts, "Square must receive a physical count")
        for search in searches:
            self.assertEqual(search, {"attribute_name": "sku", "attribute_value": "SYNC-SKU"})
        self.assertEqual(
            set(counts),
            {("SYNC-VARIATION", "SQ-STOCK-LOCATION", "IN_STOCK", expected_quantity)},
        )

    def test_validated_receipt_updates_mapped_square_inventory(self):
        picking_type = self.warehouse.in_type_id
        receipt = self.env["stock.picking"].create(
            {
                "picking_type_id": picking_type.id,
                "location_id": picking_type.default_location_src_id.id,
                "location_dest_id": self.warehouse.lot_stock_id.id,
                "move_ids": [
                    Command.create(
                        {
                            "product_id": self.product.id,
                            "product_uom_qty": 4,
                            "location_id": picking_type.default_location_src_id.id,
                            "location_dest_id": self.warehouse.lot_stock_id.id,
                        }
                    )
                ],
            }
        )

        with patch.object(SquareApiClient, "_make_api_request", side_effect=self._square_response) as square_requests:
            receipt.button_validate()

        self.assertEqual(receipt.state, "done")
        self.assertEqual(self.product.with_context(warehouse_id=self.warehouse.id).qty_available, 4)
        self._assert_physical_count(square_requests, "4")
        logs = self.env["square.integration.log"].search(
            [("event_type", "=", "stock_sync"), ("status", "=", "success")]
        )
        self.assertTrue(logs.filtered(lambda log: "SYNC-SKU" in log.description))

from unittest.mock import patch

from odoo import Command
from odoo.tests import Form, TransactionCase
from odoo.tests.common import tagged

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient


@tagged("post_install", "-at_install", "TestSquareStockSync")
class TestSquareStockSync(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        warehouse_model = cls.env["stock.warehouse"]
        cls.warehouse = warehouse_model.create({"name": "Downtown", "code": "DWTN"})
        cls.other_warehouse = warehouse_model.create({"name": "Airport", "code": "AIRP"})
        config = cls.env["square.config"].create(
            {
                "square_application_id": "test-app",
                "square_access_token": "test-token",
                "square_environment": "sandbox",
            }
        )
        for warehouse, square_location_id in (
            (cls.warehouse, "SQUARE-DOWNTOWN"),
            (cls.other_warehouse, "SQUARE-AIRPORT"),
        ):
            cls.env["square.location.mapping"].create(
                {
                    "config_id": config.id,
                    "square_location_id": square_location_id,
                    "square_location_name": warehouse.name,
                    "warehouse_id": warehouse.id,
                }
            )
        cls.stock_sync = cls.env["square.stock.sync"].create({})
        cls.product = cls.env["product.product"].create(
            {
                "name": "Stock Sync Item",
                "default_code": "SKU-STOCK",
                "type": "consu",
                "is_storable": True,
            }
        )
        cls.quant, cls.other_quant = cls.env["stock.quant"].create(
            [
                {
                    "product_id": cls.product.id,
                    "location_id": cls.warehouse.lot_stock_id.id,
                    "quantity": 8,
                },
                {
                    "product_id": cls.product.id,
                    "location_id": cls.other_warehouse.lot_stock_id.id,
                    "quantity": 15,
                },
            ]
        )

    def setUp(self):
        super().setUp()
        self.square_request = self.enterContext(
            patch.object(SquareApiClient, "_make_api_request", side_effect=self._square_response)
        )

    @staticmethod
    def _square_response(endpoint, method, data):
        responses = {
            "/v2/catalog/search": {"objects": [{"id": "SQUARE-VARIATION"}]},
            "/v2/inventory/changes/batch-create": {"counts": []},
        }
        return responses[endpoint]

    def assert_square_counts(self, expected):
        searches = []
        counts = []
        idempotency_keys = set()
        for request in self.square_request.call_args_list:
            endpoint = request.args[0]
            self.assertEqual(request.kwargs["method"], "POST")
            data = request.kwargs["data"]
            if endpoint == "/v2/catalog/search":
                searches.append(data)
                continue

            self.assertEqual(endpoint, "/v2/inventory/changes/batch-create")
            self.assertIs(data["ignore_unchanged_counts"], False)
            idempotency_key = data["idempotency_key"]
            self.assertTrue(idempotency_key)
            self.assertNotIn(idempotency_key, idempotency_keys)
            idempotency_keys.add(idempotency_key)
            self.assertEqual(len(data["changes"]), 1)
            change = data["changes"][0]
            self.assertEqual(change["type"], "PHYSICAL_COUNT")
            count = change["physical_count"]
            self.assertEqual(count["catalog_object_id"], "SQUARE-VARIATION")
            self.assertEqual(count["state"], "IN_STOCK")
            counts.append((count["location_id"], count["quantity"]))

        self.assertCountEqual(counts, expected)
        self.assertEqual(
            searches,
            [
                {
                    "object_types": ["ITEM_VARIATION"],
                    "query": {
                        "exact_query": {"attribute_name": "sku", "attribute_value": "SKU-STOCK"}
                    },
                }
            ] * len(expected),
        )

    def _validate_picking(self, picking_type, source, destination, origin=None):
        picking = self.env["stock.picking"].create(
            {
                "picking_type_id": picking_type.id,
                "location_id": source.id,
                "location_dest_id": destination.id,
                "origin": origin,
                "move_ids": [
                    Command.create(
                        {
                            "name": "Stock Sync Move",
                            "origin": origin,
                            "product_id": self.product.id,
                            "product_uom_qty": 3,
                        }
                    ),
                ],
            }
        )
        picking.action_confirm()
        picking.action_assign()
        picking.move_ids.picked = True
        picking.button_validate()

        self.assertEqual(picking.state, "done")

    def test_delivery_synchronizes_remaining_stock_to_mapped_location(self):
        self._validate_picking(
            self.warehouse.out_type_id,
            self.warehouse.lot_stock_id,
            self.env.ref("stock.stock_location_customers"),
        )

        self.assertEqual(self.quant.quantity, 5)
        self.assertEqual(self.other_quant.quantity, 15)
        self.assert_square_counts([("SQUARE-DOWNTOWN", "5")])

        admin = self.env.ref("base.user_admin")
        sync_form = Form(
            self.stock_sync.with_user(admin), view="odoo_square.view_square_stock_sync_form"
        )
        self.assertTrue(sync_form.auto_sync_enabled)
        self.assertEqual(sync_form.successful_syncs, 1)
        self.assertTrue(sync_form.last_sync_date)
        log = self.env["square.integration.log"].with_user(admin).search(
            [
                ("event_type", "=", "stock_sync"),
                ("status", "=", "success"),
                ("title", "=", "Stock synchronized for Stock Sync Item"),
            ]
        )
        self.assertEqual(len(log), 1)
        self.assertIn("SQUARE-DOWNTOWN", log.description)

    def test_transfer_synchronizes_both_mapped_locations(self):
        self._validate_picking(
            self.warehouse.int_type_id,
            self.warehouse.lot_stock_id,
            self.other_warehouse.lot_stock_id,
        )

        self.assertEqual(self.quant.quantity, 5)
        self.assertEqual(self.other_quant.quantity, 18)
        self.assert_square_counts([("SQUARE-DOWNTOWN", "5"), ("SQUARE-AIRPORT", "18")])

    def test_inventory_adjustment_syncs_zero_for_non_positive_warehouse_stock(self):
        quant = self.quant.with_context(inventory_mode=True)
        for quantity in (0, -2):
            with self.subTest(quantity=quantity):
                self.square_request.reset_mock()
                quant.inventory_quantity = quantity
                quant.action_apply_inventory()

                self.assertEqual(quant.quantity, quantity)
                self.assertEqual(self.other_quant.quantity, 15)
                self.assert_square_counts([("SQUARE-DOWNTOWN", "0")])

    def test_manual_bin_count_synchronizes_entire_warehouse_stock(self):
        stock_bin = self.env["stock.location"].create(
            {
                "name": "Bin A",
                "location_id": self.warehouse.lot_stock_id.id,
                "usage": "internal",
            }
        )
        bin_quant = self.env["stock.quant"].create(
            {"product_id": self.product.id, "location_id": stock_bin.id, "quantity": 4}
        ).with_context(inventory_mode=True)

        bin_quant.inventory_quantity = 6
        bin_quant.action_apply_inventory()

        self.assertEqual(bin_quant.quantity, 6)
        self.assertEqual(self.quant.quantity, 8)
        self.assertEqual(self.other_quant.quantity, 15)
        self.assert_square_counts([("SQUARE-DOWNTOWN", "14")])

    def test_square_originated_return_does_not_send_an_inventory_update(self):
        warehouse_stock = self.warehouse.lot_stock_id
        customer_stock = self.env.ref("stock.stock_location_customers")
        self._validate_picking(self.warehouse.out_type_id, warehouse_stock, customer_stock)
        self.assertEqual(self.quant.quantity, 5)
        self.square_request.reset_mock()

        self._validate_picking(
            self.warehouse.in_type_id, customer_stock, warehouse_stock, origin="Square refund"
        )

        self.assertEqual(self.quant.quantity, 8)
        self.assertEqual(self.other_quant.quantity, 15)
        self.assert_square_counts([])

    def test_disabling_automatic_sync_keeps_inventory_adjustments_local(self):
        with Form(self.stock_sync, view="odoo_square.view_square_stock_sync_form") as form:
            form.auto_sync_enabled = False

        quant = self.quant.with_context(inventory_mode=True)
        quant.inventory_quantity = 9
        quant.action_apply_inventory()

        self.assertEqual(quant.quantity, 9)
        self.assert_square_counts([])

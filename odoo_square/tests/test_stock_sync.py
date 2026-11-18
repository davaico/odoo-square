from unittest.mock import patch

from odoo import Command, fields
from odoo.exceptions import AccessError, ValidationError
from odoo.tests import Form, TransactionCase
from odoo.tests.common import new_test_user, tagged

from odoo.addons.odoo_square.models.square_api_client import SquareApiClient
from odoo.addons.odoo_square.models.stock_move import _SQUARE_MANAGED_PICKING


@tagged("post_install", "-at_install", "TestSquareStockSync")
class TestSquareStockSync(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        warehouse_model = cls.env["stock.warehouse"]
        cls.warehouse = warehouse_model.create({"name": "Downtown", "code": "DWTN"})
        cls.other_warehouse = warehouse_model.create({"name": "Airport", "code": "AIRP"})
        cls.config = cls.env["square.config"].create(
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
                    "config_id": cls.config.id,
                    "square_location_id": square_location_id,
                    "square_location_name": warehouse.name,
                    "warehouse_id": warehouse.id,
                }
            )
        cls.stock_sync = cls.env["square.stock.sync"].create({})
        cls.employee = new_test_user(cls.env, login="square-count-employee", groups="base.group_user")
        cls.salesperson = new_test_user(
            cls.env, login="square-count-salesperson", groups="sales_team.group_sale_salesman",
        )
        cls.stock_user = new_test_user(cls.env, login="square-count-stock", groups="stock.group_stock_user")
        cls.admin = cls.env.ref("base.user_admin")
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
    def _square_response(endpoint, method="GET", data=None, square_config=None):
        responses = {
            "/v2/locations": {"locations": [
                {"id": "SQUARE-DOWNTOWN", "status": "ACTIVE"},
                {"id": "SQUARE-AIRPORT", "status": "ACTIVE"},
            ]},
            "/v2/catalog/search": {"objects": [{"id": "SQUARE-VARIATION"}]},
            "/v2/inventory/changes/batch-create": {"counts": []},
        }
        return responses[endpoint]

    def assert_square_counts(self, expected):
        self._send_pending_counts()
        searches = []
        counts = []
        idempotency_keys = set()
        for request in self.square_request.call_args_list:
            endpoint = request.args[0]
            if endpoint == "/v2/locations":
                self.assertEqual(request.kwargs["square_config"], self.config)
                continue
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

    def _validate_picking(self, picking_type, source, destination, origin=None, user=None, **context):
        model = self.env["stock.picking"]
        if user:
            model = model.with_user(user)
        picking = model.with_context(**context).create(
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

    def _send_pending_counts(self):
        action = self.env.ref("odoo_square.ir_cron_square_stock_counts").ir_actions_server_id
        action.run()  # Resolve the variation.
        action.run()  # Dispatch the persisted count.

    def test_delivery_synchronizes_remaining_stock_to_mapped_location(self):
        self._validate_picking(
            self.warehouse.out_type_id,
            self.warehouse.lot_stock_id,
            self.env.ref("stock.stock_location_customers"),
            user=self.stock_user,
            origin="Square typed by an employee",
            square_move_in_progress=True,
            square_managed_picking=True,
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

    def test_fractional_stock_preserves_the_warehouse_count(self):
        self.quant.quantity = 4.25
        self.assert_square_counts([("SQUARE-DOWNTOWN", "4.25")])

    def test_ambiguous_square_sku_does_not_update_either_variation(self):
        response = self._square_response

        def duplicate_sku(endpoint, **kwargs):
            if endpoint == "/v2/catalog/search":
                return {"objects": [{"id": "A"}, {"id": "B"}]}
            return response(endpoint, **kwargs)

        self.square_request.side_effect = duplicate_sku
        self.quant.quantity = 4
        self.square_request.assert_not_called()
        self._send_pending_counts()
        self.assertEqual(
            [call.args[0] for call in self.square_request.call_args_list],
            ["/v2/locations", "/v2/catalog/search"],
        )
        self.assertEqual(self.stock_sync.failed_syncs, 1)

    def test_square_originated_return_does_not_send_an_inventory_update(self):
        warehouse_stock = self.warehouse.lot_stock_id
        customer_stock = self.env.ref("stock.stock_location_customers")
        self._validate_picking(self.warehouse.out_type_id, warehouse_stock, customer_stock)
        self.assertEqual(self.quant.quantity, 5)
        self.assert_square_counts([("SQUARE-DOWNTOWN", "5")])
        self.square_request.reset_mock()

        self._validate_picking(
            self.warehouse.in_type_id, customer_stock, warehouse_stock,
            origin="Square refund", square_managed_picking=_SQUARE_MANAGED_PICKING,
        )

        self.assertEqual(self.quant.quantity, 8)
        self.assertEqual(self.other_quant.quantity, 15)
        self.assert_square_counts([])

    def test_disabling_automatic_sync_keeps_inventory_adjustments_local(self):
        with Form(self.stock_sync.with_user(self.admin), view="odoo_square.view_square_stock_sync_form") as form:
            form.auto_sync_enabled = False

        quant = self.quant.with_context(inventory_mode=True)
        quant.inventory_quantity = 9
        quant.action_apply_inventory()

        self.assertEqual(quant.quantity, 9)
        self.assert_square_counts([])

    def test_stock_sync_controls_require_administrator(self):
        for user in (self.employee, self.salesperson, self.stock_user):
            with self.subTest(user=user.login):
                with self.assertRaises(AccessError):
                    self.stock_sync.with_user(user).write({"auto_sync_enabled": False})
                with self.assertRaises(AccessError):
                    self.stock_sync.with_user(user).manual_sync_all_products()
        self.square_request.assert_not_called()

        sync = self.stock_sync.with_user(self.admin)
        with self.assertRaises(AccessError):
            sync.write({"successful_syncs": 100})
        self.assertTrue(sync.auto_sync_enabled)
        result = sync.manual_sync_all_products()
        self.assertEqual(result["successful"], 2)
        self.assertEqual(sync.successful_syncs, 2)
        self.assert_square_counts([("SQUARE-DOWNTOWN", "8"), ("SQUARE-AIRPORT", "15")])

    def test_admin_can_disable_move_sync_without_changing_direct_quant_sync(self):
        self.stock_sync.with_user(self.admin).write({"auto_sync_enabled": False})
        self._validate_picking(
            self.warehouse.out_type_id,
            self.warehouse.lot_stock_id,
            self.env.ref("stock.stock_location_customers"),
            user=self.stock_user,
        )
        self.assertEqual(self.quant.quantity, 5)
        self.square_request.assert_not_called()

        self.quant.quantity = 4
        self.assert_square_counts([("SQUARE-DOWNTOWN", "4")])

    def test_mapping_write_and_outbound_scope(self):
        mapping = self.config.location_mapping_ids.filtered(lambda m: m.warehouse_id == self.warehouse)
        with self.assertRaises(AccessError):
            mapping.with_user(self.salesperson).write({"square_location_id": "UNAUTHORIZED"})
        self.assertEqual(mapping.square_location_id, "SQUARE-DOWNTOWN")

        other_company = self.env["res.company"].create({"name": "Unrelated stock company"})
        warehouse = self.env["stock.warehouse"].search([("company_id", "=", other_company.id)], limit=1)
        self.assertTrue(warehouse)
        with self.assertRaises(ValidationError), self.cr.savepoint():
            mapping.write({"warehouse_id": warehouse.id})

        mapping.with_user(self.admin).write({"square_location_id": "OUTSIDE-MERCHANT"})
        self._validate_picking(
            self.warehouse.out_type_id,
            self.warehouse.lot_stock_id,
            self.env.ref("stock.stock_location_customers"),
            user=self.stock_user,
        )
        self.assertEqual(self.quant.quantity, 5)
        self._send_pending_counts()
        self.square_request.assert_called_once()
        self.assertEqual(self.square_request.call_args.args, ("/v2/locations",))
        self.assert_square_counts([])

    def test_another_company_cannot_operate_the_square_stock_service(self):
        company = self.env["res.company"].create({"name": "Another stock company"})
        admin = new_test_user(
            self.env, login="other-square-admin", groups="base.group_system",
            company_id=company.id, company_ids=[Command.set(company.ids)],
        )
        sync = self.stock_sync.with_user(admin).with_context(allowed_company_ids=company.ids)
        with self.assertRaises(AccessError):
            sync.write({"auto_sync_enabled": False})
        with self.assertRaises(AccessError):
            sync.manual_sync_all_products()
        self.assertTrue(self.stock_sync.auto_sync_enabled)
        self.square_request.assert_not_called()

    def test_rolled_back_stock_never_reaches_square_and_failed_counts_retry(self):
        quant = self.quant
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
        # Square was unreachable for more than a day. Stock received in a new
        # bin since this observation must be included in the recovered count.
        pending.counted_at = fields.Datetime.subtract(fields.Datetime.now(), hours=25)
        self.env["stock.quant"].create({
            "product_id": quant.product_id.id,
            "location_id": self.env["stock.location"].create({
                "name": "Additional bin", "location_id": self.warehouse.lot_stock_id.id,
                "usage": "internal",
            }).id,
            "quantity": 3,
        })
        pending.next_attempt = fields.Datetime.now()
        self.assert_square_counts([("SQUARE-DOWNTOWN", "5.5")])
        self.assertFalse(jobs.search([]))


    def test_lost_physical_count_response_replays_the_original_observation(self):
        quant = self.quant
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

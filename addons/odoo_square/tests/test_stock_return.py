import json
import logging
from unittest.mock import patch, MagicMock
from odoo.tests.common import tagged
from odoo.tools import mute_logger
from odoo.addons.odoo_square.models.square_api_client import SquareApiClient

from .common import SquareHttpCase

_logger = logging.getLogger(__name__)


@tagged("post_install", "-at_install", "TestStockReturn")
class TestStockReturn(SquareHttpCase):

    def setUp(self):
        super().setUp()

        self.warehouse = self.env["stock.warehouse"].create(
            {"name": "Richelieu", "code": "RICH"}
        )

        self.square_config = self.env["square.config"].create(
            {
                "name": "Test Config",
                "square_application_id": "test_app",
                "square_access_token": "test_token",
                "square_environment": "sandbox",
            }
        )

        self.env["square.location.mapping"].create(
            {
                "config_id": self.square_config.id,
                "square_location_id": "LOC_RICH",
                "square_location_name": "Boutique Richelieu",
                "warehouse_id": self.warehouse.id,
            }
        )

        self.payment_journal = self.env["account.journal"].create(
            {"name": "Square Test", "type": "bank", "code": "SQT"}
        )
        self.square_config.payment_journal_id = self.payment_journal.id

        self.product = self.env["product.product"].create(
            {
                "name": "Cardigan Selmana Jaune S",
                "default_code": "25FWFESUCARSLMJAC-S",
                "list_price": 110.00,
                "type": "product",
            }
        )

        self.partner = self.env["res.partner"].create(
            {"name": "Test Customer", "email": "test@example.com"}
        )

        self._set_stock(self.product, self.warehouse, 5)

        # Mock Square API
        self._patcher_order = patch.object(
            SquareApiClient, "get_order", side_effect=self._mock_get_order_side,
        )
        self._patcher_catalog = patch.object(
            SquareApiClient, "get_catalog_object", side_effect=self._mock_get_catalog_side,
        )
        self._patcher_order.start()
        self._patcher_catalog.start()

    def tearDown(self):
        for p in (self._patcher_order, self._patcher_catalog):
            try:
                p.stop()
            except RuntimeError:
                pass
        super().tearDown()

    # ── helpers ────────────────────────────────────────────────────────

    def _set_stock(self, product, warehouse, qty):
        self.env["stock.quant"].create(
            {
                "product_id": product.id,
                "location_id": warehouse.lot_stock_id.id,
                "quantity": qty,
            }
        )

    def _get_stock(self, product, warehouse):
        quant = self.env["stock.quant"].search(
            [
                ("product_id", "=", product.id),
                ("location_id", "=", warehouse.lot_stock_id.id),
            ],
            limit=1,
        )
        return quant.quantity if quant else 0.0

    def _create_confirmed_so_with_delivery(self):
        """Create a SO, confirm it, deliver from warehouse, and invoice."""
        so = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "warehouse_id": self.warehouse.id,
                "square_order_id": "ORDER_TEST",
            }
        )
        self.env["sale.order.line"].create(
            {
                "order_id": so.id,
                "product_id": self.product.id,
                "product_uom_qty": 1,
                "price_unit": 91.67,
                "name": self.product.name,
            }
        )

        so.action_confirm()

        picking = so.picking_ids.filtered(
            lambda p: p.picking_type_code == "outgoing"
            and p.state not in ("done", "cancel")
        )[:1]
        self.assertTrue(picking, "Outgoing picking should exist after confirmation")

        picking.action_assign()
        for move in picking.move_ids:
            if move.move_line_ids:
                move.move_line_ids.write({"quantity": move.product_uom_qty})
            else:
                self.env["stock.move.line"].create(
                    {
                        "move_id": move.id,
                        "product_id": move.product_id.id,
                        "product_uom_id": move.product_uom.id,
                        "location_id": move.location_id.id,
                        "location_dest_id": move.location_dest_id.id,
                        "quantity": move.product_uom_qty,
                        "picking_id": picking.id,
                    }
                )
        picking.button_validate()
        self.assertEqual(picking.state, "done")

        invoice = so._create_invoices()
        invoice.action_post()

        return so

    # ── Square API mocks ──────────────────────────────────────────────

    def _mock_get_order(self, order_id):
        if order_id == "ORDER_TEST":
            return {
                "id": order_id,
                "location_id": "LOC_RICH",
                "state": "COMPLETED",
                "line_items": [
                    {
                        "uid": "line_test_1",
                        "name": "Cardigan Selmana Jaune S",
                        "catalog_object_id": "25FWFESUCARSLMJAC-S",
                        "quantity": "1",
                        "total_money": {"amount": 11000, "currency": "EUR"},
                    }
                ],
                "total_money": {"amount": 11000, "currency": "EUR"},
            }
        return None

    def _mock_get_catalog(self, catalog_id):
        if catalog_id == "25FWFESUCARSLMJAC-S":
            return {"success": True, "sku": "25FWFESUCARSLMJAC-S", "name": "Cardigan Selmana Jaune S"}
        return {"success": False, "not_found": True}

    def _mock_get_order_side(self, *args, **kwargs):
        oid = kwargs.get("order_id")
        if oid is None:
            oid = args[1] if len(args) >= 2 else (args[0] if args and isinstance(args[0], str) else None)
        return self._mock_get_order(oid) if oid else None

    def _mock_get_catalog_side(self, *args, **kwargs):
        cid = kwargs.get("catalog_object_id")
        if cid is None:
            cid = args[1] if len(args) >= 2 else (args[0] if args and isinstance(args[0], str) else None)
        return self._mock_get_catalog(cid) if cid else {"success": False}

    @mute_logger(
        "odoo.addons.odoo_square.models.square_refund",
        "odoo.addons.odoo_square.models.square_order_processor",
    )
    def test_refund_return_picking_forces_incoming_quantities(self):
        """
        After a refund, the return (incoming) picking must be fully validated
        with done quantities equal to demanded quantities, restoring stock.

        Before the fix, button_validate only forced quantities for 'outgoing',
        so the incoming picking was validated with qty=0 and stock stayed depleted.
        """
        so = self._create_confirmed_so_with_delivery()
        stock_after_sale = self._get_stock(self.product, self.warehouse)

        refund = self.env["square.refund"].create(
            {
                "square_refund_id": "REFUND_TEST",
                "square_order_id": "ORDER_TEST",
                "status": "pending",
                "refund_amount": 110.00,
                "currency_id": self.env.ref("base.EUR").id,
                "sale_order_id": so.id,
            }
        )

        refund.action_process_refund()

        # Return picking should have been created
        self.assertTrue(
            refund.return_picking_ids,
            "Return picking should be created during pending refund actions",
        )

        # Now complete the refund
        refund.write({"status": "completed"})
        refund.action_process_refund()

        # Verify return picking is done
        for picking in refund.return_picking_ids:
            self.assertEqual(
                picking.state,
                "done",
                f"Return picking {picking.name} should be validated (done), got {picking.state}",
            )
            for move in picking.move_ids:
                self.assertEqual(
                    move.quantity,
                    move.product_uom_qty,
                    f"Move done qty should equal demand qty ({move.product_uom_qty}), "
                    f"got {move.quantity}",
                )

        # Stock must be restored
        stock_after_refund = self._get_stock(self.product, self.warehouse)
        self.assertGreater(
            stock_after_refund,
            stock_after_sale,
            f"Stock should increase after refund return. "
            f"Before refund: {stock_after_sale}, after: {stock_after_refund}",
        )

    @mute_logger(
        "odoo.addons.odoo_square.models.square_refund",
        "odoo.addons.odoo_square.models.square_order_processor",
    )
    def test_force_quantity_works_for_incoming_picking(self):
        """
        Directly verify that _force_quantity_for_square + button_validate
        works for incoming pickings, not just outgoing.
        """
        so = self._create_confirmed_so_with_delivery()

        outgoing = so.picking_ids.filtered(
            lambda p: p.picking_type_code == "outgoing" and p.state == "done"
        )[:1]
        self.assertTrue(outgoing)

        # Create a return via the wizard
        return_wiz = (
            self.env["stock.return.picking"]
            .with_context(active_id=outgoing.id, active_model="stock.picking")
            .create({})
        )
        result = return_wiz.create_returns()
        return_picking = self.env["stock.picking"].browse(result["res_id"])

        self.assertEqual(return_picking.picking_type_code, "incoming")

        if return_picking.state == "draft":
            return_picking.action_confirm()
        if return_picking.state in ("confirmed", "waiting"):
            return_picking.action_assign()

        # This is exactly what _complete_refund does — only with_context(force_validate=True)
        return_picking.with_context(force_validate=True).button_validate()

        self.assertEqual(
            return_picking.state,
            "done",
            "Incoming picking with force_validate should be fully validated",
        )

        for move in return_picking.move_ids:
            self.assertGreater(
                move.quantity,
                0,
                f"Move for {move.product_id.name} should have done qty > 0",
            )

    @mute_logger(
        "odoo.addons.odoo_square.controllers.square_webhook",
        "odoo.addons.odoo_square.models.square_order_processor",
    )
    def test_sync_skips_negative_quantity_lines(self):
        """
        When Square sends an order.updated (OPEN) with a line whose quantity
        is negative (return/exchange), _sync_order_line_changes must NOT write
        that negative value to the SO line — doing so creates phantom forecast.
        """
        processor = self.env["square.order.processor"]

        so = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "warehouse_id": self.warehouse.id,
                "square_order_id": "ORDER_SYNC_NEG",
            }
        )
        line = self.env["sale.order.line"].create(
            {
                "order_id": so.id,
                "product_id": self.product.id,
                "product_uom_qty": 1,
                "price_unit": 91.67,
                "name": self.product.name,
                "square_line_id": "line_sync_1",
            }
        )

        # Simulate Square sending an OPEN update with a negative qty line
        order_data = {
            "order_id": "ORDER_SYNC_NEG",
            "state": "OPEN",
            "line_items": [
                {
                    "uid": "line_sync_1",
                    "name": "Cardigan Selmana Jaune S",
                    "quantity": "-1",
                    "total_money": {"amount": -11000, "currency": "EUR"},
                }
            ],
        }

        processor.process_square_order_update(order_data, so)

        line.invalidate_recordset()
        self.assertEqual(
            line.product_uom_qty,
            1,
            "SO line quantity must remain positive (1) — negative qty from "
            "webhook should be skipped",
        )

    @mute_logger(
        "odoo.addons.odoo_square.controllers.square_webhook",
        "odoo.addons.odoo_square.models.square_order_processor",
    )
    def test_sync_skips_zero_quantity_lines(self):
        """
        Lines with quantity=0 (cancelled items) should also be skipped.
        """
        processor = self.env["square.order.processor"]

        so = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "warehouse_id": self.warehouse.id,
                "square_order_id": "ORDER_SYNC_ZERO",
            }
        )
        line = self.env["sale.order.line"].create(
            {
                "order_id": so.id,
                "product_id": self.product.id,
                "product_uom_qty": 2,
                "price_unit": 91.67,
                "name": self.product.name,
                "square_line_id": "line_zero_1",
            }
        )

        order_data = {
            "order_id": "ORDER_SYNC_ZERO",
            "state": "OPEN",
            "line_items": [
                {
                    "uid": "line_zero_1",
                    "name": "Cardigan Selmana Jaune S",
                    "quantity": "0",
                    "total_money": {"amount": 0, "currency": "EUR"},
                }
            ],
        }

        processor.process_square_order_update(order_data, so)

        line.invalidate_recordset()
        self.assertEqual(
            line.product_uom_qty,
            2,
            "SO line quantity must stay at 2 — zero qty line should be skipped",
        )

    @mute_logger(
        "odoo.addons.odoo_square.controllers.square_webhook",
        "odoo.addons.odoo_square.models.square_order_processor",
    )
    def test_sync_skips_when_payload_has_returns(self):
        """
        When an order.updated (OPEN) payload contains a 'returns' key,
        _sync_order_line_changes should not be called at all — the return
        must go through the dedicated refund/exchange flow.
        """
        processor = self.env["square.order.processor"]

        so = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "warehouse_id": self.warehouse.id,
                "square_order_id": "ORDER_SYNC_RETURN",
            }
        )
        line = self.env["sale.order.line"].create(
            {
                "order_id": so.id,
                "product_id": self.product.id,
                "product_uom_qty": 1,
                "price_unit": 91.67,
                "name": self.product.name,
                "square_line_id": "line_ret_1",
            }
        )

        order_data = {
            "order_id": "ORDER_SYNC_RETURN",
            "state": "OPEN",
            "returns": [
                {
                    "source_order_id": "ORDER_SYNC_RETURN",
                    "return_line_items": [
                        {
                            "uid": "ret_line_1",
                            "source_line_item_uid": "line_ret_1",
                            "name": "Cardigan Selmana Jaune S",
                            "quantity": "1",
                            "catalog_object_id": "25FWFESUCARSLMJAC-S",
                        }
                    ],
                }
            ],
            "line_items": [
                {
                    "uid": "line_ret_1",
                    "name": "Cardigan Selmana Jaune S",
                    "quantity": "0",
                    "total_money": {"amount": 0, "currency": "EUR"},
                }
            ],
        }

        processor.process_square_order_update(order_data, so)

        line.invalidate_recordset()
        self.assertEqual(
            line.product_uom_qty,
            1,
            "SO line qty must remain 1 — payload with 'returns' key should "
            "skip line sync entirely",
        )

    @mute_logger(
        "odoo.addons.odoo_square.controllers.square_webhook",
        "odoo.addons.odoo_square.models.square_order_processor",
    )
    def test_sync_positive_quantity_still_works(self):
        """
        Normal positive quantity updates (e.g., customer adds items before
        payment) must still be synced correctly.
        """
        processor = self.env["square.order.processor"]

        so = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "warehouse_id": self.warehouse.id,
                "square_order_id": "ORDER_SYNC_POS",
            }
        )
        line = self.env["sale.order.line"].create(
            {
                "order_id": so.id,
                "product_id": self.product.id,
                "product_uom_qty": 1,
                "price_unit": 91.67,
                "name": self.product.name,
                "square_line_id": "line_pos_1",
            }
        )

        order_data = {
            "order_id": "ORDER_SYNC_POS",
            "state": "OPEN",
            "line_items": [
                {
                    "uid": "line_pos_1",
                    "name": "Cardigan Selmana Jaune S",
                    "quantity": "3",
                    "total_money": {"amount": 33000, "currency": "EUR"},
                }
            ],
        }

        processor.process_square_order_update(order_data, so)

        line.invalidate_recordset()
        self.assertEqual(
            line.product_uom_qty,
            3,
            "Positive quantity update from 1 → 3 should be applied normally",
        )

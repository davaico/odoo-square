import json
from datetime import timedelta
from unittest.mock import patch

from odoo import fields
from odoo.tests.common import new_test_user, tagged
from odoo.addons.odoo_square.models.square_api_client import SquareApiClient

from .common import SquareHttpCase


@tagged("post_install", "-at_install")
class TestSquarePosSecurity(SquareHttpCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.config = cls.env["square.config"].create({
            "square_application_id": "synthetic-app",
            "square_access_token": "synthetic-token",
            "square_webhook_signature_key": cls.square_test_key,
        })
        cls.sales = new_test_user(cls.env, login="square_sales_test", groups="sales_team.group_sale_salesman,base.group_allow_export")
        cls.staff = new_test_user(cls.env, login="square_staff_test", groups="base.group_user,base.group_allow_export")
        cls.warehouse = cls.env["stock.warehouse"].search([("company_id", "=", cls.config.company_id.id)], limit=1)
        cls.other_warehouse = cls.env["stock.warehouse"].create({"name": "Second Warehouse", "code": "SEC2"})
        cls.mapping = cls.env["square.location.mapping"].create({
            "config_id": cls.config.id,
            "square_location_id": "SEC-LOCATION",
            "square_location_name": "Security Fixture",
            "warehouse_id": cls.warehouse.id,
        })
        cls.partner = cls.env["res.partner"].create({"name": "Synthetic Customer"})
        cls.sale = cls.env["sale.order"].create({
            "partner_id": cls.partner.id, "user_id": cls.sales.id, "square_order_id": "SEC-EXISTING",
        })
        cls.refund = cls.env["square.refund"].create({
            "square_refund_id": "SEC-REFUND", "square_order_id": "SEC-EXISTING", "sale_order_id": cls.sale.id,
            "refund_amount": 1, "currency_id": cls.env.ref("base.EUR").id, "square_data": {"historical": "private"},
        })
        cls.log = cls.env["square.integration.log"].create({
            "company_id": cls.config.company_id.id, "event_type": "webhook_received",
            "title": "Legacy inspection fixture", "technical_details": "synthetic private detail",
        })
        cls.queue = cls.env["square.webhook.queue"].create({
            "webhook_event_id": "SEC-RETRY", "event_type": "order.updated",
            "order_data": '{"private": "historical", "order_id": "SEC-ORDER", "state": "CANCELED"}',
            "square_order_id": "SEC-ORDER", "next_retry_time": fields.Datetime.now(),
        })

    def _call_as(self, user):
        self.authenticate(user.login, user.login)

    def _database_state(self):
        return {
            model: self.env[model].search_count([])
            for model in (
                "sale.order", "account.move", "account.payment", "stock.move", "stock.quant",
                "square.refund", "square.webhook.queue", "square.integration.log", "square.event.receipt",
            )
        }

    def _notification(self, kind, event_id, state, *, order_id="SEC-ORDER", **values):
        return {
            "type": kind,
            "event_id": event_id,
            "data": {"object": {kind.replace(".", "_"): {
                "id": order_id, "state": state, "location_id": "SEC-LOCATION", **values,
            }}},
        }

    def test_sales_cannot_call_payload_or_inventory_helpers(self):
        self._call_as(self.sales)
        before = self._database_state()
        with patch.object(SquareApiClient, "_make_api_request") as square:
            order = {"order_id": "SEC-FORGED", "state": "COMPLETED"}
            for model, method, args in (
                ("square.order.processor", "process_square_order", (order,)),
                ("square.order.processor", "_process_square_order", (order,)),
                ("square.order.processor", "process_square_order_update", (order, self.sale.id)),
                ("sale.order", "create_from_square", (order,)),
                ("sale.order", "write", ([self.sale.id], {"square_order_id": "SEC-FORGED"})),
                ("square.manual.resync.wizard", "action_validate_resync", ([1],)),
                ("square.api.client", "set_physical_count", ("CATALOG", "LOCATION", 80, "SKU")),
                ("square.api.client", "_set_physical_count", ("CATALOG", "LOCATION", 80, "SKU")),
                ("square.stock.sync", "sync_product_stock_for_location", (1, "LOCATION", 80)),
            ):
                with self.subTest(model=model, method=method):
                    self._assert_square_rpc_denied(model, method, *args)
            square.assert_not_called()
        self.assertEqual(self._database_state(), before)
        self.assertEqual(self.sale.square_order_id, "SEC-EXISTING")

    def test_configuration_and_payloads_require_role_and_company(self):
        for user in (self.sales, self.staff):
            self._call_as(user)
            for record, names in (
                (self.config, ["square_access_token", "square_webhook_signature_key"]),
                (self.log, ["technical_details"]),
                (self.queue, ["order_data"]),
                (self.refund, ["square_data"]),
            ):
                with self.subTest(user=user.id, model=record._name):
                    self._assert_square_rpc_denied(record._name, "read", record.ids, names)
                    self._assert_square_rpc_denied(record._name, "export_data", record.ids, names)
        foreign = self.env["res.company"].create({"name": "Other POS Company"})
        self.authenticate("admin", "admin")
        self.assertEqual(self._square_rpc(
            self.log._name, "read", self.log.ids, ["title"],
        )["result"][0]["title"], self.log.title)
        self.assertEqual(self._square_rpc(
            self.config._name, "read", self.config.ids, ["square_access_token"],
        )["result"][0]["square_access_token"], self.config.square_access_token)
        self.log.company_id = foreign
        self.queue.company_id = foreign
        self.refund.sale_order_id = self.env["sale.order"].with_company(foreign).create({
            "company_id": foreign.id, "partner_id": self.partner.id,
        })
        for record, name in ((self.log, "technical_details"), (self.queue, "order_data"),
                             (self.refund, "square_data")):
            self._assert_square_rpc_denied(record._name, "read", record.ids, [name])
            self._assert_square_rpc_denied(record._name, "export_data", record.ids, [name])

    def test_missing_or_invalid_signing_key_has_no_effects(self):
        before = self._database_state()
        order = json.dumps(self._notification("order.created", "SEC-NO-SIGNATURE", "COMPLETED"))
        refund = json.dumps({
            "type": "refund.updated",
            "event_id": "SEC-NO-SIGNATURE",
            "data": {"object": {"refund": {
                "id": self.refund.square_refund_id, "order_id": self.sale.square_order_id, "status": "COMPLETED",
            }}},
        })
        for active, key in ((True, False), (False, self.square_test_key), (True, self.square_test_key)):
            self.config.write({"active": active, "square_webhook_signature_key": key})
            for body in (order, refund, b"\xff"):
                result = self.url_open("/square/webhook", data=body, headers={
                    "Content-Type": "application/json",
                    "X-Square-Hmacsha256-Signature": "invalid-signature",
                })
                self.assertEqual(result.status_code, 403)
                self.assertEqual(self._database_state(), before)
                self.assertEqual(self.refund.status, "pending")

    def test_queue_and_log_writes_are_denied_and_signed_replay_works(self):
        self._call_as(self.sales)
        before = self._database_state()
        for record, vals in (
            (self.queue, {"state": "pending", "verified": True}),
            (self.log, {"status": "success", "webhook_event_id": "SEC-CREATE"}),
        ):
            self._assert_square_rpc_denied(record._name, "write", record.ids, vals)
            self._assert_square_rpc_denied(record._name, "unlink", record.ids)
        self._assert_square_rpc_denied("square.webhook.queue", "process_pending_events")
        self._assert_square_rpc_denied("square.webhook.queue", "process_pending_for_order", "SEC-ORDER")
        self.assertEqual(self._database_state(), before)
        self.assertFalse(self.queue.verified)
        self.assertEqual(self.log.status, "info")

        # An old success row and an old queue payload can have the same identifiers.
        self.log.write({"status": "success", "webhook_event_id": "SEC-CREATE", "square_order_id": "SEC-ORDER"})
        update = self._notification("order.updated", "SEC-RETRY", "OPEN", version=2, line_items=[])
        create = self._notification("order.created", "SEC-CREATE", "OPEN", version=1)
        with patch.object(SquareApiClient, "get_order", return_value=None):
            queued = self._post_signed_square_webhook("/square/webhook", json=update).json()
            self.assertEqual(queued["status"], "queued")
            created = self._post_signed_square_webhook("/square/webhook", json=create).json()
            self.assertEqual(created["status"], "success")
            replay = self._post_signed_square_webhook("/square/webhook", json=create).json()
            self.assertEqual(replay["status"], "already_processed")

        imported = self.env["sale.order"].browse(created["sale_order_id"])
        self.assertEqual(imported.square_order_version, 2)
        self.assertEqual(imported.state, "draft")
        self.assertFalse(imported.invoice_ids)
        self.assertEqual(self.queue.state, "failed")
        trusted = self.env["square.webhook.queue"].search([("verified", "=", True)])
        self.assertEqual(trusted.state, "completed")

    def test_cron_runner_cannot_apply_legacy_payload_and_retries_signed_update(self):
        update = self._notification(
            "order.updated", "SEC-CRON", "OPEN", order_id="SEC-CRON-ORDER", version=7, line_items=[],
        )
        with patch.object(SquareApiClient, "get_order", return_value=None):
            result = self._post_signed_square_webhook("/square/webhook", json=update).json()
        self.assertEqual(result["status"], "queued")
        sale = self.env["sale.order"].create({"partner_id": self.partner.id, "square_order_id": "SEC-CRON-ORDER"})
        queued = self.env["square.webhook.queue"].search([("verified", "=", True)])
        queued.next_retry_time = fields.Datetime.now()
        self.queue.write({
            "square_order_id": sale.square_order_id,
            "order_data": json.dumps({"id": sale.square_order_id, "state": "CANCELED"}),
        })
        cron = self.env.ref("odoo_square.cron_square_webhook_queue")
        now = fields.Datetime.now()
        self.env.cr.execute("UPDATE square_webhook_queue SET create_date = %s WHERE id = %s", (now - timedelta(days=45), self.queue.id))
        queued.next_retry_time = now + timedelta(hours=1)
        self.authenticate("admin", "admin")
        result = self._square_rpc("ir.cron", "method_direct_trigger", cron.ids)
        self.assertNotIn("error", result)
        self.assertEqual(queued.state, "pending")
        self.assertEqual(self.queue.state, "failed")
        queued.next_retry_time = now
        result = self._square_rpc("ir.cron", "method_direct_trigger", cron.ids)
        self.assertNotIn("error", result)
        self.assertEqual(queued.state, "completed")
        self.assertEqual(self.queue.state, "failed")
        self.assertEqual(sale.square_order_version, 7)
        self.assertEqual(sale.state, "draft")
        self.assertFalse(sale.invoice_ids)

    def test_only_administrator_can_change_location_mapping(self):
        self._call_as(self.sales)
        new_warehouse = {"warehouse_id": self.other_warehouse.id}
        self._assert_square_rpc_denied(self.mapping._name, "write", self.mapping.ids, new_warehouse)
        self.assertEqual(self.mapping.warehouse_id, self.warehouse)
        self.authenticate("admin", "admin")
        self.assertTrue(self._square_rpc(self.mapping._name, "write", self.mapping.ids, new_warehouse)["result"])
        self.assertEqual(self.config.get_warehouse_for_location("SEC-LOCATION"), self.other_warehouse)
        other_company = self.env["res.company"].create({"name": "Other Warehouse Company"})
        foreign = self.env["stock.warehouse"].search([("company_id", "=", other_company.id)], limit=1)
        self.assertTrue(foreign)
        response = self._square_rpc(self.mapping._name, "write", self.mapping.ids, {"warehouse_id": foreign.id})
        self.assertIn("error", response)
        self.assertEqual(self.mapping.warehouse_id, self.other_warehouse)

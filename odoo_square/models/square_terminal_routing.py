# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from functools import cache
from urllib.parse import quote

from odoo import _, models
from odoo.exceptions import UserError


class SquareOrderProcessor(models.Model):
    _inherit = "square.order.processor"

    def _square_terminal_import_config(self, square_config=None, *, environment=None):
        if "pos.square.order" not in self.env:
            return None
        config = square_config if square_config is not None else self.env["square.config"].sudo().search(
            [("company_id", "=", self.sudo().env.company.id)], limit=1,
        )
        if not config or (environment and environment.lower() != config.square_environment):
            return None
        if self.env["pos.square.order"]._square_terminal_import_scope_exists(
            config.company_id.id, config.square_environment,
        ):
            return config

    def _square_terminal_matches(self, kind, resource, config, *, merchant_id=None):
        return self.env["pos.square.order"]._square_terminal_matches_import_resource(
            config.company_id.id, config.square_environment, kind, resource, merchant_id=merchant_id,
        )

    def _square_read_routing_resource(self, kind, resource_id, config):
        if not isinstance(resource_id, str) or not resource_id:
            return None
        try:
            response = self.env["square.api.client"].sudo()._make_api_request(
                f"/v2/{kind}s/{quote(resource_id, safe='')}", square_config=config,
            )
        except UserError:
            return None
        result = response.get(kind) if isinstance(response, dict) else None
        if isinstance(result, dict) and result.get("id") == resource_id:
            return result

    def _square_terminal_retry(self):
        return {"status": "retry", "message": _("Could not verify Odoo POS Square Terminal ownership")}

    def _square_terminal_resource_result(self, kind, resource, config, *, expected_merchant=None):
        if not resource or not resource.get("location_id"):
            return self._square_terminal_retry()
        if not self._square_terminal_matches(kind, resource, config):
            return None
        location = self._square_read_routing_resource("location", resource["location_id"], config)
        merchant_id = location and location.get("merchant_id")
        if not isinstance(merchant_id, str) or not merchant_id:
            return self._square_terminal_retry()
        if self._square_terminal_matches(kind, resource, config, merchant_id=merchant_id):
            if expected_merchant and expected_merchant != merchant_id:
                return self._square_terminal_retry()
            return {"status": "ignored", "message": _("Odoo POS Square Terminal operation")}

    def _square_terminal_order_result(self, order_id, config, *, full_order=None, merchant_id=None, read_order=None):
        if not order_id or not config:
            return None
        read_order = read_order or self.env["square.api.client"].sudo().get_order
        order = full_order if full_order is not None else read_order(order_id, square_config=config)
        if not isinstance(order, dict) or order.get("id") != order_id or not order.get("location_id"):
            return self._square_terminal_retry()

        retry = self._square_terminal_resource_result("order", order, config, expected_merchant=merchant_id)
        if retry and retry["status"] == "ignored":
            return retry

        source_ids = {
            entry["source_order_id"] for entry in order.get("returns") or []
            if isinstance(entry, dict) and isinstance(entry.get("source_order_id"), str) and entry["source_order_id"]
        }
        for source_id in sorted(source_ids - {order_id}):
            source = read_order(source_id, square_config=config)
            if not isinstance(source, dict) or source.get("id") != source_id or not source.get("location_id"):
                retry = retry or self._square_terminal_retry()
                continue
            result = self._square_terminal_resource_result("order", source, config, expected_merchant=merchant_id)
            if result and result["status"] == "ignored":
                return result
            retry = retry or result
        return retry

    def _square_terminal_payment_result(self, notice, config):
        if not config or not self._square_terminal_matches("payment", notice, config):
            return None
        payment = self._square_read_routing_resource("payment", notice["id"], config)
        return self._square_terminal_resource_result("payment", payment, config)

    def _square_terminal_refund_result(self, refund_id):
        config = self._square_terminal_import_config()
        if not config:
            return None
        refund = self._square_read_routing_resource("refund", refund_id, config)
        if not refund or not refund.get("location_id"):
            return self._square_terminal_retry()
        # A return has its own Square order, while payment_id points to the original sale.
        if not refund.get("payment_id"):
            return self._square_terminal_order_result(refund.get("order_id"), config)
        payment = self._square_read_routing_resource("payment", refund["payment_id"], config)
        routing = self._square_terminal_resource_result("payment", payment, config)
        if routing or not payment.get("order_id"):
            return routing
        return self._square_terminal_order_result(payment["order_id"], config)

    def _square_pos_order_result(
        self, *, order_id, location_id=None, merchant_id=None, environment=None, square_config=None, full_order=None,
    ):
        # Both classifiers must see the same authenticated order, including a failed read.
        read_order = cache(self.env["square.api.client"].sudo().get_order)
        website = self._square_online_event_result(
            order_id=order_id, location_id=location_id, merchant_id=merchant_id,
            environment=environment, square_config=square_config, full_order=full_order, read_order=read_order,
        )
        if website and website["status"] == "ignored":
            return website
        config = self._square_terminal_import_config(square_config, environment=environment)
        return self._square_terminal_order_result(
            order_id, config, full_order=full_order, merchant_id=merchant_id, read_order=read_order,
        ) or website

from urllib.parse import quote

from odoo import models
from odoo.exceptions import UserError


class SquareWebsiteRouting(models.Model):
    _inherit = "square.api.client"

    def _website_routing_config(self, config=None):
        return config or self.env["square.config"].sudo().with_context(active_test=True).search(
            [("company_id", "=", self.env.company.id)], limit=1,
        )

    def _read_pos_order(self, order_id, config=None):
        order = self._get_order(order_id, square_config=config) if config else self._get_order(order_id)
        return self._route_website_order(order_id, self._website_routing_config(config), order=order or {})

    def _website_scope_exists(self, config):
        online_scope_exists = getattr(
            self.env["payment.transaction"], "_square_online_pos_scope_exists", None
        )
        return bool(online_scope_exists and online_scope_exists(config.square_environment if config else None))

    def _read_website_routing_resource(self, collection, kind, resource_id, config):
        try:
            response = self._make_api_request(
                f"/v2/{collection}/{quote(resource_id, safe='')}", square_config=config
            )
        except UserError:
            return {}
        resource = response.get(kind) if isinstance(response, dict) else None
        if isinstance(resource, dict) and resource.get("id") == resource_id:
            return resource
        return {}

    def _website_resource_matches(self, kind, resource, config):
        """Return True for website ownership, False for POS, or None for an unresolved match."""
        matches = self.env["payment.transaction"]._square_online_matches_pos_resource
        if not matches(config.square_environment, kind, resource):
            return False
        # POS may use a separate application.
        # The Square location retrieved with its own token identifies the merchant.
        location_id = resource["location_id"]
        location = self._read_website_routing_resource("locations", "location", location_id, config)
        merchant_id = location.get("merchant_id")
        if not merchant_id:
            return None
        return matches(config.square_environment, kind, resource, merchant_id=merchant_id)

    def _route_website_order(self, order_id, config, *, order=None):
        """Return a routing response, and reuse any order retrieved for POS ingestion."""
        if not order_id or not self._website_scope_exists(config):
            return None, order
        if not config:
            return self._website_routing_retry(), order
        if order is None:
            order = self._get_order(order_id, square_config=config)
        if not order or order.get("id") != order_id or not order.get("location_id"):
            return self._website_routing_retry(), order
        ownership = self._website_resource_matches("order", order, config)
        if ownership is not False:
            return self._website_routing_result(ownership), order

        # Square sends the return Order independently of the payment refund.
        source_ids = {
            item["source_order_id"] for item in order.get("returns") or []
            if isinstance(item, dict) and item.get("source_order_id")
        }
        retry = False
        for source_id in sorted(source_ids - {order_id}):
            source = self._get_order(source_id, square_config=config)
            if not source or source.get("id") != source_id or not source.get("location_id"):
                retry = True
                continue
            ownership = self._website_resource_matches("order", source, config)
            if ownership:
                return self._website_routing_result(True), order
            retry = retry or ownership is None
        return (self._website_routing_retry() if retry else None), order

    def _route_website_refund(self, event, config, *, signed=False):
        refund_id = event.get("id")
        if not refund_id or not self._website_scope_exists(config):
            return None
        if not config:
            return self._website_routing_retry()

        # A signed notification naming both IDs of an imported POS sale has all the
        # evidence needed locally. Return orders and unrecognized sales still need Square.
        order_id, payment_id = event.get("order_id"), event.get("payment_id")
        if signed and order_id and payment_id and self.env["sale.order"].sudo().search_count([
            ("square_order_id", "=", order_id), ("square_payment_id", "=", payment_id),
            ("company_id", "=", config.company_id.id),
        ], limit=1):
            return None

        refund = self._read_website_routing_resource("refunds", "refund", refund_id, config)
        if not refund or not refund.get("location_id"):
            return self._website_routing_retry()
        ownership = self._website_resource_matches("refund", refund, config)
        if ownership is not False:
            return self._website_routing_result(ownership)
        # PaymentRefund.order_id belongs to the return. payment_id points to the original payment.
        payment_id = refund.get("payment_id")
        if not payment_id:
            return None
        payment = self._read_website_routing_resource("payments", "payment", payment_id, config)
        if not payment or not payment.get("location_id"):
            return self._website_routing_retry()
        return self._website_routing_result(self._website_resource_matches("payment", payment, config))

    def _website_routing_result(self, ownership):
        if ownership is None:
            return self._website_routing_retry()
        if ownership:
            return {"status": "ignored", "message": "Square website operation"}

    def _website_routing_retry(self):
        return {"status": "retry", "message": "Could not verify Square website ownership"}

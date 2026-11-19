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
        return self._route_pos_order(order_id, self._website_routing_config(config), order=order or {})

    def _website_scope_exists(self, config):
        online_scope_exists = getattr(
            self.env["payment.transaction"], "_square_online_pos_scope_exists", None
        )
        return bool(online_scope_exists and online_scope_exists(config.square_environment if config else None))

    def _read_routing_resource(self, collection, kind, resource_id, config):
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
        location = self._read_routing_resource("locations", "location", location_id, config)
        merchant_id = location.get("merchant_id")
        if not merchant_id:
            return None
        return matches(config.square_environment, kind, resource, merchant_id=merchant_id)

    def _terminal_scope_exists(self, config):
        return (
            config and "pos.square.order" in self.env
            and self.env["pos.square.order"]._square_terminal_import_scope_exists(
                config.company_id.id, config.square_environment,
            )
        )

    def _terminal_matches_import_resource(self, kind, resource, config, *, merchant_id=None):
        return self.env["pos.square.order"]._square_terminal_matches_import_resource(
            config.company_id.id, config.square_environment, kind, resource, merchant_id=merchant_id,
        )

    def _terminal_resource_routing(self, kind, resource, config):
        if not self._terminal_matches_import_resource(kind, resource, config):
            return None
        location = self._read_routing_resource("locations", "location", resource["location_id"], config)
        merchant_id = location.get("merchant_id")
        if not merchant_id:
            return self._terminal_routing_retry()
        if self._terminal_matches_import_resource(kind, resource, config, merchant_id=merchant_id):
            return {"status": "ignored", "message": "Odoo POS Square Terminal operation"}

    def _resource_ownership(self, kind, resource, config, *, website, terminal):
        if terminal:
            routing = self._terminal_resource_routing(kind, resource, config)
            if routing:
                return routing
        if website:
            return self._website_routing_result(self._website_resource_matches(kind, resource, config))

    def _route_terminal_payment(self, notice, config):
        if (
            not config or "pos.square.order" not in self.env
            or not self._terminal_matches_import_resource("payment", notice, config)
        ):
            return None
        # The notification identifies what to retrieve. Only Square's scoped
        # Payments API response can confirm which register owns the payment.
        payment = self._read_routing_resource("payments", "payment", notice["id"], config)
        if not payment or not payment.get("location_id"):
            return self._terminal_routing_retry()
        return self._terminal_resource_routing("payment", payment, config)

    def _route_pos_order(self, order_id, config, *, order=None):
        """Check Odoo-originated operations before allowing Square POS ingestion."""
        if not order_id:
            return None, order
        website = self._website_scope_exists(config)
        terminal = self._terminal_scope_exists(config)
        if not website and not terminal:
            return None, order
        retry_result = self._terminal_routing_retry() if terminal else self._website_routing_retry()
        if not config:
            return retry_result, order
        if order is None:
            order = self._get_order(order_id, square_config=config)
        if not order or order.get("id") != order_id or not order.get("location_id"):
            return retry_result, order

        ownership = self._resource_ownership("order", order, config, website=website, terminal=terminal)
        if ownership and ownership["status"] == "ignored":
            return ownership, order
        retry = ownership

        # Square sends the return Order independently of the payment refund.
        source_ids = {
            item["source_order_id"] for item in order.get("returns") or []
            if isinstance(item, dict) and isinstance(item.get("source_order_id"), str) and item["source_order_id"]
        }
        for source_id in sorted(source_ids - {order_id}):
            source = self._get_order(source_id, square_config=config)
            if not source or source.get("id") != source_id or not source.get("location_id"):
                retry = retry or retry_result
                continue
            ownership = self._resource_ownership("order", source, config, website=website, terminal=terminal)
            if ownership and ownership["status"] == "ignored":
                return ownership, order
            retry = retry or ownership
        return retry, order

    def _terminal_routing_retry(self):
        return {"status": "retry", "message": "Could not verify Odoo POS Square Terminal ownership"}

    def _route_pos_refund(self, event, config, *, signed=False):
        refund_id = event.get("id")
        if not refund_id:
            return None
        website = self._website_scope_exists(config)
        terminal = self._terminal_scope_exists(config)
        if not website and not terminal:
            return None
        retry_result = self._terminal_routing_retry() if terminal else self._website_routing_retry()
        if not config:
            return retry_result

        # A signed notification naming both IDs of an imported POS sale has all the
        # evidence needed locally. Return orders and unrecognized sales still need Square.
        order_id, payment_id = event.get("order_id"), event.get("payment_id")
        if signed and order_id and payment_id and self.env["sale.order"].sudo().search_count([
            ("square_order_id", "=", order_id), ("square_payment_id", "=", payment_id),
            ("company_id", "=", config.company_id.id),
        ], limit=1):
            return None

        refund = self._read_routing_resource("refunds", "refund", refund_id, config)
        if not refund or not refund.get("location_id"):
            return retry_result
        if website:
            routing = self._website_routing_result(self._website_resource_matches("refund", refund, config))
            if routing:
                return routing
        # PaymentRefund.order_id belongs to the return. payment_id points to the original payment.
        payment_id = refund.get("payment_id")
        if not payment_id:
            return self._route_pos_order(refund.get("order_id"), config)[0] if terminal else None
        payment = self._read_routing_resource("payments", "payment", payment_id, config)
        if not payment or not payment.get("location_id"):
            return retry_result
        return self._resource_ownership("payment", payment, config, website=website, terminal=terminal)

    def _website_routing_result(self, ownership):
        if ownership is None:
            return self._website_routing_retry()
        if ownership:
            return {"status": "ignored", "message": "Square website operation"}

    def _website_routing_retry(self):
        return {"status": "retry", "message": "Could not verify Square website ownership"}

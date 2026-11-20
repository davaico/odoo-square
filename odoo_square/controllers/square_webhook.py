# -*- coding: utf-8 -*-
import json
import logging
import base64
import hashlib
import hmac
from odoo import http, fields
from odoo.http import request
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY
from odoo.exceptions import UserError, ValidationError, AccessError

_logger = logging.getLogger(__name__)


class SquareWebhookController(http.Controller):

    @http.route(
        "/square/webhook",
        type="http",
        auth="none",
        methods=["POST"],
        csrf=False,
        readonly=False,
    )
    def square_webhook(self, **kwargs):
        """
        Main webhook endpoint for Square events
        Expected to be configured in Square as: https://<random-string>.ngrok-free.app/square/webhook
        """
        try:
            # Get raw request body for signature validation
            raw_body = request.httprequest.data
            square_signature = request.httprequest.headers.get(
                "x-square-hmacsha256-signature"
            )

            # Validate webhook signature
            # Build the correct notification URL that Square uses for signing
            # Square signs using the base URL without language prefixes
            base_url = f"https://{request.httprequest.host}"
            notification_url = f"{base_url}/square/webhook"
            _logger.info(
                f"Notification URL for signature validation: {notification_url}"
            )

            # Validate webhook signature
            config = request.env["square.config"].sudo().search([("active", "=", True)], limit=1)
            if not self._validate_webhook_signature(
                raw_body, square_signature, notification_url, config
            ):
                _logger.error("Square webhook: Invalid signature")
                return http.Response(
                    json.dumps({"status": "error", "message": "Invalid signature"}),
                    content_type="application/json",
                    status=403,
                )

            # Get JSON data from request
            if (
                request.httprequest.content_type
                and "application/json" in request.httprequest.content_type
            ):
                try:
                    data = json.loads(raw_body.decode("utf-8"))
                except (UnicodeError, ValueError):
                    return http.Response(status=400)
            else:
                _logger.error("Square webhook: Invalid content type")
                return http.Response(
                    json.dumps(
                        {
                            "status": "error",
                            "message": "Content-Type must be application/json",
                        }
                    ),
                    content_type="application/json",
                    status=400,
                )

            if not isinstance(data, dict) or not data:
                _logger.error("Square webhook: No data received")
                return http.Response(
                    json.dumps({"status": "error", "message": "No data received"}),
                    content_type="application/json",
                    status=400,
                )

            # Extract event type and data
            event_type = data.get("type")
            event_data = data.get("data", {})

            event_id = data.get("event_id")
            if (
                not isinstance(event_type, str) or not event_type
                or not isinstance(event_data, dict)
                or (event_id is not None and (
                    not isinstance(event_id, str) or not event_id or len(event_id) > 255
                ))
            ):
                return http.Response(status=400)

            # The singleton webhook configuration determines company scope, not
            # the anonymous caller's session or selected companies.
            bot = request.env.ref("odoo_square.user_square_bot", raise_if_not_found=False)
            if not bot or not bot.sudo().active or config.company_id not in bot.sudo().company_ids:
                return http.Response(status=503)
            request.update_env(user=bot.id, context={"allowed_company_ids": config.company_id.ids}, su=True)
            with request.env.cr.savepoint():
                request.env["square.integration.log"]._log_webhook_received(
                    event_type, event_id
                )
                result = request.env["square.webhook.event"]._apply_provider_event(
                    config, event_id,
                    lambda: self._process_event(
                        event_type, event_data, event_id, square_config=config, signed=True,
                    ),
                )

            return http.Response(
                json.dumps(result),
                content_type="application/json",
                status=503 if result.get("status") == "retry" else 200,
            )

        except ValidationError as e:
            _logger.warning(f"Square webhook validation error: {str(e)}")
            return http.Response(
                json.dumps({"status": "error", "message": str(e)}),
                content_type="application/json",
                status=400,
            )
        except AccessError as e:
            _logger.error(f"Square webhook access error: {str(e)}")
            return http.Response(
                json.dumps({"status": "error", "message": "Access denied"}),
                content_type="application/json",
                status=403,
            )
        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(f"Square webhook error: {str(e)}", exc_info=True)

            return http.Response(
                json.dumps({"status": "error", "message": "Internal server error"}),
                content_type="application/json",
                status=500,
            )

    def _validate_webhook_signature(
        self, request_body, signature_header, notification_url, square_config
    ):
        """
        Validate Square webhook signature manually (based on Square SDK implementation)

        Args:
            request_body: The JSON body of the request
            signature_header: The value for the x-square-hmacsha256-signature header
            notification_url: The notification endpoint URL

        Returns:
            bool: True if signature is valid, False otherwise
        """
        try:
            signature_key = square_config.square_webhook_signature_key if square_config else None
            if (
                not signature_key or not signature_key.isascii()
                or not signature_key.isprintable() or any(c.isspace() for c in signature_key)
                or not signature_header or not request_body or not notification_url
            ):
                return False

            payload_bytes = notification_url.encode("utf-8") + request_body
            signature_header_bytes = signature_header.encode("ascii")
            signature_key_bytes = signature_key.encode("ascii")

            # Compute the hash value
            hashing_obj = hmac.new(
                key=signature_key_bytes, msg=payload_bytes, digestmod=hashlib.sha256
            )
            hash_bytes = hashing_obj.digest()

            # Compare the computed hash vs the value in the signature header
            hash_base64 = base64.b64encode(hash_bytes)
            is_valid = hmac.compare_digest(hash_base64, signature_header_bytes)
            return is_valid

        except Exception as e:
            _logger.error(f"Error validating webhook signature: {str(e)}")
            return False

    def _process_event(self, event_type, event_data, webhook_event_id=None, *, square_config=None, signed=False):
        """
        Unified event processor for all Square webhook events
        """
        try:
            # Extract event data based on type
            if event_type == "order.created":
                order_data = event_data.get("object", {}).get("order_created", {})
                return request.env["square.order.processor"]._process_webhook_order(
                    order_data, "created", webhook_event_id, square_config=square_config, signed=signed,
                )

            elif event_type == "order.updated":
                order_data = event_data.get("object", {}).get("order_updated", {})
                return request.env["square.order.processor"]._process_webhook_order(
                    order_data, "updated", webhook_event_id, square_config=square_config, signed=signed,
                )

            elif event_type == "payment.updated":
                payment_data = event_data.get("object", {}).get("payment", {})
                return request.env["square.order.processor"]._process_payment(payment_data, "updated", webhook_event_id)

            elif event_type == "refund.created":
                refund_data = event_data.get("object", {}).get("refund", {})
                return self._process_refund(
                    refund_data, "created", webhook_event_id, square_config=square_config, signed=signed,
                )

            elif event_type == "refund.updated":
                refund_data = event_data.get("object", {}).get("refund", {})
                return self._process_refund(
                    refund_data, "updated", webhook_event_id, square_config=square_config, signed=signed,
                )

            else:
                _logger.info(f"Square webhook: Unhandled event type: {event_type}")
                return {
                    "status": "ignored",
                    "message": f"Event type {event_type} not handled",
                }

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error processing {event_type} event: {str(e)}", exc_info=True
            )
            return {"status": "error", "message": str(e)}

    def _process_refund(
        self, refund_data, event_type, webhook_event_id=None, env=None, *, square_config=None, signed,
    ):
        """Process refund data from webhook using the new refund model.

        ``env`` allows unit tests to call this without a bound HTTP ``request``.
        """
        req_env = env if env is not None else request.env
        if not signed:
            raise AccessError("Square refunds require a verified Square notification.")
        try:
            if not refund_data or not isinstance(refund_data.get("id"), str) or not refund_data["id"]:
                _logger.warning(f"No refund data in {event_type} webhook")
                return {"status": "ignored", "message": "No refund data"}

            # Extract refund info
            refund_id = refund_data.get("id")
            order_id = refund_data.get("order_id")
            refund_status = refund_data.get("status")

            _logger.info(
                f"Processing Square refund {refund_id} for order {order_id} with status {refund_status}"
            )

            api_client = req_env["square.api.client"].sudo()
            config = api_client._website_routing_config(square_config)
            routing = api_client._route_pos_refund(refund_data, config, signed=signed)
            if routing:
                return routing

            sales = req_env["sale.order"].sudo()

            def find(field, provider_id):
                # Refund order IDs can identify a return; its payment ID
                # identifies the original sale. Never search across companies.
                if not provider_id:
                    return sales.browse()
                return sales.search([
                    (field, "=", provider_id), ("company_id", "=", config.company_id.id),
                    ("square_order_id", "!=", False),
                ], limit=1)

            payment_id = refund_data.get("payment_id")
            sale_order = find("square_payment_id", payment_id) or find("square_order_id", order_id)
            if not sale_order:
                refund = api_client._read_routing_resource("refunds", "refund", refund_id, config)
                api_payment_id = refund.get("payment_id")
                sale_order = find("square_payment_id", api_payment_id)
                if api_payment_id and not sale_order:
                    payment = api_client._read_routing_resource("payments", "payment", api_payment_id, config)
                    sale_order = find("square_order_id", payment.get("order_id"))

            if not sale_order:
                _logger.warning(
                    f"Original order not found for refund {refund_id}. "
                    f"Searched by: order_id={order_id}, payment_id={refund_data.get('payment_id', 'N/A')}. "
                    f"This refund may be for an order created before webhook integration, "
                    f"from a different location, or processed by another system."
                )

                req_env["square.integration.log"]._log_square_event(
                    event_type="refund_created", status="warning",
                    title="Refund references an order that was not found",
                    square_order_id=order_id, square_refund_id=refund_id,
                    webhook_event_id=webhook_event_id,
                )

                return {
                    "status": "ignored",
                    "message": f"Refund {refund_id} for unknown order {order_id} - order not found in system",
                }

            _logger.info(
                "Linked Square refund %s to sale %s", refund_id, sale_order.id,
            )

            return req_env["square.refund"].sudo()._process_webhook_refund(
                refund_data, sale_order, event_type, webhook_event_id
            )

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error processing {event_type} refund: {str(e)}", exc_info=True
            )

            # Record a safe summary without copying the payload or exception.
            try:
                req_env["square.integration.log"].sudo()._log_error(
                    title=f"Refund {event_type} Processing Error",
                    error_message="The refund could not be applied.",
                    square_order_id=(
                        refund_data.get("order_id") if refund_data else None
                    ),
                )
            except:
                pass  # Don't let logging errors break the process

            return {"status": "error", "message": str(e)}

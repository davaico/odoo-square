# -*- coding: utf-8 -*-
import json
import logging
import base64
import hashlib
import hmac
from datetime import timedelta
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
            raw_body = request.httprequest.data.decode("utf-8")
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
            config = request.env["square.api.client"]._website_routing_config()
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
                data = json.loads(raw_body)
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

            if not data:
                _logger.error("Square webhook: No data received")
                return http.Response(
                    json.dumps({"status": "error", "message": "No data received"}),
                    content_type="application/json",
                    status=400,
                )

            # Extract event type and data
            event_type = data.get("type")
            event_data = data.get("data", {})

            # Log webhook reception
            request.env["square.integration.log"].sudo().log_webhook_received(
                event_type, event_data, data.get("event_id")
            )

            if not event_type:
                _logger.error("Square webhook: No event type specified")
                raise ValidationError("No event type specified in webhook data")

            # NOTE: Uncomment this to print a full JSON
            # _logger.debug(f"Square webhook data: {json.dumps(data, indent=4)}")

            # Process different event types using unified processor with transaction isolation
            with request.env.cr.savepoint():
                result = self._process_event(
                    event_type, event_data, data.get("event_id"), square_config=config,
                    signed=bool(config.square_webhook_signature_key),
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

            # Log error to integration dashboard
            try:
                request.env["square.integration.log"].sudo().log_error(
                    title="Webhook Processing Error",
                    error_message=str(e),
                    technical_details=f"Event type: {event_type if 'event_type' in locals() else 'Unknown'}\nError: {str(e)}",
                )
            except:
                pass  # Don't let logging errors break the webhook

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
            if not square_config or not square_config.square_webhook_signature_key:
                _logger.warning(
                    "Square webhook signature key not configured - skipping validation"
                )
                return True

            signature_key = square_config.square_webhook_signature_key

            # If signature key is configured but header is missing, reject
            if not signature_header:
                _logger.error(
                    "Signature header is missing but signature key is configured"
                )
                return False

            # Validate other inputs
            if not request_body:
                _logger.error("Request body is empty")
                return False

            if not notification_url:
                _logger.error("Notification URL is empty")
                return False

            # Perform UTF-8 encoding to bytes
            # https://github.com/square/square-python-sdk/blob/4e073b8df70daa190fcbf09e1b19423241b3e717/src/square/utils/webhooks_helper.py#L41
            payload = notification_url + request_body
            payload_bytes = payload.encode("utf-8")
            signature_header_bytes = signature_header.encode("utf-8")
            signature_key_bytes = signature_key.encode("utf-8")

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
                    order_data, "created", webhook_event_id, square_config=square_config,
                )

            elif event_type == "order.updated":
                order_data = event_data.get("object", {}).get("order_updated", {})
                return request.env["square.order.processor"]._process_webhook_order(
                    order_data, "updated", webhook_event_id, square_config=square_config,
                )

            elif event_type == "payment.updated":
                payment_data = event_data.get("object", {}).get("payment", {})
                return self._process_payment(payment_data, "updated", webhook_event_id)

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

    def _process_payment(self, payment_data, event_type, webhook_event_id=None):
        """
        Process payment.updated webhook to detect and handle exchanges
        """
        try:
            payment_id = payment_data.get("id")
            order_id = payment_data.get("order_id")
            payment_status = payment_data.get("status")

            _logger.info(
                f"Processing payment.updated webhook: payment_id={payment_id}, order_id={order_id}, status={payment_status}"
            )

            # Only process COMPLETED payments
            if payment_status != "COMPLETED":
                _logger.info(
                    f"Payment {payment_id} status is {payment_status}, not COMPLETED - skipping"
                )
                return {
                    "status": "ignored",
                    "message": f"Payment status {payment_status} not processed",
                }

            # Check for existing processing of this payment event
            if webhook_event_id:
                existing_log = (
                    request.env["square.integration.log"]
                    .sudo()
                    .search(
                        [
                            ("webhook_event_id", "=", webhook_event_id),
                            ("status", "=", "success"),
                        ],
                        limit=1,
                    )
                )

                if existing_log:
                    _logger.info(
                        f"Payment event {webhook_event_id} already processed successfully, skipping"
                    )
                    return {
                        "status": "already_processed",
                        "payment_id": payment_id,
                    }

            # Fetch full order details from Square API to check for exchanges
            api_client = request.env["square.api.client"].sudo()
            routing, order_data = api_client._read_pos_order(order_id)
            if routing:
                return routing

            if not order_data:
                _logger.error(
                    f"Failed to fetch order {order_id} from Square API for payment {payment_id}"
                )
                return {
                    "status": "error",
                    "message": f"Failed to fetch order {order_id} from Square API",
                }

            # Check if this is an exchange (has returns in line items)
            has_returns = self._detect_exchange(order_data)

            if has_returns:
                _logger.info(
                    f"Exchange detected for order {order_id}, processing exchange flow"
                )
                
                # Get the source_order_id from returns - this is the original order to update
                returns = order_data.get("returns", [])
                if not returns:
                    _logger.error(
                        f"Exchange detected but no returns array found in order {order_id}"
                    )
                    return {
                        "status": "error",
                        "message": f"Exchange detected but no returns array in order {order_id}",
                    }
                
                source_order_id = returns[0].get("source_order_id")
                if not source_order_id:
                    _logger.error(
                        f"No source_order_id found in returns for order {order_id}"
                    )
                    return {
                        "status": "error",
                        "message": f"No source_order_id in returns for order {order_id}",
                    }
                
                _logger.info(
                    f"Exchange: source_order_id={source_order_id}, current_order_id={order_id}"
                )
                
                # Process the exchange
                bot_user = None
                try:
                    bot_user = request.env.ref("odoo_square.user_square_bot")
                except ValueError:
                    bot_user = request.env.ref("base.user_admin")

                square_processor = (
                    request.env["square.order.processor"].sudo().with_user(bot_user)
                )

                # Find existing sale order using source_order_id (the original order)
                # Important: Load with bot_user context so it has a user throughout processing
                sale_order = (
                    request.env["sale.order"]
                    .sudo()
                    .with_user(bot_user)
                    .search([("square_order_id", "=", source_order_id)], limit=1)
                )

                if not sale_order:
                    _logger.error(
                        f"Sale order not found for Square source order {source_order_id} during exchange processing"
                    )
                    return {
                        "status": "error",
                        "message": f"Sale order not found for source order {source_order_id}",
                    }

                # Process the exchange
                result = square_processor.process_product_exchange(
                    sale_order, order_data, payment_data
                )

                # Log successful processing
                if webhook_event_id and result.get("status") == "success":
                    request.env["square.integration.log"].sudo().with_user(
                        bot_user
                    ).create(
                        {
                            "webhook_event_id": webhook_event_id,
                            "square_order_id": source_order_id,
                            "status": "success",
                            "event_type": "order_updated",
                            "title": f"Exchange processed for order {source_order_id}",
                            "description": f"Payment event {webhook_event_id} processed successfully for exchange on source order {source_order_id} (current order: {order_id})",
                        }
                    )

                return result
            else:
                _logger.info(
                    f"No exchange detected for order {order_id}, payment processed normally"
                )
                return {
                    "status": "success",
                    "message": "Payment processed, no exchange detected",
                    "payment_id": payment_id,
                }

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error processing payment.updated event: {str(e)}", exc_info=True
            )
            return {"status": "error", "message": str(e)}

    def _detect_exchange(self, order_data):
        """
        Detect if an order contains returns (indicating an exchange)

        Args:
            order_data: Full order data from Square API

        Returns:
            bool: True if order contains returns, False otherwise
        """
        line_items = order_data.get("line_items", [])

        # Check if any line item has a returns field
        for line_item in line_items:
            if "return_line_items" in order_data or line_item.get("quantity_unit", {}).get(
                "measurement_unit", {}
            ).get("type") == "TYPE_RETURN":
                return True

        # Also check for explicit returns field at order level
        if order_data.get("returns") or order_data.get("return_line_items"):
            return True

        return False

    def _process_refund(
        self, refund_data, event_type, webhook_event_id=None, env=None, *, square_config=None, signed=False,
    ):
        """Process refund data from webhook using the new refund model.

        ``env`` allows unit tests to call this without a bound HTTP ``request``.
        """
        req_env = env if env is not None else request.env
        try:
            if not refund_data:
                _logger.warning(f"No refund data in {event_type} webhook")
                return {"status": "ignored", "message": "No refund data"}

            # Extract refund info
            refund_id = refund_data.get("id")
            order_id = refund_data.get("order_id")
            refund_status = refund_data.get("status")

            _logger.info(
                f"Processing Square refund {refund_id} for order {order_id} with status {refund_status}"
            )

            # Multi-layer idempotency check for refunds
            # First check by webhook event ID (most specific)
            if webhook_event_id:
                existing_log = (
                    req_env["square.integration.log"]
                    .sudo()
                    .search(
                        [
                            ("webhook_event_id", "=", webhook_event_id),
                            ("status", "=", "success"),
                        ],
                        limit=1,
                    )
                )

                if existing_log:
                    _logger.info(
                        f"Webhook event {webhook_event_id} for refund {refund_id} already processed successfully, skipping"
                    )
                    return {
                        "status": "already_processed",
                        "message": f"Refund webhook event {webhook_event_id} already processed",
                        "refund_id": refund_id,
                    }

            api_client = req_env["square.api.client"].sudo()
            config = api_client._website_routing_config(square_config)
            routing = api_client._route_website_refund(refund_data, config, signed=signed)
            if routing:
                return routing

            if not order_id:
                _logger.error(f"No order ID in refund {event_type} webhook")
                return {"status": "error", "message": "No order ID in refund"}

            # Find the original sale order - try multiple strategies
            sale_order = None
            search_strategy = "order_id"

            # Strategy 1: Search by order_id (most common case)
            sale_order = (
                req_env["sale.order"]
                .sudo()
                .search([("square_order_id", "=", order_id)], limit=1)
            )

            # Strategy 2: If not found, try to find by payment_id if available
            if not sale_order and refund_data.get("payment_id"):
                payment_id = refund_data.get("payment_id")
                sale_order = (
                    req_env["sale.order"]
                    .sudo()
                    .search([("square_payment_id", "=", payment_id)], limit=1)
                )
                if sale_order:
                    search_strategy = "payment_id"
                    _logger.info(
                        f"Found order {sale_order.name} by payment_id {payment_id} for refund {refund_id}"
                    )

            # Strategy 3: If still not found, try to fetch refund details from Square API
            if not sale_order:
                try:
                    refund_details = self._fetch_refund_details_from_square(
                        refund_id, env=req_env
                    )
                    if refund_details:
                        # Try to find order by payment_id from refund details
                        payment_id = refund_details.get("payment_id")
                        if payment_id:
                            sale_order = (
                                req_env["sale.order"]
                                .sudo()
                                .search(
                                    [("square_payment_id", "=", payment_id)], limit=1
                                )
                            )
                            if sale_order:
                                search_strategy = "square_api_payment_id"
                                _logger.info(
                                    f"Found order {sale_order.name} by Square API payment_id {payment_id} for refund {refund_id}"
                                )

                        # If still not found, try order_id from refund details
                        if not sale_order:
                            api_order_id = refund_details.get("order_id")
                            if api_order_id and api_order_id != order_id:
                                sale_order = (
                                    req_env["sale.order"]
                                    .sudo()
                                    .search(
                                        [("square_order_id", "=", api_order_id)],
                                        limit=1,
                                    )
                                )
                                if sale_order:
                                    search_strategy = "square_api_order_id"
                                    _logger.info(
                                        f"Found order {sale_order.name} by Square API order_id {api_order_id} for refund {refund_id}"
                                    )
                except Exception as e:
                    _logger.warning(
                        f"Failed to fetch refund details from Square API: {str(e)}"
                    )

            if not sale_order:
                _logger.warning(
                    f"Original order not found for refund {refund_id}. "
                    f"Searched by: order_id={order_id}, payment_id={refund_data.get('payment_id', 'N/A')}. "
                    f"This refund may be for an order created before webhook integration, "
                    f"from a different location, or processed by another system."
                )

                # Create a log entry for tracking purposes
                req_env["square.integration.log"].sudo().create(
                    {
                        "event_type": "refund_created",
                        "status": "error",
                        "title": f"Refund for unknown order: {refund_id}",
                        "square_order_id": order_id,
                        "square_refund_id": refund_id,
                        "description": f"Refund {refund_id} references order {order_id} which is not in the system. "
                        f"Payment ID: {refund_data.get('payment_id', 'N/A')}. "
                        f"This may be a refund for an order created before webhook integration.",
                        "technical_details": json.dumps(
                            {
                                "refund_data": refund_data,
                                "search_strategies_tried": [
                                    "order_id",
                                    "payment_id",
                                    "square_api",
                                ],
                                "reason": "Order not found in system",
                            }
                        ),
                        "webhook_event_id": webhook_event_id,
                    }
                )

                return {
                    "status": "ignored",
                    "message": f"Refund {refund_id} for unknown order {order_id} - order not found in system",
                }

            _logger.info(
                f"Successfully linked refund {refund_id} to order {sale_order.name} using strategy: {search_strategy}"
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

            # Log error
            try:
                req_env["square.integration.log"].sudo().log_error(
                    title=f"Refund {event_type} Processing Error",
                    error_message=str(e),
                    square_order_id=(
                        refund_data.get("order_id") if refund_data else None
                    ),
                    technical_details=f"Refund data: {refund_data}\nError: {str(e)}",
                )
            except:
                pass  # Don't let logging errors break the process

            return {"status": "error", "message": str(e)}

    def _fetch_refund_details_from_square(self, refund_id, env=None):
        """
        Fetch detailed refund information from Square API
        This helps us find the correct order when webhook data is incomplete
        """
        try:
            req_env = env if env is not None else request.env
            square_api = req_env["square.api.client"]

            # Try to get payment details first (refunds are associated with payments)
            payment_data = square_api.get_payment(refund_id)

            if payment_data:
                # Extract refund information from payment
                refund_info = {
                    "id": refund_id,
                    "payment_id": payment_data.get("id"),
                    "order_id": payment_data.get("order_id"),
                    "amount_money": payment_data.get("amount_money"),
                    "status": payment_data.get("status"),
                    "refund_ids": payment_data.get("refund_ids", []),
                }

                _logger.info(
                    f"Successfully fetched payment details for {refund_id} from Square API"
                )
                return refund_info

            # If payment not found, try to get refund directly
            # This is a fallback for cases where refund_id might be different from payment_id
            _logger.debug(f"Payment {refund_id} not found, might be a direct refund ID")

            # For now, return None if payment not found
            # In the future, we could add a direct refund endpoint if needed
            return None

        except Exception as e:
            _logger.error(
                f"Error fetching payment/refund details from Square API: {str(e)}"
            )
            return None

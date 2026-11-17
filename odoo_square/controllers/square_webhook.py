# -*- coding: utf-8 -*-
import json
import logging
import base64
import hashlib
import hmac
from datetime import timedelta
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY
from odoo import http, fields
from odoo.http import request
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
        authenticated = False
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

            config = request.env["square.config"].sudo().search([], limit=1)
            # Validate webhook signature
            if not self._validate_webhook_signature(
                raw_body, square_signature, notification_url, config
            ):
                _logger.error("Square webhook: Invalid signature")
                return http.Response(
                    json.dumps({"status": "error", "message": "Invalid signature"}),
                    content_type="application/json",
                    status=403,
                )

            authenticated = True
            request.update_context(allowed_company_ids=config.company_id.ids)

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
            request.env["square.integration.log"].sudo()._log_webhook_received(
                event_type, event_data, data.get("event_id")
            )

            if not event_type:
                _logger.error("Square webhook: No event type specified")
                raise ValidationError("No event type specified in webhook data")

            # Process different event types using unified processor with transaction isolation
            with request.env.cr.savepoint():
                result = self._process_event(
                    event_type, event_data, data.get("event_id"),
                    merchant_id=data.get("merchant_id"),
                    environment=config.square_environment,
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
            if authenticated:
                request.env["square.integration.log"].sudo()._log_error(
                    title="Webhook Processing Error",
                    error_message=str(e),
                    technical_details=f"Event type: {event_type if 'event_type' in locals() else 'Unknown'}\nError: {str(e)}",
                )

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
                    "Square webhook signature key not configured"
                )
                return False

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
            payload_bytes = notification_url.encode("utf-8") + request_body
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

    def _process_event(
        self, event_type, event_data, webhook_event_id=None, *, merchant_id=None, environment=None,
    ):
        """
        Unified event processor for all Square webhook events
        """
        try:
            # Extract event data based on type
            if event_type == "order.created":
                order_data = event_data.get("object", {}).get("order_created", {})
                return request.env["square.order.processor"]._process_order_event(
                    order_data, "created", webhook_event_id, merchant_id=merchant_id,
                    environment=environment,
                )

            elif event_type == "order.updated":
                order_data = event_data.get("object", {}).get("order_updated", {})
                return request.env["square.order.processor"]._process_order_event(
                    order_data, "updated", webhook_event_id, merchant_id=merchant_id,
                    environment=environment,
                )

            elif event_type == "payment.updated":
                payment_data = event_data.get("object", {}).get("payment", {})
                return self._process_payment(
                    payment_data, "updated", webhook_event_id,
                    merchant_id=merchant_id, environment=environment,
                )

            elif event_type in ("refund.created", "refund.updated"):
                refund_data = event_data.get("object", {}).get("refund", {})
                if not refund_data:
                    _logger.warning("No refund data in %s webhook", event_type)
                    return {"status": "ignored", "message": "No refund data"}
                return request.env["square.refund"]._process_refund_event(
                    refund_data, event_type.split(".")[1], webhook_event_id,
                    merchant_id=merchant_id, environment=environment,
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

    def _process_payment(
        self, payment_data, event_type, webhook_event_id=None, *, merchant_id=None, environment=None,
    ):
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

            processor = request.env["square.order.processor"]
            routing = processor._square_online_event_result(
                order_id=order_id, payment_id=payment_id, location_id=payment_data.get("location_id"),
                merchant_id=merchant_id, environment=environment,
            )
            if routing:
                return routing

            receipts = request.env["square.event.receipt"].sudo()
            if receipts._was_applied("payment.updated", payment_id, webhook_event_id):
                return {"status": "already_processed", "payment_id": payment_id}

            # Fetch full order details from Square API to check for exchanges
            api_client = request.env["square.api.client"].sudo()
            order_data = api_client.get_order(order_id)

            if not order_data or (order_data.get("id") or order_data.get("order_id")) != order_id:
                _logger.error(
                    f"Failed to fetch order {order_id} from Square API for payment {payment_id}"
                )
                return {
                    "status": "error",
                    "message": f"Failed to fetch order {order_id} from Square API",
                }

            has_returns, routing = processor._check_square_exchange_order(
                order_id, order_data, merchant_id=merchant_id, environment=environment,
            )
            if routing:
                return routing

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
                    .search([("square_order_id", "=", source_order_id), ("company_id", "=", receipts.env.company.id)], limit=1)
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
                result = square_processor._process_product_exchange(
                    sale_order, order_data, payment_data
                )

                # Log successful processing
                if result.get("status") == "success":
                    receipts._record_applied("payment.updated", payment_id, webhook_event_id)
                    request.env["square.integration.log"].sudo().with_user(
                        bot_user
                    ).create(
                        {
                            "company_id": receipts.env.company.id,
                            "webhook_event_id": webhook_event_id,
                            "square_order_id": source_order_id,
                            "status": "success",
                            "event_type": "order_updated",
                            "title": f"Exchange processed for order {source_order_id}",
                            "description": f"Payment event {webhook_event_id} processed successfully for exchange on source order {source_order_id} (current order: {order_id})",
                        }
                    )

                return result
            processor = processor.with_user(processor.sudo()._get_square_bot_user())
            sale_order = processor._check_existing_order(order_id)
            if sale_order and processor._settle_existing_order_invoice(
                sale_order, payment_data, square_order=order_data,
            ):
                receipts._record_applied("payment.updated", payment_id, webhook_event_id)
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

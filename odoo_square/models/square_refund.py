# -*- coding: utf-8 -*-
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY
from odoo import Command, models, fields, api
from odoo.exceptions import ValidationError, UserError, AccessError
from collections import defaultdict
from datetime import timedelta
import json
import logging

_logger = logging.getLogger(__name__)


class RefundProcessingDeferred(UserError):
    """The refund can be completed once its return can be resolved."""


class SquareRefund(models.Model):
    _name = "square.refund"
    _description = "Square Refund Tracking"
    _order = "create_date desc"
    _rec_name = "display_name"

    display_name = fields.Char(
        string="Display Name", compute="_compute_display_name", store=True
    )

    # Square Data
    square_refund_id = fields.Char(
        string="Square Refund ID", required=True, index=True, copy=False
    )
    square_order_id = fields.Char(string="Square Order ID", required=True, index=True)

    # Status Tracking
    status = fields.Selection(
        [
            ("pending", "Pending"),
            ("completed", "Completed"),
            ("failed", "Failed"),
            ("cancelled", "Cancelled"),
        ],
        string="Refund Status",
        required=True,
        default="pending",
    )

    # Financial Information
    refund_amount = fields.Monetary(
        string="Refund Amount", required=True, currency_field="currency_id"
    )
    currency_id = fields.Many2one(
        "res.currency",
        string="Currency",
        required=True,
        default=lambda self: self.env["res.currency"].search(
            [("name", "=", "EUR")], limit=1
        ),
        domain=[("name", "=", "EUR")],  # Only allow EUR
    )

    # Odoo Relations
    sale_order_id = fields.Many2one(
        "sale.order", string="Sale Order", required=True, ondelete="cascade"
    )
    invoice_id = fields.Many2one(
        "account.move", string="Invoice", domain=[("move_type", "=", "out_invoice")]
    )
    credit_note_id = fields.Many2one(
        "account.move", string="Credit Note", domain=[("move_type", "=", "out_refund")]
    )
    return_picking_ids = fields.Many2many(
        "stock.picking",
        string="Return Pickings",
        domain=[("picking_type_code", "=", "incoming")],
    )

    # Refund Details
    refund_reason = fields.Text(string="Refund Reason")
    refunded_line_ids = fields.Json(
        string="Refunded Line Items",
        help="JSON data containing specific line items being refunded",
    )

    # Processing Information
    processed_at = fields.Datetime(string="Processed At")
    processing_errors = fields.Text(string="Processing Errors")

    # Metadata
    square_data = fields.Json(string="Square Data", help="Raw Square webhook data")
    webhook_event_id = fields.Char(string="Webhook Event ID")

    _square_refund_id_unique = models.Constraint(
        "UNIQUE(square_refund_id)",
        "Square Refund ID must be unique.",
    )

    @api.model
    def _track_visibility(self):
        """Track visibility for refund status changes"""
        return {
            "status": {
                "square.refund": {
                    "group_system": True,
                    "group_user": True,
                }
            }
        }

    @api.depends("square_refund_id", "status", "refund_amount")
    def _compute_display_name(self):
        for record in self:
            if record.square_refund_id:
                record.display_name = f"Refund {record.square_refund_id} - {record.status.title()} - {record.refund_amount} {record.currency_id.name}"
            else:
                record.display_name = f"Refund - {record.status.title()}"

    @api.model
    def _process_refund_event(self, refund_data, event_type, webhook_event_id=None):
        """Apply a Square refund event and record the outcome for replay."""
        refund_id = refund_data.get("id")
        order_id = refund_data.get("order_id")
        refund_status = refund_data.get("status")

        try:
            _logger.info(
                f"Processing Square refund {refund_id} for order {order_id} with status {refund_status}"
            )

            # Multi-layer idempotency check for refunds
            # First check by webhook event ID (most specific)
            if webhook_event_id:
                existing_log = (
                    self.env["square.integration.log"]
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

            # Second check: look for existing refund record by Square refund ID
            # This handles the case where Square sends multiple webhooks for the same refund
            existing_refund = (
                self.sudo().search([("square_refund_id", "=", refund_id)], limit=1)
            )

            if existing_refund.status == "completed" and existing_refund.processed_at:
                return {
                    "status": "already_processed",
                    "message": f"Refund {refund_id} already fully processed",
                    "refund_id": refund_id,
                }

            # Find the original sale order - try multiple strategies
            sale_order = None
            search_strategy = "order_id"

            # Strategy 1: Search by order_id (most common case)
            sale_order = (
                self.env["sale.order"]
                .sudo()
                .search([("square_order_id", "=", order_id)], limit=1)
            )

            # Strategy 2: If not found, try to find by payment_id if available
            if not sale_order and refund_data.get("payment_id"):
                payment_id = refund_data.get("payment_id")
                sale_order = (
                    self.env["sale.order"]
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
                    refund_details = self._fetch_refund_details_from_square(refund_id)
                    if refund_details:
                        # Try to find order by payment_id from refund details
                        payment_id = refund_details.get("payment_id")
                        if payment_id:
                            sale_order = (
                                self.env["sale.order"]
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
                                    self.env["sale.order"]
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
                except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                    raise
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
                self.env["square.integration.log"].sudo().create(
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

            # Get or create refund record using bot user context
            try:
                refund_record = self.sudo().with_user(
                    self._get_bot_user()
                ).create_from_square_data(refund_data, sale_order)
            except ValidationError as ve:
                _logger.error(f"Validation error creating refund record: {str(ve)}")
                return {
                    "status": "error",
                    "message": f"Refund validation failed: {str(ve)}",
                }

            # Add webhook event ID to the refund data for tracking
            if webhook_event_id:
                refund_record.webhook_event_id = webhook_event_id

            status = refund_record._get_square_refund_status(refund_data)
            pending_prepared = (
                refund_record.credit_note_id
                and refund_record.credit_note_id.state != "cancel"
            )
            refund_record.status = status
            refund_record.action_process_refund()

            if status == "pending":
                result_message = (
                    "Actions de remboursement déjà préparées (en attente de confirmation Square)"
                    if pending_prepared
                    else "Actions de remboursement préparées (en attente de confirmation Square)"
                )
            else:
                result_message = {
                    "completed": "Refund processed successfully",
                    "failed": "Failed refund processed",
                    "cancelled": "Cancelled refund processed",
                }[status]

            # Log webhook processing
            self.env["square.integration.log"].sudo().log_square_event(
                event_type=f"refund_{event_type}",
                title=f"Remboursement {event_type} traité pour la commande {sale_order.name}",
                description=f"""
                    <p><strong>Square Refund {event_type.title()} Traité</strong></p>
                    <ul>
                        <li>Odoo Order: <strong>{sale_order.name}</strong></li>
                        <li>ID Square Refund : <code>{refund_id}</code></li>
                        <li>Statut : {status}</li>
                        <li>Montant : {refund_record.refund_amount} {refund_record.currency_id.name}</li>
                        <li>Type : {'Partiel' if refund_record._is_partial_refund() else 'Complet'}</li>
                        <li>Action : {result_message}</li>
                    </ul>
                """,
                status="success",
                square_order_id=order_id,
                square_refund_id=refund_id,
                sale_order_id=sale_order.id,
                webhook_event_id=webhook_event_id,
            )

            return {
                "status": "success",
                "message": result_message,
                "refund_id": refund_id,
                "refund_record_id": refund_record.id,
            }

        except RefundProcessingDeferred as e:
            refund_record.processing_errors = str(e)
            _logger.warning("Deferring Square refund %s: %s", refund_id, e)
            return {"status": "retry", "message": str(e), "refund_id": refund_id}

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error processing {event_type} refund: {str(e)}", exc_info=True
            )

            # Log error
            try:
                self.env["square.integration.log"].sudo().log_error(
                    title=f"Refund {event_type} Processing Error",
                    error_message=str(e),
                    square_order_id=(
                        refund_data.get("order_id") if refund_data else None
                    ),
                    technical_details=f"Refund data: {refund_data}\nError: {str(e)}",
                )
            except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                raise
            except Exception:
                pass  # Keep the original processing error if logging fails.

            return {"status": "error", "message": str(e)}

    def _fetch_refund_details_from_square(self, refund_id):
        """
        Fetch detailed refund information from Square API
        This helps us find the correct order when webhook data is incomplete
        """
        try:
            square_api = self.env["square.api.client"]

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

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(
                f"Error fetching payment/refund details from Square API: {str(e)}"
            )
            return None

    @api.model
    def _get_eur_currency(self):
        cur = self.env.ref("base.EUR", raise_if_not_found=False)
        if not cur or not cur.exists():
            cur = self.env["res.currency"].with_context(active_test=False).search(
                [("name", "=", "EUR")], limit=1
            )
        return cur

    @api.model
    def _check_existing_refund(self, square_refund_id):
        """
        Check if a refund with the given Square refund ID already exists.
        This is the centralized duplicate detection method with database locking.
        """
        if not square_refund_id:
            return None

        # Use SELECT FOR UPDATE to prevent race conditions during concurrent webhook processing
        # This ensures atomicity when checking for duplicates
        try:
            self.env.cr.execute(
                """
                SELECT id FROM square_refund
                WHERE square_refund_id = %s
                FOR UPDATE NOWAIT
                """,
                (square_refund_id,),
            )
            result = self.env.cr.fetchone()

            if result:
                existing_refund = self.env["square.refund"].browse(result[0])
                _logger.debug(
                    f"Found existing refund for Square ID {square_refund_id}: {existing_refund.display_name} (status: {existing_refund.status})"
                )
                return existing_refund

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            # If we can't acquire the lock immediately, another process is creating this refund
            # Fall back to regular search which might find the refund if it was just created
            _logger.warning(
                f"Could not acquire lock for Square refund {square_refund_id}, checking normally: {str(e)}"
            )
            existing_refund = self.env["square.refund"].search(
                [("square_refund_id", "=", square_refund_id)], limit=1
            )

            if existing_refund:
                _logger.debug(
                    f"Found existing refund for Square ID {square_refund_id}: {existing_refund.display_name} (status: {existing_refund.status})"
                )
                return existing_refund

        return None

    def _update_existing_refund(self, existing_refund, refund_data, sale_order):
        """
        Update an existing refund record with new Square webhook data
        """
        if existing_refund.sale_order_id != sale_order:
            raise ValidationError("Square refund is already linked to a different sale order")
        # Extract updated data
        amount_money = refund_data.get("amount_money", {})
        refund_amount = float(amount_money.get("amount", 0)) / 100.0

        currency = self._get_eur_currency()
        if not currency:
            raise ValidationError("EUR currency not found in the system")

        # Update the existing record
        vals = {
            "status": self._get_square_refund_status(refund_data),
            "refund_amount": refund_amount,
            "currency_id": currency.id,
            "sale_order_id": sale_order.id,
            "refund_reason": refund_data.get("reason"),
            "refunded_line_ids": refund_data.get(
                "refunded_line_ids", existing_refund.refunded_line_ids
            ),
            "square_data": refund_data,
        }

        details_changed = (
            currency.compare_amounts(existing_refund.refund_amount, refund_amount) != 0
            or set(existing_refund.refunded_line_ids or ()) != set(vals["refunded_line_ids"] or ())
        )
        existing_refund.write(vals)
        if details_changed:
            # These are provisional reservations owned by this refund. Keep
            # completed movements and canceled picking links as audit history.
            square_bot = self._get_bot_user()
            for picking in existing_refund.return_picking_ids.filtered(
                lambda picking: picking.state not in ("done", "cancel")
                and picking.create_uid == square_bot
            ):
                open_moves = picking.move_ids.filtered(
                    lambda move: move.state not in ("done", "cancel")
                )
                if picking.move_ids.filtered(lambda move: move.state == "done"):
                    open_moves._action_cancel()
                else:
                    picking.action_cancel()
        _logger.info(f"Updated existing refund record: {existing_refund.display_name}")
        return existing_refund

    @api.model
    def _get_square_refund_status(self, refund_data):
        status = (refund_data.get("status") or "PENDING").strip().lower()
        return "cancelled" if status == "canceled" else status

    def create_from_square_data(self, refund_data, sale_order):
        """
        Create a refund record from Square webhook data
        """
        if not refund_data:
            raise ValidationError("Refund data is required")

        square_refund_id = refund_data.get("id")
        if not square_refund_id:
            raise ValidationError("Square refund ID is required")

        if not sale_order:
            raise ValidationError("Sale order is required for refund processing")

        amount_money = refund_data.get("amount_money", {})
        currency_code = amount_money.get("currency", "EUR")
        if currency_code != "EUR":
            raise ValidationError(
                f"Unsupported currency '{currency_code}'. This system only supports EUR refunds."
            )

        # Robust idempotency check with database locking (similar to order processing)
        existing_refund = self._check_existing_refund(square_refund_id)
        if existing_refund:
            if existing_refund.processed_at:
                return existing_refund
            return self._update_existing_refund(existing_refund, refund_data, sale_order)

        # Extract refund amount
        refund_amount = float(amount_money.get("amount", 0)) / 100.0

        currency = self._get_eur_currency()

        if not currency:
            raise ValidationError(
                "EUR currency not found in the system. Please ensure EUR is properly configured."
            )

        vals = {
            "square_refund_id": square_refund_id,
            "square_order_id": refund_data.get("order_id"),
            "status": self._get_square_refund_status(refund_data),
            "refund_amount": refund_amount,
            "currency_id": currency.id,
            "sale_order_id": sale_order.id,
            "refund_reason": refund_data.get("reason"),
            "refunded_line_ids": refund_data.get("refunded_line_ids", []),
            "square_data": refund_data,
            "webhook_event_id": refund_data.get("webhook_event_id"),
        }

        # Get bot user for creation to ensure proper tracking messages
        bot_user = self._get_bot_user()
        # Attempt creation with defensive duplicate handling. In high concurrency, the record
        # may have been created by another transaction between the duplicate check and commit.
        with self.env.cr.savepoint():
            try:
                return self.with_user(bot_user).create(vals)
            except Exception as e:
                # Unique constraint violation - fetch existing and (optionally) update
                err = str(e)
                if "square_refund_id_unique" in err or "duplicate key value" in err:
                    _logger.warning(
                        f"Duplicate detected during refund creation for {square_refund_id}. Falling back to update logic inside savepoint. Error: {err}"
                    )
                    # At this point the savepoint will rollback only this failing CREATE, allowing further queries
                    existing = self.search(
                        [("square_refund_id", "=", square_refund_id)], limit=1
                    )
                    if existing:
                        incoming_status = vals.get("status")
                        current_status = existing.status
                        status_priority = {
                            "pending": 1,
                            "failed": 0,
                            "cancelled": 0,
                            "completed": 2,
                        }
                        if status_priority.get(incoming_status, 0) > status_priority.get(
                            current_status, 0
                        ):
                            update_vals = vals.copy()
                        else:
                            update_vals = vals.copy()
                            update_vals["status"] = current_status
                        existing.with_user(bot_user).write(update_vals)
                        _logger.info(
                            f"Refund {square_refund_id} already existed; applied safe upsert (status {current_status} -> {existing.status})."
                        )
                        return existing
                # Re-raise other errors not related to duplicate
                raise

    def _get_bot_user(self):
        """Get the Square integration bot user"""
        try:
            bot_user = self.env.ref("odoo_square.user_square_bot")
            return bot_user
        except ValueError:
            # Fallback to admin user if bot user doesn't exist
            _logger.warning("Square bot user not found, falling back to admin user")
            return self.env.ref("base.user_admin")

    def _add_adjustment_line(self, original_line, qty, bot_user):
        """Create or update a negative adjustment line for a refunded quantity.
        Ensures idempotency: if the adjustment line for this product already exists on the order,
        we extend it instead of creating a duplicate line.
        """
        if not original_line or qty <= 0:
            return None
        name = f"[RETOUR] {original_line.name}"
        existing = self.env["sale.order.line"].search(
            [
                ("order_id", "=", original_line.order_id.id),
                ("product_id", "=", original_line.product_id.id),
                ("name", "=", name),
            ],
            limit=1,
        )
        if existing:
            # Merge by subtracting additional qty
            new_qty = existing.product_uom_qty - qty  # existing is already negative
            existing.with_user(bot_user).write({"product_uom_qty": new_qty})
            _logger.info(
                f"Merged refund qty {qty} into existing adjustment line {existing.id} for product {original_line.product_id.name}; new total qty {new_qty}"
            )
            return existing
        vals = {
            "order_id": original_line.order_id.id,
            "product_id": original_line.product_id.id,
            "product_uom_qty": -qty,
            "product_uom_id": original_line.product_uom_id.id,
            "price_unit": original_line.price_unit,
            "name": name,
            "sequence": original_line.sequence + 1,
        }
        created = self.env["sale.order.line"].with_user(bot_user).create(vals)
        _logger.info(
            f"Created new adjustment line {created.id} for product {original_line.product_id.name}: qty -{qty}"
        )
        return created

    def action_process_refund(self):
        """
        Process the refund based on current status
        """
        self.ensure_one()

        if self.status == "pending":
            # Create return pickings and credit note but don't complete yet
            return self._create_pending_refund_actions()

        elif self.status == "completed":
            # Complete the refund process
            return self._complete_refund()

        elif self.status == "failed":
            # Handle failed refund
            return self._handle_failed_refund()

        elif self.status == "cancelled":
            # Handle cancelled refund
            return self._handle_cancelled_refund()

    def _create_pending_refund_actions(self):
        """Prepare the return pickings and draft credit note."""
        has_credit_note = self.credit_note_id and self.credit_note_id.state != "cancel"
        if has_credit_note and self.status == "pending":
            return True

        try:
            with self.env.cr.savepoint():
                sale_order = self.sale_order_id
                is_partial = self._is_partial_refund()
                if is_partial:
                    self._create_partial_return_pickings()
                else:
                    self._create_full_return_picking()

                if has_credit_note:
                    return True

                self.credit_note_id = self._create_credit_note(is_partial)
                self.invoice_id = self.credit_note_id.reversed_entry_id

                self.env["square.integration.log"].log_square_event(
                    event_type="refund_processed",
                    title=f"Actions de remboursement en attente créées pour {sale_order.name}",
                    description=f"""
                        <p><strong>Actions de Remboursement en Attente Créées</strong></p>
                        <ul>
                            <li>Commande Odoo : <strong>{sale_order.name}</strong></li>
                            <li>ID Remboursement Square : <code>{self.square_refund_id}</code></li>
                            <li>Montant : {self.refund_amount} {self.currency_id.name}</li>
                            <li>Type : {"Partiel" if is_partial else "Complet"}</li>
                            <li>Statut : En attente de confirmation Square</li>
                            <li>Avoir : Créé (non validé)</li>
                            <li>Retour Stock : Préparé</li>
                        </ul>
                    """,
                    status="info",
                    square_order_id=self.square_order_id,
                    square_refund_id=self.square_refund_id,
                    sale_order_id=sale_order.id,
                )
        except RefundProcessingDeferred:
            raise
        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error("Error creating pending refund actions: %s", e)
            self.processing_errors = str(e)
            raise UserError(
                f"Erreur lors de la création des actions de remboursement : {e}"
            ) from e
        return True

    def _complete_refund(self):
        """Post and settle the credit note, then complete the stock return."""
        if self.processed_at:
            return True

        bot_user = self._get_bot_user()
        try:
            with self.env.cr.savepoint():
                self._create_pending_refund_actions()
                credit_note = self.credit_note_id
                currency = credit_note.currency_id
                if credit_note.state == "draft":
                    # A provisional partial refund can become a full refund.
                    # Keep rejecting full amounts above the original invoice.
                    if self._is_partial_refund() or currency.compare_amounts(
                        self.refund_amount, credit_note.reversed_entry_id.amount_total
                    ) == 0:
                        self._adjust_credit_note_for_partial_refund(credit_note)
                    credit_note.with_user(bot_user).action_post()

                if currency.compare_amounts(credit_note.amount_total, self.refund_amount) != 0:
                    raise UserError("Le montant de l'avoir ne correspond pas au remboursement Square")

                processor = self.env["square.order.processor"]
                if not currency.is_zero(credit_note.amount_residual):
                    if not processor._create_payment_for_credit_note(
                        credit_note, self.sale_order_id, bot_user, refund_amount=self.refund_amount
                    ) or not currency.is_zero(credit_note.amount_residual):
                        raise UserError("Impossible de comptabiliser le paiement du remboursement Square")

                for picking in self.return_picking_ids.filtered(
                    lambda picking: picking.state not in ("done", "cancel")
                ):
                    picking.with_user(bot_user).with_context(
                        force_validate=True,
                        mail_auto_subscribe_no_notify=True,
                        mail_create_nosubscribe=True,
                    ).button_validate()
                    if picking.state != "done":
                        raise UserError(f"Impossible de valider le retour {picking.name}")

                if self._is_partial_refund():
                    self._update_sale_order_quantities_after_refund()
                else:
                    lines = self.sale_order_id.order_line.filtered(
                        lambda line: line.product_uom_qty > line.returned_qty
                    )
                    for line in lines:
                        quantity = line.update_returned_quantity(
                            line.product_uom_qty - line.returned_qty
                        )
                        self._add_adjustment_line(line, quantity, bot_user)

                self.write({"processed_at": fields.Datetime.now(), "processing_errors": False})

                self.sale_order_id.with_user(bot_user).with_context(
                    mail_auto_subscribe_no_notify=True, mail_create_nosubscribe=True
                ).message_post(
                    body=(
                        f"Avoir Square {credit_note.name} : "
                        f"{self.refund_amount} {self.currency_id.name}"
                    ),
                    subject="Intégration Square : Remboursement finalisé",
                    message_type="comment",
                )

                refund_data = {
                    "id": self.square_refund_id,
                    "order_id": self.square_order_id,
                    "amount_money": {
                        "amount": round(self.refund_amount * 100),
                        "currency": self.currency_id.name,
                    },
                }

                self.env["square.integration.log"].log_refund_processed(
                    sale_order=self.sale_order_id, refund_data=refund_data
                )

                self.env["square.integration.log"].log_square_event(
                    event_type="refund_processed",
                    title=f"Remboursement finalisé pour {self.sale_order_id.name}",
                    description=f"""
                        <p><strong>Remboursement Square Finalisé</strong></p>
                        <ul>
                            <li>Commande Odoo : <strong>{self.sale_order_id.name}</strong></li>
                            <li>ID Remboursement Square : <code>{self.square_refund_id}</code></li>
                            <li>Montant : {self.refund_amount} {self.currency_id.name}</li>
                            <li>Statut : Finalisé</li>
                            <li>Avoir : Validé et comptabilisé</li>
                            <li>Stock : Retourné à l'inventaire</li>
                        </ul>
                    """,
                    status="success",
                    square_order_id=self.square_order_id,
                    square_refund_id=self.square_refund_id,
                    sale_order_id=self.sale_order_id.id,
                )
        except RefundProcessingDeferred:
            raise
        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error("Error completing refund: %s", e)
            self.processing_errors = str(e)
            raise UserError(f"Erreur lors de la finalisation du remboursement : {e}") from e
        return True

    def _is_partial_refund(self):
        return bool(self.refunded_line_ids) or (
            self.currency_id.compare_amounts(
                self.refund_amount, self.sale_order_id._get_original_sale_total()
            ) < 0
        )

    def _create_partial_return_pickings(self):
        """Allocate the requested quantities across the original deliveries."""
        sale_order = self.sale_order_id
        deliveries = sale_order.picking_ids.filtered(
            lambda picking: picking.picking_type_code == "outgoing" and picking.state == "done"
        )
        if not deliveries:
            _logger.info(
                f"No delivery picking found for sale order {sale_order.name}. "
                f"This appears to be a physical store order where items are handed directly to customers. "
                f"Skipping partial return picking creation for refund {self.square_refund_id}."
            )
            return

        remaining = self._get_partial_return_quantities()
        for move in self.return_picking_ids.move_ids.filtered(lambda move: move.state != "cancel"):
            sale_line = move.origin_returned_move_id.sale_line_id
            if sale_line in remaining:
                quantity = move.quantity if move.state == "done" else move.product_uom_qty
                remaining[sale_line] -= move.product_uom._compute_quantity(
                    quantity, sale_line.product_uom_id, round=False
                )

        wizard_model = self.env["stock.return.picking"].with_user(self._get_bot_user())
        for delivery in deliveries.sorted("id"):
            wizard = wizard_model.create({"picking_id": delivery.id})
            for line in wizard.product_return_moves:
                sale_line = line.move_id.sale_line_id
                requested = remaining.get(sale_line, 0)
                if requested <= 0:
                    continue

                line.quantity, remaining[sale_line] = line.move_id._allocate_square_return_quantity(
                    requested, sale_line.product_uom_id, line.uom_id
                )
                if line.quantity:
                    line.to_refund = True
                    _logger.info(
                        "Configured refund %s return for %s: %s on delivery %s",
                        self.square_refund_id, sale_line.product_id.name, line.quantity,
                        delivery.name,
                    )

            if any(wizard.product_return_moves.mapped("quantity")):
                action = wizard.action_create_returns()
                self.return_picking_ids = [(4, action["res_id"])]

        if self.status == "completed":
            other_returns = (
                deliveries.move_ids.returned_move_ids - self.return_picking_ids.move_ids
            ).filtered(
                lambda move: move.state not in ("done", "cancel") and move.product_uom_qty > 0
            )
            reserved_lines = other_returns.origin_returned_move_id.sale_line_id
            if any(
                line.product_uom_id.compare(remaining.get(line, 0), 0) > 0
                for line in reserved_lines
            ):
                raise RefundProcessingDeferred(
                    "Un autre retour réserve les articles de ce remboursement."
                )

    def _get_partial_return_quantities(self):
        """Get the requested quantities per sale line in its own unit."""
        remaining = {
            line: line.product_uom_qty - line.returned_qty
            for line in self.sale_order_id.order_line
            if line.product_uom_qty > line.returned_qty
        }
        if self.refunded_line_ids:
            order_details = self._fetch_order_details_from_square()
            square_line_items = order_details and order_details.get("line_items")
            if square_line_items:
                quantities = {}
                for line in remaining:
                    item = self._find_matching_square_line_item(line, square_line_items)
                    if item and self._should_return_line_item(item):
                        quantities[line] = self._calculate_return_quantity_for_line(line, item)
                return quantities

            requested_ids = set(self.refunded_line_ids)
            matching_lines = self.sale_order_id.order_line.filtered(
                lambda line: line.square_line_id in requested_ids
            )
            if not requested_ids.issubset(matching_lines.mapped("square_line_id")):
                raise RefundProcessingDeferred(
                    "Les articles du remboursement Square ne sont pas encore identifiables."
                )
            return {line: remaining[line] for line in matching_lines if line in remaining}

        total_order_amount = sum(
            quantity * line.price_total / line.product_uom_qty
            for line, quantity in remaining.items()
        )
        if total_order_amount <= 0:
            _logger.warning(
                f"Total order amount is zero for refund {self.square_refund_id}"
            )
            return {}

        refund_ratio = min(self.refund_amount / total_order_amount, 1)
        return {line: quantity * refund_ratio for line, quantity in remaining.items()}

    def _find_matching_square_line_item(self, odoo_line, square_line_items):
        """
        Find the matching Square line item for an Odoo order line
        """
        if odoo_line.square_line_id:
            return next(
                (item for item in square_line_items if item.get("uid") == odoo_line.square_line_id),
                None,
            )

        # Older sale lines may lack a Square line ID.
        if odoo_line.product_id.default_code:
            for square_item in square_line_items:
                # Match by catalog_object_id or item name containing SKU
                if (
                    square_item.get("catalog_object_id")
                    == odoo_line.product_id.default_code
                    or odoo_line.product_id.default_code in square_item.get("name", "")
                ):
                    return square_item

        # Fallback: match by product name similarity
        odoo_name = odoo_line.product_id.name.lower()
        for square_item in square_line_items:
            square_name = square_item.get("name", "").lower()
            if odoo_name in square_name or square_name in odoo_name:
                return square_item

        return None

    def _should_return_line_item(self, square_item):
        """
        Determine if a line item should be returned based on refund data
        """
        if not self.refunded_line_ids:
            return True  # Return all items if no specific line data

        # Check if this line item is in the refunded lines
        square_line_uid = square_item.get("uid")
        if square_line_uid and square_line_uid in self.refunded_line_ids:
            return True

        return False

    def _calculate_return_quantity_for_line(self, odoo_line, square_item):
        """
        Calculate the return quantity for a specific line item
        """
        # Get the original quantity from Square data
        original_quantity = float(square_item.get("quantity", "0"))

        # Square webhook line data does not provide already-refunded quantity; assume original as baseline.

        # For partial refunds, we need to determine how much of this line was refunded
        # This is a simplified approach - in practice, Square refund data should specify quantities
        if self.refunded_line_ids and square_item.get("uid") in self.refunded_line_ids:
            # If this line is specifically mentioned in refund, return the full remaining quantity
            remaining_quantity = odoo_line.product_uom_qty - odoo_line.returned_qty
            return min(remaining_quantity, original_quantity)

        return 0

    def _create_full_return_picking(self):
        """Reuse matching Odoo returns and prepare any remaining goods."""
        delivery_pickings = self.sale_order_id.picking_ids.filtered(
            lambda picking: picking.picking_type_code == "outgoing" and picking.state == "done"
        )
        return_moves = delivery_pickings.move_ids.returned_move_ids.filtered(
            lambda move: move.state != "cancel"
            and move.picking_id.return_id == move.origin_returned_move_id.picking_id
            and move.picking_id.picking_type_code == "incoming"
        )
        candidate_returns = return_moves.picking_id.filtered(
            lambda picking: not picking.square_exchange_order_id
        )
        other_refunds = self.search([
            ("id", "!=", self.id),
            ("return_picking_ids", "in", candidate_returns.ids),
        ])
        claimed_returns = other_refunds.filtered(
            lambda refund: refund.status in ("pending", "completed")
        ).return_picking_ids
        linked_returns = other_refunds.return_picking_ids
        square_bot = self.env.ref("odoo_square.user_square_bot", raise_if_not_found=False)
        # A refund link distinguishes a bot-created return from an older Square exchange.
        self.return_picking_ids |= (candidate_returns - claimed_returns).filtered(
            lambda picking: picking.state == "done"
            and (picking.create_uid != square_bot or picking in linked_returns)
        )
        if self.status == "completed":
            # Link open operator returns at completion so failure cannot cancel them.
            self.return_picking_ids |= (candidate_returns - linked_returns).filtered(
                lambda picking: picking.state not in ("done", "cancel")
                and picking.create_uid != square_bot
                and not (
                    picking.move_ids.filtered(lambda move: move.state != "cancel")
                    - return_moves
                )
            )

        if self.status == "completed":
            blocked_returns = (
                delivery_pickings.move_ids.returned_move_ids - self.return_picking_ids.move_ids
            ).filtered(
                lambda move: move.state not in ("done", "cancel") and move.product_uom_qty > 0
            )
            if blocked_returns:
                raise RefundProcessingDeferred(
                    "Un autre retour réserve les articles de ce remboursement."
                )

        wizard_model = self.env["stock.return.picking"].with_user(self._get_bot_user())
        for delivery in delivery_pickings:
            return_wizard = wizard_model.create({"picking_id": delivery.id})
            for line in return_wizard.product_return_moves:
                line.quantity = line.move_id._get_square_returnable_quantity()
            if any(return_wizard.product_return_moves.mapped("quantity")):
                action = return_wizard.action_create_returns()
                self.return_picking_ids = [(4, action["res_id"])]

    def _create_credit_note(self, is_partial):
        """Prepare a draft reversal of the original posted invoice."""
        invoice = self.sale_order_id.invoice_ids.filtered(
            lambda move: move.move_type == "out_invoice" and move.state == "posted"
        )
        if not invoice:
            raise UserError("Aucune facture validée trouvée pour créer l'avoir")

        credit_note_wizard = (
            self.env["account.move.reversal"]
            .with_user(self._get_bot_user())
            .with_context(active_model="account.move", active_ids=invoice.ids)
            .create(
                {
                    "reason": f"Square Refund: {self.square_refund_id}",
                    "journal_id": invoice.journal_id.id,
                }
            )
        )
        result = credit_note_wizard.reverse_moves()
        credit_note = self.env["account.move"].browse(result.get("res_id"))
        if not credit_note.exists():
            raise UserError("L'avoir créé n'existe pas")
        if is_partial:
            self._adjust_credit_note_for_partial_refund(credit_note)
        return credit_note

    def _update_order_line_quantities(self):
        """
        Update order line quantities to reflect returned items.
        Distributes refund amount proportionally across order lines
        when no explicit refunded line IDs are provided.
        Uses tax-included prices for consistency with Square refund amounts.
        """
        # Get bot user for operations to ensure proper user context
        bot_user = (
            self.env["res.users"].sudo().search([("login", "=", "square_bot")], limit=1)
        )
        if not bot_user:
            _logger.warning(
                "Square bot user not found, using current user for quantity updates"
            )
            bot_user = self.env.user

        sale_order = self.sale_order_id
        if not sale_order.order_line:
            _logger.warning(f"No order lines found for sale order {sale_order.name}")
            return

        eligible_lines = sale_order.order_line.filtered(
            lambda line: line.product_uom_qty > 0 and not (line.name or "").startswith("[RETOUR]")
        )
        if not eligible_lines:
            _logger.warning(f"No eligible order lines found for {sale_order.name}")
            return

        # Single line refund case
        if len(eligible_lines) == 1:
            single_line = eligible_lines[0]
            returned_qty = 0
            if self.refunded_line_ids:
                returned_qty = len(self.refunded_line_ids)
                _logger.info(
                    f"Single line refund - Using refunded_line_ids: {self.refunded_line_ids}, returning {returned_qty} units"
                )
            else:
                # Infer quantity from refund amount trying multiple price bases (tax incl/excl)
                candidates = []
                if single_line.product_uom_qty > 0:
                    price_incl = single_line.price_total / single_line.product_uom_qty
                    price_excl = single_line.price_subtotal / single_line.product_uom_qty
                    candidates.append(price_incl)
                    candidates.append(price_excl)
                candidates.append(single_line.price_unit)
                # Remove zero/duplicate prices
                uniq_prices = []
                for p in candidates:
                    if p and p > 0 and all(abs(p - up) > 1e-9 for up in uniq_prices):
                        uniq_prices.append(p)
                best_qty = 0
                best_err = None
                chosen_price = None
                for price in uniq_prices:
                    qty_est = self.refund_amount / price
                    qty_round = int(round(qty_est))
                    if qty_round <= 0:
                        continue
                    err = abs(self.refund_amount - qty_round * price)
                    if (best_err is None) or (err < best_err - 1e-9):
                        best_err = err
                        best_qty = qty_round
                        chosen_price = price
                if best_qty:
                    returned_qty = best_qty
                    _logger.info(
                        f"Single line refund - Multi-price inference chose qty {returned_qty} using unit price {chosen_price:.6f} (amount {self.refund_amount}, residual error {best_err:.6f})"
                    )
                else:
                    unit_price_fallback = single_line.price_total / single_line.product_uom_qty if single_line.product_uom_qty > 0 else single_line.price_unit
                    if unit_price_fallback > 0:
                        exact_returned_qty = self.refund_amount / unit_price_fallback
                        returned_qty = max(1, int(round(exact_returned_qty)))
                        _logger.info(
                            f"Single line refund - Fallback quantity inference: {self.refund_amount} / {unit_price_fallback} = {exact_returned_qty:.2f} -> {returned_qty}"
                        )
            if returned_qty > 0:
                max_returnable = single_line.product_uom_qty - single_line.returned_qty
                returned_qty = min(returned_qty, max_returnable)
                actual_returned = single_line.update_returned_quantity(returned_qty)
                self._add_adjustment_line(single_line, returned_qty, bot_user)
                _logger.info(
                    f"Single line refund - Created adjustment line for {single_line.product_id.name}: qty -{returned_qty}, returned {actual_returned} units"
                )
            return

        # Multi-line refund using refunded_line_ids list
        if self.refunded_line_ids:
            refunded_lines_by_product = {}
            for line in eligible_lines:
                if hasattr(line, "square_line_id") and line.square_line_id in self.refunded_line_ids:
                    refunded_lines_by_product.setdefault(line.product_id.id, {"line": line, "count": 0})["count"] += 1
            if not refunded_lines_by_product:
                total_refunded_items = len(self.refunded_line_ids)
                total_lines = len(eligible_lines)
                items_per_line = total_refunded_items // total_lines
                remainder = total_refunded_items % total_lines
                for i, line in enumerate(eligible_lines):
                    count = items_per_line + (1 if i < remainder else 0)
                    if count > 0:
                        refunded_lines_by_product[line.product_id.id] = {"line": line, "count": count}
            total_returned_qty = 0
            for product_data in refunded_lines_by_product.values():
                line = product_data["line"]
                returned_qty = min(product_data["count"], line.product_uom_qty - line.returned_qty)
                if returned_qty <= 0:
                    continue
                actual_returned = line.update_returned_quantity(returned_qty)
                self._add_adjustment_line(line, returned_qty, bot_user)
                _logger.info(
                    f"Multi-line refund - Adjustment line for {line.product_id.name}: qty -{returned_qty}, returned {actual_returned} units"
                )
                total_returned_qty += actual_returned
            _logger.info(
                f"Completed multi-line refund using refunded_line_ids. Total returned qty: {total_returned_qty}"
            )
            return

        # Proportional distribution fallback
        line_infos = []
        refundable_total = 0.0
        for line in eligible_lines:
            remaining_qty = max(line.product_uom_qty - line.returned_qty, 0)
            if remaining_qty <= 0:
                continue
            unit_price_with_tax = (
                line.price_total / line.product_uom_qty if line.product_uom_qty > 0 else line.price_unit
            )
            amount_remaining = unit_price_with_tax * remaining_qty
            refundable_total += amount_remaining
            line_infos.append((line, remaining_qty, unit_price_with_tax, amount_remaining))
        if refundable_total <= 0:
            _logger.warning(f"No refundable amount left for sale order {sale_order.name}")
            return
        amount_to_distribute = min(self.refund_amount, refundable_total)
        qty_allocations = []
        total_allocated = 0
        for line, remaining_qty, unit_price, amount_remaining in line_infos:
            if unit_price > 0:
                exact_qty = (amount_remaining / refundable_total) * (amount_to_distribute / unit_price)
            else:
                exact_qty = (remaining_qty * amount_to_distribute) / refundable_total
            floored = int(exact_qty)
            qty_allocations.append({
                "line": line,
                "remaining_qty": remaining_qty,
                "unit_price": unit_price,
                "exact": exact_qty,
                "allocated": floored,
                "fraction": exact_qty - floored,
            })
            total_allocated += floored
        remaining_units = round(sum(a["exact"] for a in qty_allocations) - total_allocated)
        for alloc in sorted(qty_allocations, key=lambda a: a["fraction"], reverse=True):
            if remaining_units <= 0:
                break
            if alloc["allocated"] < alloc["remaining_qty"]:
                alloc["allocated"] += 1
                remaining_units -= 1
        total_returned_qty = 0
        for alloc in qty_allocations:
            returned_qty = min(alloc["allocated"], alloc["remaining_qty"])
            if returned_qty <= 0:
                continue
            actual_returned = alloc["line"].update_returned_quantity(returned_qty)
            self._add_adjustment_line(alloc["line"], returned_qty, bot_user)
            _logger.info(
                f"Proportional refund - Adjustment line for {alloc['line'].product_id.name}: qty -{returned_qty}, returned {actual_returned} units (exact={alloc['exact']:.2f})"
            )
            total_returned_qty += actual_returned
        _logger.info(
            f"Completed proportional refund distribution for {self.square_refund_id}. Total returned qty: {total_returned_qty}"
        )

    def _reduce_sale_order_quantity(self, order_line, returned_qty):
        """
        Reduce the original sale order line quantity to reflect refunded items
        This makes the sale order show the net quantity after refunds
        """
        if returned_qty <= 0:
            return

        # Get bot user for operations to ensure proper user context
        bot_user = (
            self.env["res.users"].sudo().search([("login", "=", "square_bot")], limit=1)
        )

        if not bot_user:
            _logger.warning(
                "Square bot user not found, using current user for quantity reduction"
            )
            bot_user = self.env.user

        original_qty = order_line.product_uom_qty
        qty_delivered = order_line.qty_delivered
        available_to_reduce = original_qty - qty_delivered

        # If we can't reduce the quantity (already delivered), create negative adjustment line
        if available_to_reduce <= 0 or returned_qty > available_to_reduce:
            _logger.info(
                f"Cannot reduce quantity for {order_line.product_id.name} (delivered: {qty_delivered}, "
                f"ordered: {original_qty}, available to reduce: {available_to_reduce}). "
                f"Creating negative adjustment line instead."
            )

            # Create negative adjustment line for the returned items
            self._add_adjustment_line(order_line, returned_qty, bot_user)
            _logger.info(
                f"Created negative adjustment line for {order_line.product_id.name}: "
                f"quantity -{returned_qty:.2f}"
            )
        else:
            # We can safely reduce the quantity
            new_qty = max(
                qty_delivered, original_qty - returned_qty
            )  # Don't go below delivered qty

            if new_qty != original_qty:
                # Use write with context to prevent automatic amount recalculation during edit
                order_line.with_user(bot_user).write({"product_uom_qty": new_qty})
                _logger.info(
                    f"Reduced sale order quantity for {order_line.product_id.name}: "
                    f"{original_qty:.2f} -> {new_qty:.2f} (reduced by {returned_qty:.2f})"
                )

                # Recalculate order totals after quantity change
                order_line.order_id.with_user(bot_user)._compute_amounts()
            else:
                _logger.debug(
                    f"No quantity reduction needed for {order_line.product_id.name}: "
                    f"already at minimum quantity"
                )

    def _update_sale_order_quantities_after_refund(self):
        """Reflect completed returns on the original sale lines."""
        returned_moves = self.return_picking_ids.move_ids.filtered(
            lambda move: move.state == "done" and move.origin_returned_move_id.sale_line_id
        )
        if returned_moves or self.refunded_line_ids:
            bot_user = self._get_bot_user()
            quantities = defaultdict(float)
            for move in returned_moves:
                line = move.origin_returned_move_id.sale_line_id
                quantities[line] += move.product_uom._compute_quantity(
                    move.quantity, line.product_uom_id, round=False
                )
            if not returned_moves:
                quantities = self._get_partial_return_quantities()
            for line, quantity in quantities.items():
                returned_qty = line.update_returned_quantity(quantity)
                self._add_adjustment_line(line, returned_qty, bot_user)
            return

        _logger.info(
            f"Processing partial refund quantity updates for refund {self.square_refund_id}"
        )
        # For partial refunds, try to get order details from Square for precise updates
        order_details = self._fetch_order_details_from_square()
        if order_details:
            self._update_order_line_quantities_from_square(order_details)
        else:
            # Fallback to proportional distribution if Square API not available
            self._update_order_line_quantities()

    def _fetch_order_details_from_square(self):
        """
        Fetch order details from Square API to get accurate quantities
        """
        try:
            square_api = self.env["square.api.client"]
            order_data = square_api.get_order(self.square_order_id)

            if not order_data:
                _logger.warning(
                    f"Could not fetch order {self.square_order_id} from Square"
                )
                return None

            # Extract line items from the order
            line_items = []
            if "line_items" in order_data:
                for item in order_data["line_items"]:
                    line_items.append(
                        {
                            "name": item.get("name", ""),
                            "quantity": float(item.get("quantity", "0")),
                            "total_money": item.get("total_money", {}),
                            "variation_name": item.get("variation_name", ""),
                            "catalog_object_id": item.get("catalog_object_id", ""),
                            "item_type": item.get("item_type", ""),
                            # Include UID so we can aggregate refunds per Square line
                            "uid": item.get("uid"),
                        }
                    )

            return {
                "order_id": self.square_order_id,
                "line_items": line_items,
                "total_money": order_data.get("total_money", {}),
                "net_amounts": order_data.get("net_amounts", {}),
                "refunds": order_data.get("refunds", []),
            }

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(f"Error fetching order details from Square: {str(e)}")
            return None

    def _update_order_line_quantities_from_square(self, order_details):
        """
        Update order line quantities based on Square order details
        This provides more accurate quantity tracking than proportional distribution
        """
        # Get bot user for operations to ensure proper user context
        bot_user = (
            self.env["res.users"].sudo().search([("login", "=", "square_bot")], limit=1)
        )

        if not bot_user:
            _logger.warning(
                "Square bot user not found, using current user for quantity updates"
            )
            bot_user = self.env.user

        if not order_details or not order_details.get("line_items"):
            _logger.warning(
                "No order details available from Square, falling back to proportional distribution"
            )
            self._update_order_line_quantities()
            return

        sale_order = self.sale_order_id
        square_line_items = order_details["line_items"]

        _logger.info(
            f"Updating quantities from Square order {order_details['order_id']} "
            f"with {len(square_line_items)} line items"
        )

        total_returned_qty = 0

        # If we have specific refunded line IDs, use those for precise updates
        if self.refunded_line_ids:
            _logger.info(
                f"Using specific refunded line IDs: {self.refunded_line_ids} for precise quantity updates"
            )
            total_returned_qty = self._update_quantities_from_refunded_lines(
                order_details
            )
        else:
            # Try to match Square line items with Odoo order lines
            for square_item in square_line_items:
                # Find matching order line by product name or other criteria
                matching_line = self._find_matching_order_line(sale_order, square_item)

                if matching_line:
                    # For refunds, we want to update the returned quantity
                    # This is a simplified approach - in practice, we'd need to track
                    # which specific items were returned
                    current_quantity = float(square_item.get("quantity", "0"))

                    # If the current quantity in Square is less than original,
                    # it means some items were returned
                    if hasattr(matching_line, "square_original_quantity"):
                        original_qty = matching_line.square_original_quantity
                    else:
                        original_qty = matching_line.product_uom_qty

                    if current_quantity < original_qty:
                        returned_qty = original_qty - current_quantity
                        actual_returned = matching_line.update_returned_quantity(
                            returned_qty
                        )

                        # Create negative adjustment line for returned items
                        self._add_adjustment_line(matching_line, returned_qty, bot_user)
                        total_returned_qty += actual_returned

                        _logger.info(
                            f"Updated {matching_line.product_id.name}: "
                            f"Square quantity {current_quantity}, returned {actual_returned}, "
                            f"created negative adjustment line -{returned_qty}"
                        )

        # If no specific matches found, fall back to proportional distribution
        if total_returned_qty == 0:
            _logger.info(
                "No specific quantity matches found, using proportional distribution"
            )
            self._update_order_line_quantities()

        _logger.info(
            f"Completed Square-based quantity updates: "
            f"Total returned quantity: {total_returned_qty}"
        )

    def _update_quantities_from_refunded_lines(self, order_details):
        """
        Update quantities based on specific refunded line IDs from Square
        This provides precise quantity tracking for partial refunds
        """
        # Get bot user for operations to ensure proper user context
        bot_user = (
            self.env["res.users"].sudo().search([("login", "=", "square_bot")], limit=1)
        )

        if not bot_user:
            _logger.warning(
                "Square bot user not found, using current user for quantity updates"
            )
            bot_user = self.env.user

        sale_order = self.sale_order_id
        square_line_items = order_details["line_items"]
        total_returned_qty = 0
        # Aggregate refunded quantities per matching order line to avoid duplicate adjustment lines
        aggregated_refunds = {}

        for refunded_line_uid in self.refunded_line_ids:
            refunded_square_item = next(
                (si for si in square_line_items if si.get("uid") == refunded_line_uid),
                None,
            )
            if not refunded_square_item:
                _logger.warning(
                    f"Could not find Square line item with UID {refunded_line_uid} in order details"
                )
                continue

            matching_line = self._find_matching_order_line(sale_order, refunded_square_item)
            if not matching_line:
                _logger.warning(
                    f"Could not find matching Odoo line for refunded Square item {refunded_square_item.get('name', 'Unknown')}"
                )
                continue

            refunded_quantity = float(refunded_square_item.get("quantity", "1")) or 1.0
            data = aggregated_refunds.setdefault(
                matching_line.id, {"line": matching_line, "qty": 0.0}
            )
            data["qty"] += refunded_quantity

        # Apply aggregated refunds
        for data in aggregated_refunds.values():
            line = data["line"]
            refunded_quantity = data["qty"]
            if refunded_quantity <= 0:
                continue
            # Clamp to remaining refundable quantity
            refundable_left = line.product_uom_qty - getattr(line, "returned_qty", 0)
            effective_qty = min(refunded_quantity, refundable_left)
            if effective_qty <= 0:
                continue
            actual_returned = line.update_returned_quantity(effective_qty)
            self._add_adjustment_line(line, effective_qty, bot_user)
            total_returned_qty += actual_returned
            _logger.info(
                f"Aggregated refund - {line.product_id.name}: returned {actual_returned} units (UIDs grouped), created single adjustment line -{effective_qty}"
            )

        if not aggregated_refunds:
            _logger.info(
                "No matching Square line UIDs aggregated; no quantity adjustments applied in UID-based path"
            )

        return total_returned_qty

    def _find_matching_order_line(self, sale_order, square_item):
        """
        Find the matching order line for a Square line item
        """
        item_name = square_item.get("name", "").strip()
        variation_name = square_item.get("variation_name", "")

        # Try to match by product name
        for line in sale_order.order_line:
            if not line.product_id:
                continue

            # Check product name match
            if line.product_id.name and item_name:
                if (
                    item_name.lower() in line.product_id.name.lower()
                    or line.product_id.name.lower() in item_name.lower()
                ):
                    return line

            # Check if variation name matches
            if variation_name and line.product_id.name:
                if variation_name.lower() in line.product_id.name.lower():
                    return line

        # If no match found, try to match by catalog object ID if available
        catalog_id = square_item.get("catalog_object_id")
        if catalog_id:
            for line in sale_order.order_line:
                if (
                    hasattr(line, "square_catalog_id")
                    and line.square_catalog_id == catalog_id
                ):
                    return line

        _logger.debug(f"No matching order line found for Square item: {item_name}")
        return None

    def _adjust_credit_note_for_partial_refund(self, credit_note):
        """Scale the reversal lines and match the Square amount after rounding."""
        currency = credit_note.currency_id
        if currency.compare_amounts(credit_note.amount_total, self.refund_amount) == 0:
            return
        if currency.is_zero(credit_note.amount_total):
            raise UserError("Impossible d'ajuster un avoir dont le montant est nul")

        credit_note = credit_note.with_user(self._get_bot_user())
        ratio = self.refund_amount / credit_note.amount_total
        product_lines = credit_note.invoice_line_ids.filtered(
            lambda line: line.display_type == "product"
        )
        credit_note.write({
            "invoice_line_ids": [
                Command.update(line.id, {"price_unit": line.price_unit * ratio})
                for line in product_lines
            ],
        })

        if credit_note.amount_tax or product_lines.tax_ids:
            # Use Odoo's invoice rounding correction for taxable reversals.
            credit_note._check_total_amount(self.refund_amount)
        else:
            difference = currency.round(self.refund_amount - credit_note.amount_total)
            if difference:
                line = max(product_lines, key=lambda line: abs(line.price_subtotal))
                unit_quantity = line.quantity * (1 - line.discount / 100)
                if unit_quantity:
                    line.price_unit += difference / unit_quantity

        if currency.compare_amounts(credit_note.amount_total, self.refund_amount) != 0:
            raise UserError("Le montant de l'avoir ne correspond pas au remboursement Square")

    def _find_and_process_credit_note(self, invoice):
        """
        Find and process a credit note (used by full credit note creation)
        """
        # Find the created credit note
        credit_notes = self.env["account.move"].search(
            [
                ("move_type", "=", "out_refund"),
                ("invoice_origin", "=", invoice.name),
                ("state", "=", "draft"),
            ],
            order="create_date desc",
            limit=1,
        )

        # If not found by invoice_origin, try by payment_reference or ref
        if not credit_notes:
            credit_notes = self.env["account.move"].search(
                [
                    ("move_type", "=", "out_refund"),
                    ("ref", "ilike", self.square_refund_id),
                    ("state", "=", "draft"),
                ],
                order="create_date desc",
                limit=1,
            )

        if credit_notes:
            # Get bot user
            bot_user = (
                self.env["res.users"]
                .sudo()
                .search([("login", "=", "square_bot")], limit=1)
            )

            if not bot_user:
                _logger.warning(
                    "Square bot user not found, using current user for credit note posting"
                )
                bot_user = self.env.user

            # Post the credit note
            credit_notes.with_user(bot_user).action_post()

            # Create payment for the credit note
            processor = self.env["square.order.processor"]
            # For full refunds, pass None to use credit note total; for partial, we already handle in separate method
            payment_created = processor._create_payment_for_credit_note(
                credit_notes, self.sale_order_id, bot_user
            )

            # Ensure the credit note payment state is properly updated
            if payment_created:
                if credit_notes.payment_state != "paid":
                    # Force update of payment state if needed
                    credit_notes._compute_payment_state()
                    _logger.info(
                        f"Credit note {credit_notes.name} payment state after update: {credit_notes.payment_state}"
                    )

            # Add chatter message for credit note creation
            try:
                self.sale_order_id.with_user(bot_user).with_context(
                    mail_auto_subscribe_no_notify=True, mail_create_nosubscribe=True
                ).message_post(
                    body=f"Avoir Square créé - {credit_notes.name}, Montant: {credit_notes.amount_total} {credit_notes.currency_id.name}, Paiement: {'Créé' if payment_created else 'En attente'}",
                    subject="Intégration Square : Avoir Créé et Traité",
                    message_type="comment",
                )
                _logger.info(
                    f"Posted chatter message for credit note {credit_notes.name}"
                )
            except Exception as e:
                _logger.warning(
                    f"Could not post chatter message for credit note {credit_notes.name}: {str(e)}"
                )

            _logger.info(
                f"Created and processed credit note {credit_notes.name} for refund {self.square_refund_id}"
            )
            return credit_notes
        else:
            _logger.error(
                f"Failed to find created credit note for refund {self.square_refund_id}"
            )
            raise UserError("Erreur lors de la création de l'avoir")

    def _handle_failed_refund(self):
        """
        Handle failed refund - clean up pending actions
        """
        try:
            # Cancel return pickings
            for picking in self.return_picking_ids:
                if picking.state not in ["done", "cancel"]:
                    picking.action_cancel()

            # Cancel credit note if not posted
            if self.credit_note_id and self.credit_note_id.state == "draft":
                self.credit_note_id.button_cancel()

            # Log failure
            self.env["square.integration.log"].log_square_event(
                event_type="refund_processed",
                title=f"Remboursement échoué pour {self.sale_order_id.name}",
                description=f"""
                    <p><strong>Remboursement Square Échoué</strong></p>
                    <ul>
                        <li>Commande Odoo : <strong>{self.sale_order_id.name}</strong></li>
                        <li>ID Remboursement Square : <code>{self.square_refund_id}</code></li>
                        <li>Statut : Échoué</li>
                        <li>Actions : Retours et avoir annulés</li>
                    </ul>
                """,
                status="error",
                square_order_id=self.square_order_id,
                square_refund_id=self.square_refund_id,
                sale_order_id=self.sale_order_id.id,
            )

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(f"Error handling failed refund: {str(e)}")

    def _handle_cancelled_refund(self):
        """
        Handle cancelled refund - clean up pending actions
        """
        try:
            # Cancel return pickings
            for picking in self.return_picking_ids:
                if picking.state not in ["done", "cancel"]:
                    picking.action_cancel()

            # Cancel credit note if not posted
            if self.credit_note_id and self.credit_note_id.state == "draft":
                self.credit_note_id.button_cancel()

            # Log cancellation
            self.env["square.integration.log"].log_square_event(
                event_type="refund_processed",
                title=f"Remboursement annulé pour {self.sale_order_id.name}",
                description=f"""
                    <p><strong>Remboursement Square Annulé</strong></p>
                    <ul>
                        <li>Commande Odoo : <strong>{self.sale_order_id.name}</strong></li>
                        <li>ID Remboursement Square : <code>{self.square_refund_id}</code></li>
                        <li>Statut : Annulé</li>
                        <li>Actions : Retours et avoir annulés</li>
                    </ul>
                """,
                status="warning",
                square_order_id=self.square_order_id,
                square_refund_id=self.square_refund_id,
                sale_order_id=self.sale_order_id.id,
            )

        except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
            raise
        except Exception as e:
            _logger.error(f"Error handling cancelled refund: {str(e)}")

# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError, AccessError
from datetime import timedelta
import logging

_logger = logging.getLogger(__name__)


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

    _sql_constraints = [
        (
            "square_refund_id_unique",
            "UNIQUE(square_refund_id)",
            "Square Refund ID must be unique.",
        ),
    ]

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
        # Extract updated data
        amount_money = refund_data.get("amount_money", {})
        refund_amount = float(amount_money.get("amount", 0)) / 100.0

        currency = self._get_eur_currency()
        if not currency:
            raise ValidationError("EUR currency not found in the system")

        # Update the existing record
        vals = {
            "status": refund_data.get("status", "PENDING").lower(),
            "refund_amount": refund_amount,
            "currency_id": currency.id,
            "sale_order_id": sale_order.id,
            "refund_reason": refund_data.get("reason"),
            "refunded_line_ids": refund_data.get("refunded_line_ids", []),
            "square_data": refund_data,
        }

        existing_refund.write(vals)
        _logger.info(f"Updated existing refund record: {existing_refund.display_name}")
        return existing_refund

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

        # Robust idempotency check with database locking (similar to order processing)
        existing_refund = self._check_existing_refund(square_refund_id)
        if existing_refund:
            _logger.info(
                f"Found existing refund record: {existing_refund.display_name} (status: {existing_refund.status})"
            )

            # If already completed, don't reprocess
            if existing_refund.status == "completed":
                _logger.info(
                    f"Refund {square_refund_id} already completed, skipping duplicate processing"
                )
                return existing_refund

            # If failed or cancelled, allow reprocessing by updating the existing record
            if existing_refund.status in ["failed", "cancelled"]:
                _logger.info(
                    f"Refund {square_refund_id} was {existing_refund.status}, updating existing record"
                )
                # Update the existing record with new data
                return self._update_existing_refund(
                    existing_refund, refund_data, sale_order
                )
            else:
                # For pending refunds, return the existing record to continue processing
                _logger.info(
                    f"Refund {square_refund_id} is {existing_refund.status}, returning existing record"
                )
                return existing_refund

        # Extract refund amount
        amount_money = refund_data.get("amount_money", {})
        refund_amount = float(amount_money.get("amount", 0)) / 100.0

        # Get currency - we only support EUR
        currency_code = amount_money.get("currency", "EUR")

        # Validate that it's EUR (since we only support euros)
        if currency_code != "EUR":
            raise ValidationError(
                f"Unsupported currency '{currency_code}'. This system only supports EUR refunds."
            )

        currency = self._get_eur_currency()

        if not currency:
            raise ValidationError(
                "EUR currency not found in the system. Please ensure EUR is properly configured."
            )

        vals = {
            "square_refund_id": square_refund_id,
            "square_order_id": refund_data.get("order_id"),
            "status": refund_data.get("status", "PENDING").lower(),
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
            "product_uom": original_line.product_uom.id,
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
        """
        Create return pickings and credit note for pending refund
        """
        with self.env.cr.savepoint():
            try:
                sale_order = self.sale_order_id

                # IDEMPOTENCY CHECK: Only create return pickings if they don't already exist
                # This prevents duplicate returns when webhook processing happens multiple times
                if self.return_picking_ids:
                    _logger.info(
                        f"Return pickings already exist for refund {self.square_refund_id}. "
                        f"Skipping duplicate creation. Existing pickings: {self.return_picking_ids.mapped('name')}"
                    )
                else:
                    # Check if refund is partial or full
                    is_partial = self._is_partial_refund()

                    if is_partial:
                        # Create partial return picking for specific lines
                        self._create_partial_return_picking()
                    else:
                        # Create full return picking
                        self._create_full_return_picking()

                # Create credit note (but don't validate yet)
                credit_note = self._create_credit_note()
                self.credit_note_id = credit_note.id

                # Update sale order quantities immediately for full refunds
                # This ensures the order reflects the refund even if COMPLETED webhook is delayed

                # Log the pending refund actions
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

                return True

            except Exception as e:
                _logger.error(f"Error creating pending refund actions: {str(e)}")
                self.processing_errors = str(e)
                raise UserError(
                    f"Erreur lors de la création des actions de remboursement : {str(e)}"
                )

    def _complete_refund(self):
        """
        Complete the refund process
        """
        # Get bot user for all operations
        bot_user = (
            self.env["res.users"].sudo().search([("login", "=", "square_bot")], limit=1)
        )

        if not bot_user:
            _logger.warning(
                "Square bot user not found, using current user for refund completion"
            )
            bot_user = self.env.user

        with self.env.cr.savepoint():
            try:
                # Validate credit note
                if self.credit_note_id and self.credit_note_id.state == "draft":
                    self.credit_note_id.with_user(bot_user).action_post()

                # Process return pickings using standard methods
                for picking in self.return_picking_ids:
                    if picking.state not in ["done", "cancel"]:
                        if picking.state == "draft":
                            picking.with_user(bot_user).action_confirm()
                        if picking.state in ["confirmed", "waiting"]:
                            picking.with_user(bot_user).action_assign()
                        # Validate even if stock is insufficient for returns
                        if picking.state in ["assigned", "confirmed", "waiting"]:
                            picking.with_user(bot_user).with_context(
                                force_validate=True,
                                mail_auto_subscribe_no_notify=True,
                                mail_create_nosubscribe=True,
                            ).button_validate()

                # Quantities are already updated when the credit note is created
                # (_update_order_line_quantities_from_square / proportional paths).
                # Running _update_sale_order_quantities_after_refund again double-counts.

                # Update refund status
                self.processed_at = fields.Datetime.now()

                # Log completion with specialized refund logging
                refund_data = {
                    "id": self.square_refund_id,
                    "order_id": self.square_order_id,
                    "amount_money": {
                        "amount": int(self.refund_amount * 100),  # Convert to cents
                        "currency": self.currency_id.name,
                    },
                }

                self.env["square.integration.log"].log_refund_processed(
                    sale_order=self.sale_order_id, refund_data=refund_data
                )

                # Log completion
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

                return True

            except Exception as e:
                _logger.error(f"Error completing refund: {str(e)}")
                self.processing_errors = str(e)
                raise UserError(
                    f"Erreur lors de la finalisation du remboursement : {str(e)}"
                )

    def _is_partial_refund(self):
        """
        Determine if this is a partial refund based on amount and line items
        """
        # Check if refund amount is less than order total (with small tolerance for rounding)
        amount_tolerance = 0.01  # 1 cent tolerance
        if self.refund_amount < (self.sale_order_id.amount_total - amount_tolerance):
            return True

        # If specific line IDs are provided, it's definitely a partial refund
        # (even if the amount matches, it might be for specific items)
        if self.refunded_line_ids and len(self.refunded_line_ids) > 0:
            return True

        # Check if there are already other refunds for this order
        existing_refunds = self.env["square.refund"].search(
            [
                ("sale_order_id", "=", self.sale_order_id.id),
                ("id", "!=", self.id),  # Exclude current refund
                ("status", "in", ["completed", "pending"]),
            ]
        )

        if existing_refunds:
            # If there are other refunds, this could be additional partial refund
            total_refunded = sum(existing_refunds.mapped("refund_amount"))
            remaining_amount = self.sale_order_id.amount_total - total_refunded

            # If this refund amount is less than remaining amount, it's partial
            if self.refund_amount < (remaining_amount - amount_tolerance):
                return True

        return False

    def _create_partial_return_picking(self):
        """
        Create return picking for specific line items based on Square refund data
        """
        sale_order = self.sale_order_id

        # Get bot user for all operations
        bot_user = (
            self.env["res.users"].sudo().search([("login", "=", "square_bot")], limit=1)
        )

        if not bot_user:
            _logger.warning(
                "Square bot user not found, using current user for return picking creation"
            )
            bot_user = self.env.user

        # Find the original delivery picking
        delivery_picking = sale_order.picking_ids.filtered(
            lambda p: p.picking_type_code == "outgoing" and p.state == "done"
        )

        if not delivery_picking:
            # For physical store orders where items are handed directly to customers,
            # there are no delivery pickings. Skip return picking creation.
            _logger.info(
                f"No delivery picking found for sale order {sale_order.name}. "
                f"This appears to be a physical store order where items are handed directly to customers. "
                f"Skipping partial return picking creation for refund {self.square_refund_id}."
            )
            return

        # IDEMPOTENCY CHECK: Check if a return picking already exists for this delivery
        # This prevents duplicate returns when both refund.created and refund.updated webhooks arrive
        existing_return = self.env["stock.picking"].search(
            [
                ("origin", "=", f"Return of {delivery_picking.name}"),
                ("picking_type_code", "=", "incoming"),
            ],
            limit=1,
        )

        if existing_return:
            _logger.info(
                f"Return picking {existing_return.name} already exists for delivery {delivery_picking.name}. "
                f"Skipping duplicate return creation for refund {self.square_refund_id}. "
                f"Linking existing return to this refund record."
            )
            # Link the existing return to this refund if not already linked
            if existing_return.id not in self.return_picking_ids.ids:
                self.return_picking_ids = [(4, existing_return.id)]
            return

        # Create return picking wizard
        return_wizard = (
            self.env["stock.return.picking"]
            .with_user(bot_user)
            .with_context(active_id=delivery_picking.id, active_model="stock.picking")
            .create({})
        )

        # Configure return lines based on Square refund data
        if self.refunded_line_ids:
            self._configure_partial_return_lines(return_wizard, delivery_picking)
        else:
            # If no specific line IDs, calculate proportionally based on refund amount
            self._configure_proportional_return_lines(return_wizard, delivery_picking)

        # Create the return picking
        return_wizard.with_user(bot_user).create_returns()

        # Store the created return picking
        return_pickings = self.env["stock.picking"].search(
            [("origin", "=", f"Return of {delivery_picking.name}")], limit=1
        )

        if return_pickings:
            self.return_picking_ids = [(4, return_pickings.id)]
            _logger.info(
                f"Created partial return picking {return_pickings.name} for refund {self.square_refund_id}"
            )
        else:
            _logger.warning(
                f"No partial return pickings found after creation for refund {self.square_refund_id}"
            )

    def _configure_partial_return_lines(self, return_wizard, delivery_picking):
        """
        Configure return lines based on specific refunded line IDs from Square
        """
        # Get order details from Square to match line items
        order_details = self._fetch_order_details_from_square()

        if not order_details or not order_details.get("line_items"):
            _logger.warning(
                f"No order details available from Square for refund {self.square_refund_id}, "
                "falling back to proportional distribution"
            )
            self._configure_proportional_return_lines(return_wizard, delivery_picking)
            return

        # Match Square line items with Odoo order lines and configure return quantities
        square_line_items = order_details["line_items"]

        for return_line in return_wizard.product_return_moves:
            odoo_line = return_line.move_id.sale_line_id
            if not odoo_line:
                continue

            # Find matching Square line item
            matching_square_item = self._find_matching_square_line_item(
                odoo_line, square_line_items
            )

            if matching_square_item and self._should_return_line_item(
                matching_square_item
            ):
                # Calculate return quantity based on Square data
                return_quantity = self._calculate_return_quantity_for_line(
                    odoo_line, matching_square_item
                )

                if return_quantity > 0:
                    return_line.to_refund = True
                    return_line.quantity = return_quantity
                    _logger.info(
                        f"Configured return for line {odoo_line.product_id.name}: "
                        f"quantity {return_quantity}"
                    )
                else:
                    return_line.to_refund = False
            else:
                return_line.to_refund = False

    def _configure_proportional_return_lines(self, return_wizard, delivery_picking):
        """
        Configure return lines proportionally based on refund amount when no specific line data
        """
        # Calculate proportional distribution based on refund amount
        total_order_amount = sum(
            line.price_subtotal for line in self.sale_order_id.order_line
        )

        if total_order_amount <= 0:
            _logger.warning(
                f"Total order amount is zero for refund {self.square_refund_id}"
            )
            return

        refund_ratio = self.refund_amount / total_order_amount

        for return_line in return_wizard.product_return_moves:
            odoo_line = return_line.move_id.sale_line_id
            if not odoo_line:
                continue

            # Calculate return quantity proportionally
            proportional_quantity = odoo_line.product_uom_qty * refund_ratio

            # Ensure we don't return more than was originally delivered
            max_returnable = odoo_line.product_uom_qty - odoo_line.returned_qty
            return_quantity = min(proportional_quantity, max_returnable)

            if return_quantity > 0:
                return_line.to_refund = True
                return_line.quantity = return_quantity
                _logger.info(
                    f"Configured proportional return for line {odoo_line.product_id.name}: "
                    f"quantity {return_quantity} (ratio: {refund_ratio:.2%})"
                )
            else:
                return_line.to_refund = False

    def _find_matching_square_line_item(self, odoo_line, square_line_items):
        """
        Find the matching Square line item for an Odoo order line
        """
        # Try to match by product SKU first
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
        """
        Create full return picking for complete refund
        """
        sale_order = self.sale_order_id

        # Get bot user for all operations
        bot_user = (
            self.env["res.users"].sudo().search([("login", "=", "square_bot")], limit=1)
        )

        if not bot_user:
            _logger.warning(
                "Square bot user not found, using current user for return picking creation"
            )
            bot_user = self.env.user

        # Find the original delivery picking
        delivery_picking = sale_order.picking_ids.filtered(
            lambda p: p.picking_type_code == "outgoing" and p.state == "done"
        )

        if not delivery_picking:
            # For physical store orders where items are handed directly to customers,
            # there are no delivery pickings. Skip return picking creation.
            _logger.info(
                f"No delivery picking found for sale order {sale_order.name}. "
                f"This appears to be a physical store order where items are handed directly to customers. "
                f"Skipping return picking creation for refund {self.square_refund_id}."
            )
            return

        # IDEMPOTENCY CHECK: Check if a return picking already exists for this delivery
        # This prevents duplicate returns when both refund.created and refund.updated webhooks arrive
        existing_return = self.env["stock.picking"].search(
            [
                ("origin", "=", f"Return of {delivery_picking.name}"),
                ("picking_type_code", "=", "incoming"),
            ],
            limit=1,
        )

        if existing_return:
            _logger.info(
                f"Return picking {existing_return.name} already exists for delivery {delivery_picking.name}. "
                f"Skipping duplicate return creation for refund {self.square_refund_id}. "
                f"Linking existing return to this refund record."
            )
            # Link the existing return to this refund if not already linked
            if existing_return.id not in self.return_picking_ids.ids:
                self.return_picking_ids = [(6, 0, [existing_return.id])]
            return

        # Create return picking for all lines
        return_wizard = (
            self.env["stock.return.picking"]
            .with_user(bot_user)
            .with_context(active_id=delivery_picking.id, active_model="stock.picking")
            .create({})
        )

        return_wizard.with_user(bot_user).create_returns()

        # Store the created return picking
        return_pickings = self.env["stock.picking"].search(
            [("origin", "=", f"Return of {delivery_picking.name}")]
        )

        if return_pickings:
            self.return_picking_ids = [(6, 0, return_pickings.ids)]
            _logger.info(
                f"Created return picking {return_pickings.name} for refund {self.square_refund_id}"
            )
        else:
            _logger.warning(
                f"No return pickings found after creation for refund {self.square_refund_id}"
            )

    def _create_credit_note(self):
        """
        Create credit note for the refund - supports both full and partial refunds
        """
        sale_order = self.sale_order_id

        # Find the original invoice
        invoice = sale_order.invoice_ids.filtered(
            lambda inv: inv.move_type == "out_invoice" and inv.state == "posted"
        )

        if not invoice:
            raise UserError("Aucune facture validée trouvée pour créer l'avoir")

        _logger.info(
            f"Creating credit note for invoice {invoice.name} and refund {self.square_refund_id} - Amount: {self.refund_amount}"
        )

        # Create partial credit note for specific refund amount
        return self._create_partial_credit_note(invoice)

    def _create_partial_credit_note(self, invoice):
        """
        Create a partial credit note for the specific refund amount
        """
        # Get bot user for operations to ensure proper tracking messages
        bot_user = self._get_bot_user()

        # Fetch order details from Square to get accurate quantities
        order_details = self._fetch_order_details_from_square()

        # Use the reversal wizard to create the credit note with bot user context
        _logger.info(
            f"Creating credit note reversal for invoice {invoice.name} "
            f"with refund ID {self.square_refund_id}"
        )

        credit_note_wizard = (
            self.env["account.move.reversal"]
            .with_user(bot_user)
            .with_context(active_model="account.move", active_ids=invoice.ids)
            .create(
                {
                    "reason": f"Square Refund: {self.square_refund_id}",
                    "journal_id": invoice.journal_id.id,
                }
            )
        )

        # Create the reversal and get the credit note directly from the result
        result = credit_note_wizard.reverse_moves()
        credit_note_id = result.get("res_id")

        if not credit_note_id:
            _logger.error("Reversal wizard did not return a credit note ID")
            raise UserError("Échec de création de l'avoir - aucun ID retourné")

        # Get the credit note directly by ID
        credit_note = self.env["account.move"].browse(credit_note_id)
        if not credit_note.exists():
            _logger.error(f"Credit note with ID {credit_note_id} does not exist")
            raise UserError("L'avoir créé n'existe pas")

        _logger.info(f"Found credit note: {credit_note.name} (ID: {credit_note.id})")

        # Post the credit note FIRST (required for payment creation)
        credit_note.with_user(bot_user).action_post()

        # Update order line quantities based on Square order details
        self._update_order_line_quantities_from_square(order_details)

        # Create payment for the posted credit note
        processor = self.env["square.order.processor"]
        payment_created = processor._create_payment_for_credit_note(
            credit_note, self.sale_order_id, bot_user, refund_amount=self.refund_amount
        )

        # Add chatter message
        try:
            self.sale_order_id.with_user(bot_user).with_context(
                mail_auto_subscribe_no_notify=True, mail_create_nosubscribe=True
            ).message_post(
                body=f"Avoir Square partiel créé - {credit_note.name}, Montant: {credit_note.amount_total} {credit_note.currency_id.name}, Paiement: {'Créé' if payment_created else 'En attente'}",
                subject="Intégration Square : Avoir Partiel Créé et Traité",
                message_type="comment",
            )
            _logger.info(
                f"Posted chatter message for partial credit note {credit_note.name}"
            )
        except Exception as e:
            _logger.warning(
                f"Could not post chatter message for credit note {credit_note.name}: {str(e)}"
            )

        _logger.info(
            f"Created and processed partial credit note {credit_note.name} for refund {self.square_refund_id}"
        )
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
        """
        Update sale order quantities after refund completion
        Determines whether it's a full or partial refund and handles accordingly
        """
        # Check if this is a full refund

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

    def _adjust_credit_note_for_partial_refund(
        self, credit_note, invoice, order_details=None
    ):
        """
        Adjust the credit note lines to match the partial refund amount
        Since Square doesn't provide line-level refund details, we adjust proportionally
        """
        if credit_note.amount_total == 0:
            _logger.warning(
                f"Credit note {credit_note.name} has zero amount, skipping adjustment"
            )
            return

        # Get bot user for operations to ensure proper tracking messages
        bot_user = self._get_bot_user()

        # Calculate the adjustment ratio
        adjustment_ratio = self.refund_amount / credit_note.amount_total

        _logger.info(
            f"Adjusting credit note {credit_note.name} for partial refund: "
            f"Original amount: {credit_note.amount_total}, Target amount: {self.refund_amount}, "
            f"Adjustment ratio: {adjustment_ratio:.4f}"
        )

        # Calculate total credit note amount before adjustment
        total_credit_amount = abs(credit_note.amount_total)

        # For partial refunds, adjust line amounts proportionally by modifying price_unit
        # This preserves quantities but adjusts the amounts to match the refund
        if total_credit_amount > 0 and credit_note.invoice_line_ids:
            adjustment_ratio = self.refund_amount / total_credit_amount

            _logger.info(
                f"Applying adjustment ratio {adjustment_ratio:.4f} to all credit note lines "
                f"to achieve refund amount {self.refund_amount}"
            )

            lines_adjusted = 0
            for line in credit_note.invoice_line_ids:
                # Debug: Log line information to understand structure
                _logger.debug(
                    f"Credit note line '{line.name}': price_unit={line.price_unit}, "
                    f"quantity={line.quantity}, amount={line.price_subtotal}"
                )

                # Adjust lines with non-zero amounts (both positive and negative)
                if line.price_unit != 0:
                    original_price = line.price_unit
                    # Keep quantity the same, adjust price proportionally
                    new_price = original_price * adjustment_ratio

                    # Use write with bot user context to ensure proper tracking messages
                    line.with_user(bot_user).with_context(
                        check_move_validity=False
                    ).write(
                        {
                            "price_unit": new_price,
                        }
                    )

                    lines_adjusted += 1
                    _logger.info(
                        f"Adjusted credit note line '{line.name}': "
                        f"Price {original_price:.2f} -> {new_price:.2f} "
                        f"(quantity remains {line.quantity:.2f})"
                    )
                else:
                    _logger.debug(
                        f"Skipping credit note line '{line.name}' with zero price_unit"
                    )

            if lines_adjusted == 0:
                _logger.warning(
                    f"No credit note lines were adjusted for refund {self.square_refund_id}. "
                    f"Credit note may have unexpected structure."
                )
        else:
            _logger.warning(
                f"Cannot adjust credit note {credit_note.name}: total_credit_amount={total_credit_amount}, "
                f"line_count={len(credit_note.invoice_line_ids)}"
            )

        # The credit note amounts will be recomputed when posted
        # No need to manually recompute here as it will happen automatically

        _logger.info(
            f"Credit note {credit_note.name} lines adjusted for partial refund amount {self.refund_amount}. "
            f"Amount before posting: {credit_note.amount_total} (will be recalculated after posting)"
        )

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

        except Exception as e:
            _logger.error(f"Error handling cancelled refund: {str(e)}")

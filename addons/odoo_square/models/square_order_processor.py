# -*- coding: utf-8 -*-
from decimal import Decimal
from odoo import models, fields
import logging

_logger = logging.getLogger(__name__)


class SquareOrderProcessor(models.Model):
    _name = "square.order.processor"
    _description = "Square Order Processor"

    def _get_square_bot_user(self):
        """Get the Square integration bot user"""
        try:
            bot_user = self.env.ref("odoo_square.user_square_bot")
            # Ensure the bot user has a partner_id
            if not bot_user.partner_id:
                _logger.warning(
                    f"Square bot user {bot_user.name} has no partner_id, this may cause issues"
                )
            return bot_user
        except ValueError:
            # Fallback to admin user if bot user doesn't exist
            _logger.warning("Square bot user not found, falling back to admin user")
            admin_user = self.env.ref("base.user_admin")
            return admin_user

    def _check_existing_order(self, square_order_id):
        """
        Check if an order with the given Square order ID already exists.
        This is the centralized duplicate detection method with database locking.
        """
        if not square_order_id:
            return None

        # Use SELECT FOR UPDATE to prevent race conditions during concurrent webhook processing
        # This ensures atomicity when checking for duplicates
        try:
            self.env.cr.execute(
                """
                SELECT id FROM sale_order 
                WHERE square_order_id = %s 
                FOR UPDATE NOWAIT
                """,
                (square_order_id,),
            )
            result = self.env.cr.fetchone()

            if result:
                existing_order = self.env["sale.order"].browse(result[0])
                _logger.debug(
                    f"Found existing order for Square ID {square_order_id}: {existing_order.name}"
                )
                return existing_order

        except Exception as e:
            # If we can't acquire the lock immediately, another process is creating this order
            # Fall back to regular search which might find the order if it was just created
            _logger.warning(
                f"Could not acquire lock for Square order {square_order_id}, checking normally: {str(e)}"
            )
            existing_order = self.env["sale.order"].search(
                [("square_order_id", "=", square_order_id)], limit=1
            )

            if existing_order:
                _logger.debug(
                    f"Found existing order for Square ID {square_order_id}: {existing_order.name}"
                )
                return existing_order

        return None

    def process_square_order(self, square_order_data):
        """
        Main entry point for processing Square order data from webhooks
        Creates new orders only, keeps them in draft state
        """
        square_order_id = square_order_data.get("order_id")
        _logger.info(f"Processing Square order: {square_order_id}")

        # EXCHANGE ORDER CHECK: Detect if this is an exchange order (has returns with source_order_id)
        # Exchange orders should NOT create new Odoo orders - they modify the source order
        returns = square_order_data.get("returns", [])
        if returns:
            source_order_id = None
            for return_obj in returns:
                if isinstance(return_obj, dict):
                    source_order_id = return_obj.get("source_order_id")
                    if source_order_id:
                        break
            
            if source_order_id:
                _logger.info(
                    f"Order {square_order_id} is an exchange order (source: {source_order_id}), skipping creation. "
                    f"Exchange should be processed via payment.updated webhook."
                )
                
                # Log to integration dashboard
                self.env["square.integration.log"].log_square_event(
                    event_type="order_created",
                    title=f"Exchange order detected - {square_order_id}",
                    description=f"""
                        <p><strong>Square Exchange Order Detected</strong></p>
                        <ul>
                            <li>Square Exchange Order ID: <strong>{square_order_id}</strong></li>
                            <li>Source Order ID: <strong>{source_order_id}</strong></li>
                            <li>Action: Creation ignored (exchange is processed via payment.updated)</li>
                            <li>Note: Cet ordre représente la transaction d'échange et ne doit pas créer de nouveau devis</li>
                        </ul>
                    """,
                    status="info",
                    square_order_id=square_order_id,
                )
                
                return {
                    "status": "success",
                    "message": f"Exchange order {square_order_id} detected, skipping creation (processed via payment webhook)",
                    "square_order_id": square_order_id,
                    "source_order_id": source_order_id,
                    "is_exchange": True,
                }

        # IDEMPOTENCY CHECK: Ensure we don't create duplicate orders
        # This is the single point of duplicate detection to prevent race conditions
        existing_order = self._check_existing_order(square_order_id)
        if existing_order:
            _logger.info(
                f"Order {square_order_id} already exists as {existing_order.name}, skipping creation (idempotent)"
            )
            return {
                "status": "success",
                "sale_order_id": existing_order.id,
                "sale_order_name": existing_order.name,
                "square_order_id": square_order_id,
                "message": "Order already exists (idempotent response)",
            }

        # Create new order in draft state (only for OPEN orders)
        square_order_state = square_order_data.get("state")
        if square_order_state != "OPEN":
            _logger.info(
                f"Skipping order creation for non-OPEN state: {square_order_state}"
            )
            return {
                "status": "success",
                "message": f"Order creation skipped for state: {square_order_state}",
                "square_order_id": square_order_id,
            }

        try:
            # Get bot user for order creation
            bot_user = self._get_square_bot_user()
            sale_order = (
                self.env["sale.order"]
                .with_user(bot_user)
                .create_from_square(square_order_data)
            )

            if not sale_order:
                raise ValueError("Failed to create sale order")

            _logger.info(
                f"Successfully created draft order {sale_order.name} from Square order {square_order_id}"
            )

            return {
                "status": "success",
                "sale_order_id": sale_order.id,
                "sale_order_name": sale_order.name,
                "square_order_id": sale_order.square_order_id,
            }

        except Exception as e:
            # Check if this is a duplicate constraint error due to race condition
            error_str = str(e)
            if (
                "duplicate key value" in error_str
                and "square_order_id_unique" in error_str
            ) or ("IntegrityError" in error_str and square_order_id in error_str):
                _logger.warning(
                    f"Race condition detected for Square order {square_order_id}. Another process created the order first."
                )
                # Try to find the existing order that was created by the other process
                existing_order = self._check_existing_order(square_order_id)
                if existing_order:
                    _logger.info(
                        f"Found order created by concurrent process: {existing_order.name}"
                    )
                    return {
                        "status": "success",
                        "sale_order_id": existing_order.id,
                        "sale_order_name": existing_order.name,
                        "square_order_id": square_order_id,
                        "message": "Order created by concurrent process (idempotent response)",
                    }
                else:
                    _logger.error(
                        f"Could not find order after duplicate constraint error for {square_order_id}"
                    )

            _logger.error(
                f"Error creating Square order {square_order_id}: {str(e)}",
                exc_info=True,
            )
            raise

    def process_square_order_update(self, square_order_data, sale_order):
        """
        Process updates to existing Square orders
        Only create invoice and stock moves when order state is COMPLETED
        """
        square_order_id = square_order_data.get("order_id")
        square_order_state = square_order_data.get("state")

        _logger.info(
            f"Processing Square order update: {square_order_id} for sale order {sale_order.name}, state: {square_order_state}"
        )

        try:
            result = {
                "status": "updated",
                "sale_order_id": sale_order.id,
                "sale_order_name": sale_order.name,
                "square_order_id": square_order_id,
            }

            # Handle different order states and changes
            if square_order_state == "COMPLETED":
                _logger.info(
                    f"Order {square_order_id} is COMPLETED, processing invoice and stock moves"
                )
                # Process completion
                self._process_order_completion(sale_order, square_order_data)

            elif square_order_state in ["OPEN", "DRAFT"]:
                # Check for line changes and sync them
                self._sync_order_line_changes(sale_order, square_order_data)

            elif square_order_state == "CANCELED":
                # Handle order cancellation
                self._process_order_cancellation(sale_order, square_order_data)

            else:
                _logger.info(
                    f"Order {square_order_id} state is {square_order_state}, no specific processing needed"
                )

                # Log to integration dashboard
                self.env["square.integration.log"].log_square_event(
                    event_type="order_updated",
                    title=f"Order update received for {sale_order.name}",
                    description=f"""
                        <p><strong>Square Order Update Received</strong></p>
                        <ul>
                            <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                            <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                            <li>État Square: {square_order_state}</li>
                            <li>État Odoo: {sale_order.state}</li>
                            <li>Action: Notification de mise à jour traitée</li>
                        </ul>
                    """,
                    status="info",
                    square_order_id=square_order_id,
                    sale_order_id=sale_order.id,
                )

            _logger.info(
                f"Successfully processed Square order update for {sale_order.name}"
            )

            return result

        except Exception as e:
            _logger.error(
                f"Error processing Square order update: {str(e)}", exc_info=True
            )
            raise

    def _process_order_completion(self, sale_order, square_order_data):
        """
        Process order completion - create invoice and stock movements
        """
        square_order_id = square_order_data.get("order_id")
        square_order_state = square_order_data.get("state")

        # Get bot user for all operations
        bot_user = self._get_square_bot_user()

        # Step 1: Confirm the order if still in draft using standard Odoo method
        if sale_order.state == "draft":
            try:
                # Use Odoo's standard confirmation with proper context
                sale_order.with_user(bot_user).with_context(
                    mail_auto_subscribe_no_notify=True,
                    mail_create_nosubscribe=True,
                    mail_auto_subscribe=False,
                ).action_confirm()

                _logger.info(
                    f"Confirmed sale order {sale_order.name} using standard action_confirm()"
                )
            except Exception as e:
                _logger.error(f"Failed to confirm order {sale_order.name}: {str(e)}")
                raise
        elif sale_order.state in ["sale", "done"]:
            _logger.info(
                f"Order {sale_order.name} already confirmed (state: {sale_order.state}), skipping confirmation"
            )

        # Step 2: Generate and validate invoice
        if not sale_order.invoice_ids.filtered(lambda inv: inv.state == "posted"):
            invoice = self._create_and_validate_invoice(sale_order)
            if invoice:
                # Store invoice info for return
                result = {"invoice_id": invoice.id, "invoice_name": invoice.name}

        # Step 3: Create stock movements
        square_location_id = square_order_data.get("location_id")
        self._create_stock_movements(sale_order, square_location_id)

        # TODO: fix TTC ammout
        # Step 4: Recalculate totals
        # sale_order._amount_all()

        # Log to integration dashboard
        self.env["square.integration.log"].log_square_event(
            event_type="order_updated",
            title=f"Order completed and processed",
            description=f"""
                <p><strong>Commande Square Terminée et Traîtée</strong></p>
                <ul>
                    <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                    <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                    <li>État Square: {square_order_state}</li>
                    <li>État Odoo: {sale_order.state}</li>
                    <li>Action: Invoice and stock movements created</li>
                </ul>
            """,
            status="success",
            square_order_id=square_order_id,
            sale_order_id=sale_order.id,
        )

    def _sync_order_line_changes(self, sale_order, square_order_data):
        """
        Sync order line changes from Square to Odoo
        """
        square_order_id = square_order_data.get("order_id")
        square_lines = square_order_data.get("line_items", [])

        _logger.info(f"Syncing line changes for order {sale_order.name}")

        # Get current Odoo lines
        odoo_lines = {
            line.square_line_id: line
            for line in sale_order.order_line
            if line.square_line_id
        }

        # Track changes
        added_lines = []
        updated_lines = []
        removed_lines = []

        # Process Square lines
        for square_line in square_lines:
            square_line_id = square_line.get("uid")
            quantity = int(square_line.get("quantity", "1"))
            price_data = square_line.get("total_money", {})
            amount = round(float(price_data.get("amount", 0)) / 100.0, 2)

            if square_line_id in odoo_lines:
                # Update existing line
                odoo_line = odoo_lines[square_line_id]
                if odoo_line.product_uom_qty != quantity:
                    odoo_line.write({"product_uom_qty": quantity})
                    updated_lines.append(
                        f"{odoo_line.product_id.name}: {odoo_line.product_uom_qty} -> {quantity}"
                    )
                odoo_lines.pop(square_line_id)
            else:
                # This is a new line - for now, just log it
                # In a full implementation, you might want to add it
                added_lines.append(f"{square_line.get('name', 'Unknown')}: {quantity}")

        # Remaining Odoo lines are potentially removed
        for remaining_line in odoo_lines.values():
            removed_lines.append(
                f"{remaining_line.product_id.name}: {remaining_line.product_uom_qty} -> 0"
            )

        # Log changes if any
        if added_lines or updated_lines or removed_lines:
            self.env["square.integration.log"].log_square_event(
                event_type="order_updated",
                title=f"Line changes synchronized for {sale_order.name}",
                description=f"""
                    <p><strong>Square Order Line Changes Synchronized</strong></p>
                    <ul>
                        <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                        <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                        <li>Lines Added: {len(added_lines)}</li>
                        <li>Lines Modified: {len(updated_lines)}</li>
                        <li>Lines Removed: {len(removed_lines)}</li>
                    </ul>
                    {"<p><strong>Changements:</strong></p>" if updated_lines else ""}
                    <ul>
                        {"".join(f"<li>Modified: {change}</li>" for change in updated_lines)}
                    </ul>
                """,
                status="info",
                square_order_id=square_order_id,
                sale_order_id=sale_order.id,
            )
        else:
            _logger.debug(f"No line changes detected for order {sale_order.name}")

    def _process_order_cancellation(self, sale_order, square_order_data):
        """
        Process order cancellation from Square
        Handles different order states appropriately
        """
        square_order_id = square_order_data.get("order_id")

        _logger.info(
            f"Processing cancellation for order {sale_order.name} (current state: {sale_order.state})"
        )

        # Handle different order states
        if sale_order.state == "cancel":
            _logger.info(f"Order {sale_order.name} is already cancelled")
            return

        elif sale_order.state == "done":
            _logger.warning(
                f"Order {sale_order.name} is already done/locked - processing refund instead"
            )
            # For completed orders, we should create a refund/credit note
            self._process_completed_order_cancellation(sale_order, square_order_data)
            return

        # For draft and sale states, we can cancel
        elif sale_order.state in ["draft", "sale"]:
            try:
                # Get bot user for the cancellation
                bot_user = self._get_square_bot_user()

                # Cancel the order
                sale_order.with_user(bot_user).action_cancel()

                _logger.info(f"Successfully cancelled order {sale_order.name}")

                # Log successful cancellation
                self.env["square.integration.log"].log_square_event(
                    event_type="order_updated",
                    title=f"order cancelled",
                    description=f"""
                        <p><strong>Commande Square Annulée avec Succès</strong></p>
                        <ul>
                            <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                            <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                            <li>État Précédent: {sale_order.state}</li>
                            <li>New State: Cancelled</li>
                            <li>Action: order cancelled dans Odoo via webhook Square</li>
                        </ul>
                    """,
                    status="warning",
                    square_order_id=square_order_id,
                    sale_order_id=sale_order.id,
                )

            except Exception as e:
                _logger.error(f"Failed to cancel order {sale_order.name}: {str(e)}")
                # Log the error
                self.env["square.integration.log"].log_square_event(
                    event_type="order_updated",
                    title=f"Failed to cancel order {sale_order.name}",
                    description=f"""
                        <p><strong>Failed to Cancel Square Order</strong></p>
                        <ul>
                            <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                            <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                            <li>Erreur: {str(e)}</li>
                            <li>Action: Manual cancellation may be necessary</li>
                        </ul>
                    """,
                    status="error",
                    square_order_id=square_order_id,
                    sale_order_id=sale_order.id,
                )
                raise

        else:
            _logger.warning(
                f"Order {sale_order.name} in unexpected state: {sale_order.state}"
            )
            # Log this as an issue
            self.env["square.integration.log"].log_square_event(
                event_type="order_updated",
                title=f"État de commande inattendu pour l'annulation: {sale_order.name}",
                description=f"""
                    <p><strong>Annulation Commande Square - État Inattendu</strong></p>
                    <ul>
                        <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                        <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                        <li>État Actuel: {sale_order.state}</li>
                        <li>Problème: Commande dans un état inattendu pour l'annulation</li>
                        <li>Action: Révision manuelle requise</li>
                    </ul>
                """,
                status="error",
                square_order_id=square_order_id,
                sale_order_id=sale_order.id,
            )

    def _process_completed_order_cancellation(self, sale_order, square_order_data):
        """
        Process cancellation of a completed order by creating a credit note/refund
        """
        square_order_id = square_order_data.get("order_id")

        _logger.info(f"Processing refund for completed order {sale_order.name}")

        try:
            # Get bot user
            bot_user = self._get_square_bot_user()

            # Check if the order has invoices that need to be refunded
            if not sale_order.invoice_ids:
                _logger.warning(
                    f"No invoices found for completed order {sale_order.name}"
                )
                self.env["square.integration.log"].log_square_event(
                    event_type="order_updated",
                    title=f"Aucune facture à rembourser pour {sale_order.name}",
                    description=f"""
                        <p><strong>Annulation Commande Square - Aucune Facture</strong></p>
                        <ul>
                            <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                            <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                            <li>État: Terminé (Complété)</li>
                            <li>Problème: Aucune facture trouvée pour créer l'avoir</li>
                            <li>Action: Une intervention manuelle peut être nécessaire</li>
                        </ul>
                    """,
                    status="warning",
                    square_order_id=square_order_id,
                    sale_order_id=sale_order.id,
                )
                return

            # Get posted invoices
            posted_invoices = sale_order.invoice_ids.filtered(
                lambda inv: inv.state == "posted"
            )

            if not posted_invoices:
                _logger.warning(f"No posted invoices found for order {sale_order.name}")
                return

            # Create credit notes for posted invoices
            for invoice in posted_invoices:
                try:
                    # Create credit note using Odoo's standard method
                    credit_note_wizard = (
                        self.env["account.move.reversal"]
                        .with_user(bot_user)
                        .create(
                            {
                                "move_ids": [(6, 0, invoice.ids)],
                                "journal_id": invoice.journal_id.id,
                                "reason": f"Square Order Cancellation - {square_order_id}",
                                "refund_method": "cancel",  # This creates a credit note that can be used to reconcile
                            }
                        )
                    )

                    # Generate the credit note
                    credit_note_result = credit_note_wizard.reverse_moves()
                    credit_note = self.env["account.move"].browse(
                        credit_note_result["res_id"]
                    )

                    # Validate the credit note
                    if credit_note.state == "draft":
                        credit_note.with_user(bot_user).action_post()

                    _logger.info(
                        f"Created credit note {credit_note.name} for invoice {invoice.name}"
                    )

                    # Log the credit note creation
                    self.env["square.integration.log"].log_square_event(
                        event_type="order_updated",
                        title=f"Avoir créé pour la commande annulée {sale_order.name}",
                        description=f"""
                            <p><strong>Annulation Commande Square - Avoir Créé</strong></p>
                            <ul>
                                <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                                <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                                <li>Facture Originale: <strong>{invoice.name}</strong></li>
                                <li>Avoir: <strong>{credit_note.name}</strong></li>
                                <li>Montant: {credit_note.amount_total} {credit_note.currency_id.name}</li>
                                <li>Action: Avoir créé et comptabilisé</li>
                            </ul>
                        """,
                        status="info",
                        square_order_id=square_order_id,
                        sale_order_id=sale_order.id,
                    )

                except Exception as e:
                    _logger.error(
                        f"Failed to create credit note for invoice {invoice.name}: {str(e)}"
                    )
                    # Log the error but continue with other invoices
                    self.env["square.integration.log"].log_square_event(
                        event_type="order_updated",
                        title=f"Échec de création d'avoir pour {sale_order.name}",
                        description=f"""
                            <p><strong>Annulation Commande Square - Échec Avoir</strong></p>
                            <ul>
                                <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                                <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                                <li>Facture: <strong>{invoice.name}</strong></li>
                                <li>Erreur: {str(e)}</li>
                                <li>Action: Création manuelle d'avoir nécessaire</li>
                            </ul>
                        """,
                        status="error",
                        square_order_id=square_order_id,
                        sale_order_id=sale_order.id,
                    )

        except Exception as e:
            _logger.error(
                f"Error processing completed order cancellation for {sale_order.name}: {str(e)}"
            )
            # Log the overall error
            self.env["square.integration.log"].log_square_event(
                event_type="order_updated",
                title=f"Échec de l'annulation de commande terminée pour {sale_order.name}",
                description=f"""
                    <p><strong>Erreur d'Annulation de Commande Square</strong></p>
                    <ul>
                        <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                        <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                        <li>Erreur: {str(e)}</li>
                        <li>Action: Traitement manuel de remboursement nécessaire</li>
                    </ul>
                """,
                status="error",
                square_order_id=square_order_id,
                sale_order_id=sale_order.id,
            )
            raise

    def process_product_exchange(self, sale_order, order_data, payment_data):
        """
        Process product exchange from Square.

        New flow (Return + Credit Note + New Sale + Reconcile):
        1. Parse exchange data (returned and new products)
        2. Idempotency check (skip if exchange SO already exists)
        3. Create return pickings for returned products
        4. Create credit note for returned items
        5. Create new independent SO for replacement products
        6. Confirm, deliver, and invoice the new SO
        7. Reconcile credit note against new invoice + handle delta
        """
        square_order_id = order_data.get("order_id")
        exchange_order_id = order_data.get("id") or square_order_id

        _logger.info(
            f"Processing product exchange for order {sale_order.name} "
            f"(Square exchange ID: {exchange_order_id})"
        )

        try:
            # Idempotency: check if exchange was FULLY processed (not just SO exists)
            # Exchange is fully processed only if return pickings exist for original order
            existing_exchange_so = None
            if exchange_order_id:
                existing_exchange_so = self.env["sale.order"].search(
                    [("square_order_id", "=", exchange_order_id)], limit=1
                )
                if existing_exchange_so:
                    # Check if return pickings exist for original order (from exchange)
                    # Return pickings have incoming type and come from Customers location
                    customer_location = self.env.ref("stock.stock_location_customers")
                    return_pickings = self.env["stock.picking"].search([
                        ("origin", "=", sale_order.name),
                        ("picking_type_id.code", "=", "incoming"),
                        ("location_id", "=", customer_location.id),
                        ("state", "=", "done"),
                    ])
                    if return_pickings:
                        _logger.info(
                            f"Exchange fully processed: SO {existing_exchange_so.name} exists "
                            f"and return pickings {return_pickings.mapped('name')} exist, skipping"
                        )
                        return {
                            "status": "skipped",
                            "message": f"Exchange already processed as {existing_exchange_so.name}",
                            "sale_order_id": existing_exchange_so.id,
                            "sale_order_name": existing_exchange_so.name,
                        }
                    else:
                        _logger.info(
                            f"Exchange SO {existing_exchange_so.name} exists but no return pickings "
                            f"found - continuing with exchange flow to create return pickings"
                        )

            # Parse return line items and current line items
            returns = order_data.get("returns", [])
            current_line_items = order_data.get("line_items", [])

            return_line_items = []
            if returns:
                for return_obj in returns:
                    if isinstance(return_obj, dict):
                        items = return_obj.get("return_line_items", [])
                        return_line_items.extend(items)
                        _logger.info(f"Found {len(items)} return line items in return object")

            # Fallback: check if return_line_items exists at order level (older API)
            if not return_line_items and "return_line_items" in order_data:
                return_line_items = order_data.get("return_line_items", [])

            if not return_line_items:
                _logger.error(f"No return line items found in order {square_order_id}")
                return {
                    "status": "error",
                    "message": "No return line items found in order data",
                }

            _logger.info(f"Processing {len(return_line_items)} return line items")

            # Map catalog_object_id to quantities for returns and new items
            returned_products = {}  # catalog_object_id -> quantity
            new_products = {}  # catalog_object_id -> {quantity, name, amount, line_data}

            for return_item in return_line_items:
                catalog_object_id = return_item.get("catalog_object_id")
                quantity_str = return_item.get("quantity", "1")
                quantity = abs(int(quantity_str))

                if catalog_object_id:
                    returned_products[catalog_object_id] = (
                        returned_products.get(catalog_object_id, 0) + quantity
                    )
                    _logger.info(
                        f"Return detected: catalog_object_id={catalog_object_id}, "
                        f"qty={quantity}, name={return_item.get('name')}"
                    )
                else:
                    _logger.warning(
                        f"Return item without catalog_object_id: {return_item.get('name')}"
                    )

            _logger.info(
                f"Total returned products: {len(returned_products)}, "
                f"catalog_ids: {list(returned_products.keys())}"
            )

            for line_item in current_line_items:
                catalog_object_id = line_item.get("catalog_object_id")
                if not catalog_object_id:
                    continue

                # Check if this is a new product (not in original order lines)
                existing_line = sale_order.order_line.filtered(
                    lambda line, cid=catalog_object_id: line.square_catalog_id == cid
                )

                if not existing_line:
                    quantity = int(line_item.get("quantity", "1"))
                    price_data = line_item.get("total_money", {})
                    amount = round(float(price_data.get("amount", 0)) / 100.0, 2)

                    new_products[catalog_object_id] = {
                        "quantity": quantity,
                        "name": line_item.get("name", "Unknown Product"),
                        "amount": amount,  # TTC in euros
                        "line_data": line_item,
                        "square_line_id": line_item.get("uid"),
                    }
                    _logger.info(
                        f"New product detected: {line_item.get('name')}, "
                        f"qty={quantity}, catalog_id={catalog_object_id}"
                    )

            if not returned_products and not new_products:
                _logger.warning(
                    f"No exchange detected - no returned or new products "
                    f"found for order {square_order_id}"
                )
                return {"status": "ignored", "message": "No exchange detected"}

            # Step 1: Create return pickings for returned products
            self._create_return_pickings_for_exchange(sale_order, returned_products)

            # Step 2: Create credit note for returned items
            credit_note = self._create_exchange_credit_note(
                sale_order, returned_products, order_data
            )

            # Step 3: Get or create new independent SO for replacement products
            # If existing_exchange_so was found earlier, use it instead of creating new
            new_sale_order = existing_exchange_so
            _logger.info(
                f"Step 3: existing_exchange_so={existing_exchange_so.name if existing_exchange_so else None}, "
                f"new_products count={len(new_products)}"
            )
            if not new_sale_order and new_products:
                new_sale_order = self._create_exchange_sale_order(
                    sale_order, new_products, order_data, exchange_order_id
                )

            # Step 4: Confirm, deliver, and invoice the new SO
            new_invoice = None
            if new_sale_order:
                new_invoice = self._complete_exchange_sale_order(new_sale_order)

            # Step 5: Reconcile credit note + new invoice + handle delta
            bot_user = self._get_square_bot_user()
            if credit_note and new_invoice:
                self._reconcile_exchange_documents(
                    credit_note, new_invoice, new_sale_order, sale_order
                )
            elif credit_note and not new_invoice:
                # Only returns, no new products — refund the credit note
                self._create_payment_for_credit_note(credit_note, sale_order, bot_user)
            elif new_invoice and not credit_note:
                # Only new products, no returns — register payment for new invoice
                self._create_payment_for_invoice(new_invoice, new_sale_order, bot_user)

            # Log successful exchange processing
            new_so_name = new_sale_order.name if new_sale_order else "N/A"
            credit_note_name = credit_note.name if credit_note else "N/A"
            new_invoice_name = new_invoice.name if new_invoice else "N/A"

            self.env["square.integration.log"].log_square_event(
                event_type="order_updated",
                title=f"Échange traité: {sale_order.name} → {new_so_name}",
                description=f"""
                    <p><strong>Échange Square Traité avec Succès</strong></p>
                    <ul>
                        <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                        <li>Commande Originale: <strong>{sale_order.name}</strong></li>
                        <li>Nouvelle Commande: <strong>{new_so_name}</strong></li>
                        <li>Avoir: <strong>{credit_note_name}</strong></li>
                        <li>Nouvelle Facture: <strong>{new_invoice_name}</strong></li>
                        <li>Produits Retournés: {len(returned_products)}</li>
                        <li>Nouveaux Produits: {len(new_products)}</li>
                        <li>Date Échange: {fields.Datetime.now()}</li>
                    </ul>
                """,
                status="success",
                square_order_id=square_order_id,
                sale_order_id=sale_order.id,
            )

            return {
                "status": "success",
                "original_order_id": sale_order.id,
                "original_order_name": sale_order.name,
                "new_order_id": new_sale_order.id if new_sale_order else None,
                "new_order_name": new_so_name,
                "returned_products": len(returned_products),
                "new_products": len(new_products),
                "credit_note": credit_note_name,
                "new_invoice": new_invoice_name,
            }

        except Exception as e:
            _logger.error(
                f"Error processing exchange for order {sale_order.name}: {str(e)}",
                exc_info=True,
            )

            self.env["square.integration.log"].log_square_event(
                event_type="order_updated",
                title=f"Échec du traitement d'échange pour {sale_order.name}",
                description=f"""
                    <p><strong>Erreur de Traitement d'Échange Square</strong></p>
                    <ul>
                        <li>ID Commande Square: <strong>{square_order_id}</strong></li>
                        <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                        <li>Erreur: {str(e)}</li>
                        <li>Action: Traitement manuel nécessaire</li>
                    </ul>
                """,
                status="error",
                square_order_id=square_order_id,
                sale_order_id=sale_order.id,
            )

            raise
    
    def _create_return_pickings_for_exchange(self, sale_order, returned_products):
        """Create return pickings for products returned in an exchange"""
        try:
            warehouse = self._get_configured_warehouse()
            
            if not warehouse:
                _logger.error("No warehouse configured for return pickings")
                return []
            
            # Check if return pickings already exist for this order (idempotency)
            customer_location = self.env.ref("stock.stock_location_customers")
            existing_returns = self.env["stock.picking"].search([
                ("picking_type_id.code", "=", "incoming"),
                ("location_id", "=", customer_location.id),
                ("state", "=", "done"),
                "|",
                ("origin", "=", sale_order.name),
                ("origin", "ilike", f"Retour de {sale_order.name.split('/')[0]}"),
            ])
            
            # Also check returns linked via the original delivery pickings
            delivery_pickings = sale_order.picking_ids.filtered(
                lambda p: p.picking_type_id.code == "outgoing" and p.state == "done"
            )
            for dp in delivery_pickings:
                linked_returns = self.env["stock.picking"].search([
                    ("origin", "ilike", f"Retour de {dp.name}"),
                    ("state", "=", "done"),
                ])
                existing_returns |= linked_returns
            
            if existing_returns:
                _logger.info(
                    f"Return pickings already exist for order {sale_order.name}: "
                    f"{existing_returns.mapped('name')}, skipping creation"
                )
                return existing_returns
            
            return_pickings = self.env["stock.picking"]
            
            if not delivery_pickings:
                _logger.warning(f"No validated delivery pickings found for order {sale_order.name}")
                # Create manual return picking
                return_picking = self._create_manual_return_picking(
                    sale_order, returned_products, warehouse
                )
                if return_picking:
                    return_pickings |= return_picking
            else:
                # Create return for each delivery picking
                for delivery_picking in delivery_pickings:
                    return_picking = self._create_return_from_picking(
                        delivery_picking, returned_products
                    )
                    if return_picking:
                        return_pickings |= return_picking
            
            return return_pickings
            
        except Exception as e:
            _logger.error(f"Error creating return pickings for exchange: {str(e)}", exc_info=True)
            return self.env["stock.picking"]
    
    def _create_manual_return_picking(self, sale_order, returned_products, warehouse):
        """Create a manual return picking when no validated delivery exists"""
        try:
            # Create return picking
            return_picking_vals = {
                "picking_type_id": warehouse.in_type_id.id,
                "location_id": self.env.ref("stock.stock_location_customers").id,
                "location_dest_id": warehouse.lot_stock_id.id,
                "origin": sale_order.name,
                "partner_id": sale_order.partner_id.id,
            }
            
            return_picking = self.env["stock.picking"].create(return_picking_vals)
            
            # Add move lines for returned products
            for catalog_id, quantity in returned_products.items():
                order_line = sale_order.order_line.filtered(
                    lambda l: l.square_catalog_id == catalog_id
                ).sorted(key=lambda l: l.id, reverse=True)
                
                if not order_line:
                    continue
                    
                order_line = order_line[0]
                
                move_vals = {
                    "name": f"Return: {order_line.product_id.name}",
                    "product_id": order_line.product_id.id,
                    "product_uom_qty": quantity,
                    "product_uom": order_line.product_uom.id,
                    "picking_id": return_picking.id,
                    "location_id": self.env.ref("stock.stock_location_customers").id,
                    "location_dest_id": warehouse.lot_stock_id.id,
                }
                
                self.env["stock.move"].create(move_vals)
            
            # Check if any moves were created
            if not return_picking.move_ids:
                _logger.warning(f"No moves created for return picking, skipping validation")
                return None
            
            # Confirm and assign
            return_picking.action_confirm()
            return_picking.action_assign()
            
            # Use _force_quantity_for_square to create move lines properly
            return_picking._force_quantity_for_square()
            
            # Validate with force_validate context
            return_picking.with_context(force_validate=True).button_validate()
            
            _logger.info(f"Created and validated manual return picking {return_picking.name}, state={return_picking.state}")
            return return_picking
            
        except Exception as e:
            _logger.error(f"Error creating manual return picking: {str(e)}", exc_info=True)
            return None
    
    def _create_return_from_picking(self, delivery_picking, returned_products):
        """Create return picking from original delivery picking"""
        try:
            # Use Odoo's return picking wizard
            return_wizard = self.env["stock.return.picking"].create({
                "picking_id": delivery_picking.id,
            })
            
            # Filter return lines to only include returned products
            lines_to_keep = []
            for line in return_wizard.product_return_moves:
                # Check if this product is in returned_products
                catalog_id = line.move_id.sale_line_id.square_catalog_id if line.move_id.sale_line_id else None
                
                if catalog_id in returned_products:
                    line.quantity = min(line.quantity, returned_products[catalog_id])
                    lines_to_keep.append(line.id)
                else:
                    # Remove this line
                    line.unlink()
            
            if not return_wizard.product_return_moves:
                _logger.warning(f"No matching products to return from picking {delivery_picking.name}")
                return None
            
            # Create the return
            result = return_wizard.create_returns()
            return_picking_id = result.get("res_id")
            
            if not return_picking_id:
                _logger.error(f"Failed to create return picking from {delivery_picking.name}")
                return None
            
            return_picking = self.env["stock.picking"].browse(return_picking_id)
            
            # Confirm and assign to ensure proper state
            if return_picking.state == 'draft':
                return_picking.action_confirm()
            if return_picking.state == 'confirmed':
                return_picking.action_assign()
            
            # Use _force_quantity_for_square to create move lines properly
            return_picking._force_quantity_for_square()
            
            # Validate with force_validate context
            return_picking.with_context(force_validate=True).button_validate()
            
            _logger.info(f"Created and validated return picking {return_picking.name}, state={return_picking.state}")
            return return_picking
            
        except Exception as e:
            _logger.error(f"Error creating return from picking: {str(e)}", exc_info=True)
            return None

    def _create_exchange_credit_note(self, sale_order, returned_products, order_data):
        """
        Create a partial credit note for returned items in an exchange.
        Only credits the returned products, not the full invoice.
        """
        try:
            # Find original posted invoice
            posted_invoices = sale_order.invoice_ids.filtered(
                lambda inv: inv.state == "posted" and inv.move_type == "out_invoice"
            )
            if not posted_invoices:
                _logger.error(
                    f"No posted invoice found for order {sale_order.name} to create credit note"
                )
                return None

            original_invoice = posted_invoices[0]
            bot_user = self._get_square_bot_user()

            # Get exchange date
            exchange_date = order_data.get("updated_at") or order_data.get("created_at")
            if exchange_date:
                from dateutil import parser as dt_parser
                credit_date = dt_parser.parse(exchange_date).date()
            else:
                credit_date = fields.Date.today()

            # Determine which products and quantities were returned
            returned_product_info = {}  # product_id -> (return_qty, price_unit)
            for catalog_id, return_qty in returned_products.items():
                order_lines = sale_order.order_line.filtered(
                    lambda line, cid=catalog_id: line.square_catalog_id == cid
                )
                for ol in order_lines:
                    if ol.product_id:
                        # Get price from original invoice line
                        inv_line = original_invoice.invoice_line_ids.filtered(
                            lambda l, pid=ol.product_id.id: l.product_id.id == pid
                        )
                        price = inv_line[0].price_unit if inv_line else ol.price_unit
                        returned_product_info[ol.product_id.id] = (return_qty, price, ol.tax_id)

            if not returned_product_info:
                _logger.error(f"No returned products found for credit note of {sale_order.name}")
                return None

            # Create credit note manually with proper reversed_entry_id link
            credit_note_vals = {
                "move_type": "out_refund",
                "partner_id": sale_order.partner_id.id,
                "invoice_origin": sale_order.name,
                "invoice_date": credit_date,
                "date": credit_date,
                "ref": f"Échange - Avoir: {sale_order.name}",
                "journal_id": original_invoice.journal_id.id,
                "reversed_entry_id": original_invoice.id,  # Critical: link to original invoice
                "invoice_line_ids": [],
            }

            # Add lines only for returned products
            for product_id, (qty, price, taxes) in returned_product_info.items():
                product = self.env["product.product"].browse(product_id)
                credit_note_vals["invoice_line_ids"].append((0, 0, {
                    "product_id": product_id,
                    "name": f"Retour: {product.name}",
                    "quantity": qty,
                    "price_unit": price,
                    "tax_ids": [(6, 0, taxes.ids)] if taxes else [],
                }))

            credit_note = self.env["account.move"].with_user(bot_user).create(credit_note_vals)
            credit_note.action_post()

            _logger.info(
                f"Created exchange credit note {credit_note.name} for {sale_order.name} "
                f"(amount={credit_note.amount_total}, reversed_entry={credit_note.reversed_entry_id.name})"
            )
            return credit_note

        except Exception as e:
            _logger.error(
                f"Error creating exchange credit note for {sale_order.name}: {str(e)}",
                exc_info=True,
            )
            return None

    def _create_exchange_sale_order(
        self, original_so, new_products, order_data, exchange_order_id
    ):
        """
        Create a new independent SO for replacement products in an exchange.
        The new SO gets the exchange order's Square ID for idempotency.
        """
        try:
            bot_user = self._get_square_bot_user()
            api_client = self.env["square.api.client"]

            # Re-check if SO was created by another process (race condition)
            existing_so = self.env["sale.order"].search(
                [("square_order_id", "=", exchange_order_id)], limit=1
            )
            if existing_so:
                _logger.info(
                    f"Exchange SO {existing_so.name} was created by another process, "
                    f"using existing instead of creating new"
                )
                return existing_so

            # Look up 20% VAT tax (same pattern as sale_order.py)
            vat_tax = self.env["account.tax"].search(
                [
                    ("amount", "=", 20.0),
                    ("type_tax_use", "=", "sale"),
                    ("amount_type", "=", "percent"),
                    ("price_include", "=", False),
                ],
                limit=1,
            )
            if not vat_tax:
                raise ValueError("20% VAT tax not found in Odoo configuration")

            warehouse = self._get_configured_warehouse()
            so_vals = {
                "partner_id": original_so.partner_id.id,
                "square_order_id": exchange_order_id,
                "origin": f"Échange: {original_so.name}",
                "warehouse_id": (
                    warehouse.id if warehouse else original_so.warehouse_id.id
                ),
            }

            new_so = (
                self.env["sale.order"]
                .with_user(bot_user)
                .with_context(
                    mail_auto_subscribe_no_notify=True,
                    mail_create_nosubscribe=True,
                    tracking_disable=True,
                )
                .create(so_vals)
            )

            _logger.info(
                f"Created exchange SO {new_so.name} for exchange order {exchange_order_id}"
            )

            # Add order lines for each new product
            for catalog_id, product_info in new_products.items():
                catalog_result = api_client.get_catalog_object(catalog_id)
                if not catalog_result.get("success"):
                    _logger.error(
                        f"Failed to fetch catalog object {catalog_id} from Square"
                    )
                    continue

                sku = catalog_result.get("sku")
                if not sku:
                    _logger.warning(f"No SKU found for catalog object {catalog_id}")
                    continue

                product = self.env["product.product"].search(
                    [("default_code", "=", sku)], limit=1
                )
                if not product:
                    _logger.error(f"Product with SKU {sku} not found in Odoo")
                    self.env["square.integration.log"].log_square_event(
                        event_type="order_updated",
                        title=f"Produit non trouvé pour l'échange: SKU {sku}",
                        description=f"""
                            <p><strong>Échange Square - Produit Non Trouvé</strong></p>
                            <ul>
                                <li>Commande Originale: <strong>{original_so.name}</strong></li>
                                <li>SKU: <strong>{sku}</strong></li>
                                <li>Nom Produit: {product_info['name']}</li>
                                <li>Action: Créer le produit manuellement ou vérifier le SKU</li>
                            </ul>
                        """,
                        status="error",
                        square_order_id=exchange_order_id,
                        sale_order_id=original_so.id,
                    )
                    continue

                # TTC → HT conversion using Decimal (same pattern as sale_order.py)
                quantity = product_info["quantity"]
                total_ttc = Decimal(str(product_info["amount"]))
                unit_price_ht = total_ttc / Decimal("1.2") / Decimal(str(quantity))

                line_vals = {
                    "order_id": new_so.id,
                    "product_id": product.id,
                    "name": product.display_name or product_info["name"],
                    "product_uom_qty": quantity,
                    "price_unit": float(unit_price_ht),
                    "tax_id": [(6, 0, vat_tax.ids)],
                    "square_line_id": product_info.get("square_line_id"),
                    "square_catalog_id": catalog_id,
                }

                self.env["sale.order.line"].with_user(bot_user).create(line_vals)
                _logger.info(
                    f"Added line to exchange SO: {product.name}, "
                    f"qty={quantity}, price_ht={float(unit_price_ht):.2f}"
                )

            if not new_so.order_line:
                _logger.warning(
                    f"No order lines created for exchange SO {new_so.name}, deleting"
                )
                new_so.unlink()
                return None

            return new_so

        except Exception as e:
            # Handle duplicate key constraint - SO was created by another process
            if "sale_order_square_order_id_unique" in str(e):
                _logger.info(
                    f"Duplicate key constraint hit - exchange SO was created by another process, "
                    f"fetching existing SO for {exchange_order_id}"
                )
                # Need to rollback and fetch existing
                self.env.cr.rollback()
                existing_so = self.env["sale.order"].search(
                    [("square_order_id", "=", exchange_order_id)], limit=1
                )
                if existing_so:
                    return existing_so
            _logger.error(
                f"Error creating exchange sale order: {str(e)}", exc_info=True
            )
            return None

    def _complete_exchange_sale_order(self, new_sale_order):
        """Confirm, deliver, and invoice the new exchange SO. Returns the posted invoice."""
        try:
            bot_user = self._get_square_bot_user()

            # Confirm (skip if already confirmed)
            if new_sale_order.state == 'draft':
                new_sale_order.action_confirm()
                _logger.info(f"Confirmed exchange SO {new_sale_order.name}")
            elif new_sale_order.state == 'sale':
                _logger.info(f"Exchange SO {new_sale_order.name} already confirmed, skipping confirmation")

            # Deliver: force validate all pickings
            for picking in new_sale_order.picking_ids:
                if picking.state not in ["done", "cancel"]:
                    if picking.state == "draft":
                        picking.with_user(bot_user).action_confirm()
                    if picking.state in ["confirmed", "waiting"]:
                        picking.with_user(bot_user).action_assign()

                    if picking.state in ["assigned", "confirmed", "waiting"]:
                        picking.with_user(bot_user).with_context(
                            force_validate=True,
                            mail_auto_subscribe_no_notify=True,
                            mail_create_nosubscribe=True,
                        ).button_validate()
                        _logger.info(f"Validated exchange picking {picking.name}")

            # Check if invoice already exists
            existing_invoices = new_sale_order.invoice_ids.filtered(
                lambda i: i.state == 'posted' and i.move_type == 'out_invoice'
            )
            if existing_invoices:
                _logger.info(f"Exchange SO {new_sale_order.name} already has invoice {existing_invoices[0].name}")
                return existing_invoices[0]

            # Invoice (do NOT create payment — handled by reconciliation)
            invoices = new_sale_order.with_user(bot_user)._create_invoices()
            if not invoices:
                _logger.error(
                    f"Failed to create invoice for exchange SO {new_sale_order.name}"
                )
                return None

            invoice = invoices[0] if len(invoices) > 1 else invoices
            invoice.with_user(bot_user).action_post()

            _logger.info(
                f"Created and posted invoice {invoice.name} for exchange SO "
                f"{new_sale_order.name} (amount={invoice.amount_total})"
            )
            return invoice

        except Exception as e:
            _logger.error(
                f"Error completing exchange sale order {new_sale_order.name}: {str(e)}",
                exc_info=True,
            )
            return None

    def _reconcile_exchange_documents(
        self, credit_note, new_invoice, new_sale_order, original_sale_order
    ):
        """
        Reconcile credit note against new invoice and handle any remaining delta.
        After reconciliation:
        - If new invoice has residual > 0: customer paid extra → register payment
        - If credit note has residual > 0: customer gets refund → register refund
        - If both residual = 0: equal exchange, done
        """
        try:
            bot_user = self._get_square_bot_user()

            # Get receivable account
            receivable_account = credit_note.partner_id.property_account_receivable_id

            # Get unreconciled receivable lines from both documents
            credit_receivable_lines = credit_note.line_ids.filtered(
                lambda line: line.account_id == receivable_account and not line.reconciled
            )
            invoice_receivable_lines = new_invoice.line_ids.filtered(
                lambda line: line.account_id == receivable_account and not line.reconciled
            )

            if credit_receivable_lines and invoice_receivable_lines:
                (credit_receivable_lines | invoice_receivable_lines).reconcile()
                _logger.info(
                    f"Reconciled credit note {credit_note.name} against invoice {new_invoice.name}"
                )
            else:
                _logger.warning(
                    f"Could not find receivable lines to reconcile: "
                    f"credit={len(credit_receivable_lines)}, invoice={len(invoice_receivable_lines)}"
                )

            # Refresh residuals
            credit_note.invalidate_recordset(["amount_residual", "payment_state"])
            new_invoice.invalidate_recordset(["amount_residual", "payment_state"])

            # Handle remaining residual
            if new_invoice.amount_residual > 0.01:
                # Customer paid extra → register payment for the remaining amount
                _logger.info(
                    f"Invoice {new_invoice.name} has residual {new_invoice.amount_residual}, "
                    f"registering payment"
                )
                payment_journal = self._get_square_payment_journal()
                payment_method_line = self._get_payment_method_line(payment_journal)
                if payment_journal and payment_method_line:
                    payment_register = (
                        self.env["account.payment.register"]
                        .with_context(
                            active_model="account.move",
                            active_ids=new_invoice.ids,
                        )
                        .with_user(bot_user)
                        .create(
                            {
                                "journal_id": payment_journal.id,
                                "payment_method_line_id": payment_method_line.id,
                                "amount": new_invoice.amount_residual,
                                "payment_date": new_invoice.invoice_date
                                or fields.Date.today(),
                            }
                        )
                    )
                    payment_register.action_create_payments()
                    _logger.info(
                        f"Registered payment of {new_invoice.amount_residual} "
                        f"for invoice {new_invoice.name}"
                    )

            elif credit_note.amount_residual > 0.01:
                # Customer gets refund → register refund payment
                _logger.info(
                    f"Credit note {credit_note.name} has residual {credit_note.amount_residual}, "
                    f"registering refund"
                )
                self._create_payment_for_credit_note(
                    credit_note, original_sale_order, bot_user
                )
            else:
                _logger.info("Equal exchange: credit note and invoice fully reconciled")

        except Exception as e:
            _logger.error(
                f"Error reconciling exchange documents: {str(e)}", exc_info=True
            )

    def _create_and_validate_invoice(self, sale_order):
        """Create and validate invoice for the sale order"""
        try:
            # Check if sale order has lines before creating invoice
            if not sale_order.order_line:
                _logger.error(
                    f"Cannot create invoice for sale order {sale_order.name}: No order lines found"
                )
                return None

            bot_user = self._get_square_bot_user()

            # Create invoice using Odoo's standard method
            invoices = sale_order.with_user(bot_user)._create_invoices()

            if not invoices:
                _logger.error(
                    f"Failed to create invoice for sale order {sale_order.name}"
                )
                return None

            # Get the first invoice if it's a recordset
            invoice = invoices[0] if len(invoices) > 1 else invoices

            _logger.info(
                f"Created invoice {invoice.name} with {len(invoice.invoice_line_ids)} lines"
            )

            # Validate and post the invoice using standard action_post()
            invoice.with_user(bot_user).action_post()

            # Ensure invoice is in EUR currency (Square uses EUR)
            if invoice.currency_id.name != "EUR":
                _logger.warning(
                    f"Invoice {invoice.name} is not in EUR currency. "
                    f"Current currency: {invoice.currency_id.name}"
                )

            # Create payment for the invoice (Square orders are already paid)
            payment_created = self._create_payment_for_invoice(
                invoice, sale_order, bot_user
            )

            # Determine payment status for messaging
            payment_status = (
                "Payée" if payment_created else "Créée (paiement en attente)"
            )

            # Add chatter message for invoice creation
            try:
                sale_order.with_user(bot_user).with_context(
                    mail_auto_subscribe_no_notify=True
                ).message_post(
                    body=f"Facture Square créée - {invoice.name}, Montant: {invoice.amount_total} {invoice.currency_id.name}, Status: {payment_status}",
                    subject="Intégration Square : Facture Créée et Payée",
                    message_type="comment",
                )
            except Exception as e:
                _logger.warning(f"Could not post invoice chatter message: {str(e)}")

            # Log to integration dashboard
            payment_info = (
                "Paiement créé automatiquement"
                if payment_created
                else "Erreur lors de la création du paiement"
            )
            self.env["square.integration.log"].log_square_event(
                event_type="order_updated",
                title=f"Facture créée pour la commande {sale_order.name}",
                description=f"""
                    <p><strong>Facture Square Créée avec Succès</strong></p>
                    <ul>
                        <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                        <li>Facture: <strong>{invoice.name}</strong></li>
                        <li>Montant: {invoice.amount_total} {invoice.currency_id.name}</li>
                        <li>Statut: Comptabilisée et validée</li>
                        <li>Paiement: {payment_info}</li>
                        <li>Journal: {invoice.journal_id.name}</li>
                    </ul>
                """,
                status="success",
                square_order_id=sale_order.square_order_id,
                sale_order_id=sale_order.id,
            )

            _logger.info(
                f"Created and posted invoice {invoice.name} for sale order {sale_order.name}. Payment created: {payment_created}"
            )
            return invoice

        except Exception as e:
            _logger.error(
                f"Error creating invoice for sale order {sale_order.name}: {str(e)}",
                exc_info=True,
            )

            # Log error to integration dashboard
            try:
                self.env["square.integration.log"].log_error(
                    title=f"Erreur création facture pour {sale_order.name}",
                    error_message=f"Impossible de créer la facture pour la commande {sale_order.name}",
                    technical_details=f"Commande: {sale_order.name}, Erreur: {str(e)}",
                    square_order_id=sale_order.square_order_id,
                    sale_order_id=sale_order.id,
                )
            except:
                pass  # Don't let logging errors break the process

            return None

    def _create_payment_for_invoice(self, invoice, sale_order, bot_user):
        """Create and register payment for Square invoice (already paid orders)"""
        try:
            # Get payment journal and method line
            payment_journal = self._get_square_payment_journal()
            payment_method_line = self._get_payment_method_line(payment_journal)

            if not payment_journal or not payment_method_line:
                _logger.error("Missing payment journal or method line for Square payment")
                return False

            _logger.info(f"Creating payment for invoice {invoice.name}")

            # Create payment register with proper context
            payment_register = (
                self.env["account.payment.register"]
                .with_context(active_model="account.move", active_ids=invoice.ids)
                .with_user(bot_user)
                .create(
                    {
                        "journal_id": payment_journal.id,
                        "payment_method_line_id": payment_method_line.id,
                        "amount": invoice.amount_total,
                        "payment_date": invoice.invoice_date or fields.Date.today(),
                    }
                )
            )

            # Create and reconcile the payment
            payment_register.action_create_payments()

            _logger.info(f"Successfully created payment for invoice {invoice.name}")
            return True

        except Exception as e:
            _logger.error(f"Failed to create payment for invoice {invoice.name}: {str(e)}")
            return False

    def _create_payment_for_credit_note(self, credit_note, sale_order, bot_user, refund_amount=None):
        """Create and register payment for Square credit note (refund processed)"""
        try:
            # Get payment journal and method line
            payment_journal = self._get_square_payment_journal()
            payment_method_line = self._get_payment_method_line_for_credit_note(payment_journal)

            if not payment_journal or not payment_method_line:
                _logger.error("Missing payment journal or method line for Square credit note payment")
                return False

            # Determine payment amount
            amount = refund_amount if refund_amount is not None else credit_note.amount_residual
            if amount <= 0:
                return True

            _logger.info(f"Creating refund payment for credit note {credit_note.name}")

            # Create payment register with proper context
            payment_register = (
                self.env["account.payment.register"]
                .with_context(active_model="account.move", active_ids=credit_note.ids)
                .with_user(bot_user)
                .create(
                    {
                        "journal_id": payment_journal.id,
                        "payment_method_line_id": payment_method_line.id,
                        "amount": amount,
                        "payment_date": credit_note.invoice_date or fields.Date.today(),
                        "payment_type": "outbound",
                    }
                )
            )

            # Create and reconcile the payment
            payment_register.action_create_payments()

            _logger.info(f"Successfully created refund payment for credit note {credit_note.name}")
            return True

        except Exception as e:
            _logger.error(f"Failed to create refund payment for credit note {credit_note.name}: {str(e)}")
            return False

    def _manual_reconcile(self, move, payments):
        """Manually reconcile if auto-recon fails. Assumes same currency and full match."""
        try:
            receivable_account = move.partner_id.property_account_receivable_id
            for payment in payments:
                # Find receivable lines (debit/credit depend on direction)
                payment_receivable_line = payment.move_id.line_ids.filtered(
                    lambda l: l.account_id == receivable_account and not l.reconciled
                )
                move_receivable_line = move.line_ids.filtered(
                    lambda l: l.account_id == receivable_account and not l.reconciled
                )
                if payment_receivable_line and move_receivable_line:
                    # Determine debit/credit based on signs (for invoice vs. refund)
                    debit_line = (
                        payment_receivable_line
                        if payment_receivable_line.debit
                        else move_receivable_line
                    )
                    credit_line = (
                        payment_receivable_line
                        if payment_receivable_line.credit
                        else move_receivable_line
                    )
                    # Use native reconcile on matched receivable lines
                    (debit_line | credit_line).reconcile()
                    _logger.info(f"Manually reconciled receivable lines for move {move.name}")
                    # Invalidate cache again
                    move.invalidate_recordset(["payment_state"])
        except Exception as e:
            _logger.error(f"Manual reconciliation failed for {move.name}: {str(e)}")

    def _get_payment_method_line_for_credit_note(self, journal):
        """Get the appropriate outbound payment method line for Square credit notes"""
        try:
            # Look for manual outbound payment method line for this journal
            payment_method_line = self.env["account.payment.method.line"].search(
                [
                    ("journal_id", "=", journal.id),
                    ("payment_type", "=", "outbound"),
                    ("code", "=", "manual"),
                ],
                limit=1,
            )

            if payment_method_line:
                return payment_method_line

            # Fallback: look for any outbound payment method line for this journal
            payment_method_line = self.env["account.payment.method.line"].search(
                [("journal_id", "=", journal.id), ("payment_type", "=", "outbound")],
                limit=1,
            )

            if payment_method_line:
                return payment_method_line

            # Last fallback: create an outbound payment method line if none exists
            _logger.warning(
                f"No outbound payment method line found for journal {journal.name}, creating manual method"
            )
            manual_method = self.env["account.payment.method"].search(
                [("payment_type", "=", "outbound"), ("code", "=", "manual")], limit=1
            )

            if manual_method:
                # Create the payment method line
                payment_method_line_vals = {
                    "name": "Manual Payment (Square Outbound)",
                    "journal_id": journal.id,
                    "payment_method_id": manual_method.id,
                    "payment_type": "outbound",
                }
                payment_method_line = self.env["account.payment.method.line"].create(
                    payment_method_line_vals
                )
                _logger.info(
                    f"Created outbound payment method line for journal {journal.name}"
                )
                return payment_method_line

            _logger.error(
                f"Could not find or create outbound payment method line for journal {journal.name}"
            )
            return None

        except Exception as e:
            _logger.error(f"Error finding outbound payment method line: {str(e)}")
            return None

    def _get_square_payment_journal(self):
        """Get the configured payment journal for Square payments"""
        try:
            # Get the Square configuration
            square_config = self.env["square.config"].search([], limit=1)
            if square_config:
                payment_journal = square_config.get_payment_journal()
                if payment_journal:
                    return payment_journal
                else:
                    _logger.warning("No payment journal configured in Square settings")
            else:
                _logger.warning("No Square configuration found")

            # Fallback: try to find a journal with 'Square' in the name
            square_journal = self.env["account.journal"].search(
                [("type", "=", "bank"), ("name", "ilike", "square")], limit=1
            )

            if square_journal:
                _logger.info(
                    f"Using fallback Square-specific journal: {square_journal.name}"
                )
                return square_journal

            # Last fallback: first available bank journal
            bank_journal = self.env["account.journal"].search(
                [("type", "=", "bank")], limit=1
            )

            if bank_journal:
                _logger.warning(f"Using fallback bank journal: {bank_journal.name}")
                return bank_journal

            return None

        except Exception as e:
            _logger.error(f"Error finding payment journal: {str(e)}")
            return None

    def _get_payment_method_line(self, journal):
        """Get the appropriate payment method line for Square payments"""
        try:
            # Look for manual inbound payment method line for this journal
            payment_method_line = self.env["account.payment.method.line"].search(
                [
                    ("journal_id", "=", journal.id),
                    ("payment_type", "=", "inbound"),
                    ("code", "=", "manual"),
                ],
                limit=1,
            )

            if payment_method_line:
                return payment_method_line

            # Fallback: look for any inbound payment method line for this journal
            payment_method_line = self.env["account.payment.method.line"].search(
                [("journal_id", "=", journal.id), ("payment_type", "=", "inbound")],
                limit=1,
            )

            if payment_method_line:
                return payment_method_line

            # Last fallback: create a manual payment method line if none exists
            _logger.warning(
                f"No inbound payment method line found for journal {journal.name}, creating manual method"
            )
            manual_method = self.env["account.payment.method"].search(
                [("payment_type", "=", "inbound"), ("code", "=", "manual")], limit=1
            )

            if manual_method:
                # Create the payment method line
                payment_method_line_vals = {
                    "name": "Manual Payment (Square)",
                    "journal_id": journal.id,
                    "payment_method_id": manual_method.id,
                    "payment_type": "inbound",
                }
                payment_method_line = self.env["account.payment.method.line"].create(
                    payment_method_line_vals
                )
                _logger.info(f"Created payment method line for journal {journal.name}")
                return payment_method_line

            _logger.error(
                f"Could not find or create payment method line for journal {journal.name}"
            )
            return None

        except Exception as e:
            _logger.error(f"Error finding payment method line: {str(e)}")
            return None


    def _create_stock_movements(self, sale_order, square_location_id=None):
        """Create stock movements for the configured warehouse"""
        try:
            # Find the configured warehouse based on location
            if square_location_id:
                configured_warehouse = self._get_warehouse_for_location(
                    square_location_id
                )
            else:
                configured_warehouse = self._get_configured_warehouse()

            if not configured_warehouse:
                _logger.warning(
                    "Configured warehouse not found, skipping stock movements"
                )
                return

            # Process delivery orders using standard Odoo methods
            bot_user = self._get_square_bot_user()

            for picking in sale_order.picking_ids:
                if picking.state not in ["done", "cancel"]:
                    # Set the correct warehouse
                    picking.location_id = configured_warehouse.lot_stock_id.id

                    # Use Odoo's standard stock processing methods with bot user context
                    if picking.state == "draft":
                        picking.with_user(bot_user).action_confirm()
                    if picking.state in ["confirmed", "waiting"]:
                        picking.with_user(bot_user).action_assign()
                    
                    # For Square orders, force immediate validation even if stock is insufficient
                    # The picking might still be in "confirmed" or "waiting" if stock is unavailable
                    if picking.state in ["assigned", "confirmed", "waiting"]:
                        _logger.info(
                            f"Validating picking {picking.name} in state {picking.state} "
                            f"with force_validate context"
                        )
                        # Use bot user context and avoid mail subscription issues
                        # force_validate=True allows validation even with negative stock
                        picking.with_user(bot_user).with_context(
                            force_validate=True,
                            mail_auto_subscribe_no_notify=True,
                            mail_create_nosubscribe=True,
                        ).button_validate()

            # Add chatter message for stock movements
            processed_pickings = sale_order.picking_ids.filtered(
                lambda p: p.state == "done"
            )
            if processed_pickings:
                picking_names = ", ".join(processed_pickings.mapped("name"))
                total_qty = sum(
                    move.product_uom_qty
                    for move in processed_pickings.mapped("move_ids")
                )

                try:
                    bot_user = self._get_square_bot_user()
                    sale_order.with_user(bot_user).with_context(
                        mail_auto_subscribe_no_notify=True
                    ).message_post(
                        body=f"Stock Square traité - {picking_names}, Quantité: {total_qty} articles",
                        subject="Intégration Square : Mouvements de Stock Traités",
                        message_type="comment",
                    )
                except Exception as e:
                    _logger.warning(
                        f"Could not post stock movement chatter message: {str(e)}"
                    )

                # Log to integration dashboard
                self.env["square.integration.log"].log_square_event(
                    event_type="stock_sync",
                    title=f"Mouvements de stock traités pour la commande {sale_order.name}",
                    description=f"""
                        <p><strong>Mouvements de Stock Square Traités avec Succès</strong></p>
                        <ul>
                            <li>Commande Odoo: <strong>{sale_order.name}</strong></li>
                            <li>Ordres de Livraison: <strong>{picking_names}</strong></li>
                            <li>Quantité Totale: {total_qty} articles</li>
                            <li>Entrepôt: {configured_warehouse.name}</li>
                            <li>Statut: Terminés et validés</li>
                        </ul>
                    """,
                    status="success",
                    square_order_id=sale_order.square_order_id,
                    sale_order_id=sale_order.id,
                )

            _logger.info(f"Processed stock movements for sale order {sale_order.name}")

        except Exception as e:
            error_msg = str(e)
            _logger.error(
                f"Error creating stock movements for sale order {sale_order.name}: {error_msg}",
                exc_info=True,
            )

            # If it's a mail follower issue, provide a more specific error
            if (
                "operator does not exist: integer = boolean" in error_msg
                or "mail_followers" in error_msg
            ):
                _logger.warning(
                    f"Mail follower issue detected for sale order {sale_order.name}. "
                    f"This may be due to user/partner configuration issues. "
                    f"The order has been processed but stock movements may need manual validation."
                )

            # Don't re-raise the exception to avoid aborting the entire transaction
            # The order processing should continue even if stock movements fail

    def _get_configured_warehouse(self):
        """Get the configured warehouse from Square configuration (legacy method)"""
        square_config = self.env["square.config"].search([], limit=1)
        if square_config:
            _logger.info(f"Square configuration found: {square_config.name}")
            return square_config.get_configured_warehouse()
        else:
            # Fallback to first warehouse if no config found
            warehouse = self.env["stock.warehouse"].search([], limit=1)
            if warehouse:
                _logger.warning(
                    f"No Square configuration found, using default warehouse: {warehouse.name}"
                )
            else:
                _logger.warning("No warehouse found")
            return warehouse

    def _get_warehouse_for_location(self, square_location_id):
        """Get the warehouse mapped to a specific Square location"""
        square_config = self.env["square.config"].search([], limit=1)
        if square_config:
            return square_config.get_warehouse_for_location(square_location_id)
        return self._get_configured_warehouse()

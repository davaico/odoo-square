# -*- coding: utf-8 -*-
from collections import defaultdict
from datetime import timedelta
import logging

from odoo import Command, models, fields, api
from odoo.exceptions import ValidationError, UserError, AccessError
from odoo.service.model import PG_CONCURRENCY_EXCEPTIONS_TO_RETRY
from odoo.tools.float_utils import float_compare, float_round
from psycopg2 import errorcodes, errors

_logger = logging.getLogger(__name__)


class _ConcurrentRefundCreation(errors.SerializationFailure):
    pgcode = errorcodes.SERIALIZATION_FAILURE


class RefundProcessingDeferred(UserError):
    """The refund can complete once another return releases its reservation."""


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
        string="Square Refund ID", index=True, copy=False
    )
    unverified_square_refund_id = fields.Char(
        string="Unverified Legacy Refund ID", readonly=True, copy=False,
        help="Original identifier retained with historical documents after the refund was quarantined.",
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
    integrity_hold = fields.Boolean(readonly=True, copy=False)
    legacy_processed = fields.Boolean(readonly=True, copy=False)
    processed_at = fields.Datetime(string="Processed At")
    processing_errors = fields.Text(string="Processing Errors")

    # Metadata
    square_data = fields.Json(string="Square Data", help="Redacted Square status metadata")
    webhook_event_id = fields.Char(string="Webhook Event ID")

    _sql_constraints = [
        (
            "square_refund_id_unique",
            "UNIQUE(square_refund_id)",
            "Square Refund ID must be unique.",
        ),
        (
            "square_refund_identity",
            "CHECK ((square_refund_id IS NULL) <> (unverified_square_refund_id IS NULL))",
            "A refund must have either a Square refund ID or an unverified legacy refund ID.",
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

    @api.depends("square_refund_id", "unverified_square_refund_id", "status", "refund_amount")
    def _compute_display_name(self):
        for record in self:
            if record.unverified_square_refund_id:
                record.display_name = f"Unverified legacy refund {record.unverified_square_refund_id}"
            elif record.square_refund_id:
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

        self.env.cr.execute(
            "SELECT id FROM square_refund WHERE square_refund_id = %s FOR UPDATE",
            (square_refund_id,),
        )
        result = self.env.cr.fetchone()
        if result:
            return self.browse(result[0])

    def _create_from_square_data(self, refund_data, sale_order):
        """Create or update a refund record from Square webhook data."""
        refund, _details_changed = self._upsert_from_square_data(refund_data, sale_order)
        return refund

    def _upsert_from_square_data(self, refund_data, sale_order):
        if not refund_data:
            raise ValidationError("Refund data is required")

        square_refund_id = refund_data.get("id")
        if not square_refund_id:
            raise ValidationError("Square refund ID is required")

        if not sale_order:
            raise ValidationError("Sale order is required for refund processing")

        amount_money = refund_data.get("amount_money", {})
        refund_amount = float(amount_money.get("amount", 0)) / 100.0
        currency_code = amount_money.get("currency", "EUR")
        if currency_code != "EUR":
            raise ValidationError(
                f"Unsupported currency '{currency_code}'. This system only supports EUR refunds."
            )

        status = (refund_data.get("status") or "PENDING").strip().lower()
        if status == "canceled":
            status = "cancelled"

        currency = self._get_eur_currency()
        if not currency:
            raise ValidationError(
                "EUR currency not found in the system. Please ensure EUR is properly configured."
            )

        vals = {
            "status": status,
            "refund_amount": refund_amount,
            "currency_id": currency.id,
            "square_data": {"status": refund_data.get("status")},
        }
        if "refunded_line_ids" in refund_data:
            vals["refunded_line_ids"] = refund_data["refunded_line_ids"]

        existing_refund = self._check_existing_refund(square_refund_id)
        if existing_refund and (existing_refund.integrity_hold or existing_refund.legacy_processed):
            credit = existing_refund.credit_note_id
            posted_credit = (
                credit.state == "posted" and credit.move_type == "out_refund"
                and credit.reversed_entry_id in sale_order.invoice_ids
                and (credit.ref or "").endswith(f"Square Refund: {square_refund_id}")
            )
            if posted_credit:
                if status == "completed" and (
                    credit.currency_id != currency
                    or currency.compare_amounts(credit.amount_total, refund_amount)
                ):
                    raise RefundProcessingDeferred("A legacy Square refund requires accounting reconciliation.")
                if not existing_refund.integrity_hold and existing_refund.sale_order_id == sale_order:
                    if status == "completed":
                        existing_refund.legacy_processed = False
                    return existing_refund, False
                raise RefundProcessingDeferred("A legacy Square refund requires accounting reconciliation.")
            if any(
                picking.state == "done" and picking.return_id in sale_order.picking_ids
                for picking in existing_refund.return_picking_ids
            ):
                raise RefundProcessingDeferred("A legacy Square refund requires stock reconciliation.")
            # Untrusted legacy data cannot suppress the provider's status or
            # instruct the service to reuse its draft documents or relations.
            existing_refund.write({
                "unverified_square_refund_id": square_refund_id,
                "square_refund_id": False,
                "integrity_hold": True,
            })
            # Release the unique provider ID before inserting the verified refund.
            existing_refund.flush_recordset(["square_refund_id", "unverified_square_refund_id"])
            existing_refund = self.browse()
        if existing_refund:
            if existing_refund.sale_order_id != sale_order:
                raise ValidationError("Square refund is already linked to a different sale order")
            details_changed = False
            if not existing_refund.processed_at:
                previous_items = existing_refund.refunded_line_ids
                updated_items = vals.get("refunded_line_ids", previous_items)
                details_changed = (
                    currency.compare_amounts(refund_amount, existing_refund.refund_amount) != 0
                    or set(updated_items or ()) != set(previous_items or ())
                )
                existing_refund.write(vals)
            return existing_refund, details_changed

        vals.update({
            "square_refund_id": square_refund_id,
            "square_order_id": sale_order.square_order_id,
            "sale_order_id": sale_order.id,
            "webhook_event_id": refund_data.get("webhook_event_id"),
        })
        try:
            return self.with_user(self._get_bot_user()).sudo().create(vals), False
        except errors.UniqueViolation as error:
            if error.diag.constraint_name != f"{self._table}_square_refund_id_unique":
                raise
            # The winning row is outside this transaction's repeatable-read
            # snapshot. Let Odoo retry the webhook in a fresh transaction.
            raise _ConcurrentRefundCreation("Concurrent Square refund creation") from error

    def _process_webhook_refund(self, refund_data, sale_order, event_type, webhook_event_id):
        """Apply a Square refund event atomically and preserve completed refunds on replay."""
        if not sale_order.square_order_id or sale_order.company_id != self.env.company:
            raise AccessError("This sale is outside the Square integration's company.")
        sale_order.with_user(self._get_bot_user()).check_access("read")
        refund_id = refund_data.get("id")
        order_id = refund_data.get("order_id")
        refund_status = (refund_data.get("status") or "PENDING").strip().upper()
        if refund_status == "CANCELED":
            refund_status = "CANCELLED"

        messages = {
            "PENDING": "Actions de remboursement préparées (en attente de confirmation Square)",
            "COMPLETED": "Refund processed successfully",
            "FAILED": "Failed refund processed",
            "CANCELLED": "Cancelled refund processed",
        }
        result_message = messages.get(refund_status)
        if result_message is None:
            return {
                "status": "error",
                "message": f"Statut de remboursement inconnu: {refund_status}",
            }

        try:
            with self.env.cr.savepoint():
                refund_record, details_changed = self.with_user(self._get_bot_user()).sudo()._upsert_from_square_data(
                    refund_data, sale_order
                )

                if refund_record.processed_at:
                    return {
                        "status": "already_processed",
                        "message": f"Refund {refund_id} already fully processed",
                        "refund_id": refund_id,
                    }

                if webhook_event_id:
                    refund_record.webhook_event_id = webhook_event_id
                refund_record.with_context(square_refund_details_changed=details_changed)._action_process_refund()
        except RefundProcessingDeferred as error:
            _logger.warning("Deferring Square refund %s: %s", refund_id, error)
            return {"status": "retry", "message": str(error), "refund_id": refund_id}

        # Log webhook processing
        self.env["square.integration.log"]._log_square_event(
            event_type=f"refund_{event_type}",
            title=f"Remboursement {event_type} traité pour la commande {sale_order.name}",
            description=f"""
                <p><strong>Square Refund {event_type.title()} Traité</strong></p>
                <ul>
                    <li>Odoo Order: <strong>{sale_order.name}</strong></li>
                    <li>ID Square Refund : <code>{refund_id}</code></li>
                    <li>Statut : {refund_status}</li>
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

    def _get_bot_user(self):
        return self.env["square.order.processor"]._get_square_bot_user()

    def _add_adjustment_line(self, original_line, qty, bot_user):
        """Create or update a negative adjustment line for a refunded quantity.
        Ensures idempotency: if the adjustment line for this product already exists on the order,
        we extend it instead of creating a duplicate line.
        """
        if not original_line or qty <= 0:
            return None
        name = f"[RETOUR] {original_line.name}"
        # The return picking already handles the physical stock movement.
        order_lines = self.env["sale.order.line"].with_context(skip_procurement=True)
        existing = order_lines.search(
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
        created = order_lines.with_user(bot_user).create(vals)
        _logger.info(
            f"Created new adjustment line {created.id} for product {original_line.product_id.name}: qty -{qty}"
        )
        return created

    def _action_process_refund(self):
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

                is_partial = self._is_partial_refund()
                self._prepare_return_pickings(is_partial)

                if not self.credit_note_id or self.credit_note_id.state == "cancel":
                    self.credit_note_id = self._create_credit_note()

                # Log the pending refund actions
                self.env["square.integration.log"]._log_square_event(
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

            except RefundProcessingDeferred:
                raise
            except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                raise
            except Exception as e:
                _logger.error(f"Error creating pending refund actions: {str(e)}")
                self.processing_errors = "Square refund processing failed."
                raise UserError(
                    f"Erreur lors de la création des actions de remboursement : {str(e)}"
                )

    def _complete_refund(self):
        """
        Complete the refund process
        """
        if self.processed_at:
            return True

        bot_user = self._get_bot_user()
        with self.env.cr.savepoint():
            try:
                if not self.credit_note_id or self.credit_note_id.state == "cancel":
                    self._create_pending_refund_actions()
                else:
                    self._prepare_return_pickings(self._is_partial_refund())

                credit_note = self.credit_note_id
                if not self.invoice_id:
                    self.invoice_id = credit_note.reversed_entry_id
                if credit_note.state == "draft":
                    self._adjust_credit_note_for_partial_refund(credit_note)
                    credit_note.with_user(bot_user).action_post()

                # Refunds previously posted and paid during preparation can still
                # have a pending return to complete.
                refund_payments = credit_note.reconciled_payment_ids.filtered(
                    lambda payment: payment.payment_type == "outbound"
                    and payment.move_id.state == "posted"
                )
                if not refund_payments:
                    processor = self.env["square.order.processor"]
                    if not processor._create_payment_for_credit_note(
                        credit_note, self.sale_order_id, bot_user, refund_amount=self.refund_amount
                    ):
                        raise UserError("Le paiement du remboursement Square n'a pas été créé")

                return_pickings = self.return_picking_ids.filtered(
                    lambda picking: picking.state != "cancel"
                )
                for picking in return_pickings.with_user(bot_user):
                    if picking.state == "draft":
                        picking.action_confirm()
                    if picking.state in ("confirmed", "waiting"):
                        picking.action_assign()
                    if picking.state in ("assigned", "confirmed", "waiting"):
                        picking.with_context(
                            mail_auto_subscribe_no_notify=True,
                            mail_create_nosubscribe=True,
                        )._validate_square_picking(self.sale_order_id)
                    if picking.state != "done":
                        raise UserError(f"Le retour {picking.name} n'a pas été validé")

                if return_pickings:
                    for move in return_pickings.move_ids:
                        line = move.origin_returned_move_id.sale_line_id
                        if line and move.state == "done":
                            qty = move.product_uom._compute_quantity(
                                move.quantity, line.product_uom
                            )
                            returned_qty = line._update_returned_quantity(qty)
                            self._add_adjustment_line(line, returned_qty, bot_user)
                elif not self._get_completed_deliveries():
                    self._update_order_line_quantities_from_square(
                        self._fetch_order_details_from_square()
                    )
                self.write({"processed_at": fields.Datetime.now(), "processing_errors": False})

                self.sale_order_id.with_user(bot_user).with_context(
                    mail_auto_subscribe_no_notify=True, mail_create_nosubscribe=True
                ).message_post(
                    body=(
                        f"Avoir Square traité - {credit_note.name}, "
                        f"Montant: {credit_note.amount_total} {credit_note.currency_id.name}"
                    ),
                    subject="Intégration Square : Avoir Créé et Traité",
                    message_type="comment",
                )

                # Log completion with specialized refund logging
                refund_data = {
                    "id": self.square_refund_id,
                    "order_id": self.square_order_id,
                    "amount_money": {
                        "amount": round(self.refund_amount * 100),
                        "currency": self.currency_id.name,
                    },
                }

                self.env["square.integration.log"]._log_refund_processed(
                    sale_order=self.sale_order_id, refund_data=refund_data
                )

                # Log completion
                self.env["square.integration.log"]._log_square_event(
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

            except RefundProcessingDeferred:
                raise
            except PG_CONCURRENCY_EXCEPTIONS_TO_RETRY:
                raise
            except Exception as e:
                _logger.error(f"Error completing refund: {str(e)}")
                self.processing_errors = "Square refund processing failed."
                raise UserError(
                    f"Erreur lors de la finalisation du remboursement : {str(e)}"
                )

    def _is_partial_refund(self):
        """Compare against the original invoice total before any refund adjustments."""
        return bool(self.refunded_line_ids) or self.currency_id.compare_amounts(
            self.refund_amount, self.sale_order_id._square_refund_base_amount()
        ) < 0

    def _get_completed_deliveries(self):
        return self.sale_order_id.picking_ids.filtered(
            lambda picking: picking.picking_type_code == "outgoing" and picking.state == "done"
        )

    def _prepare_return_pickings(self, is_partial):
        """Keep matching staged returns and replace obsolete stock reservations."""
        active_returns = self.return_picking_ids.filtered(lambda picking: picking.state != "cancel")
        prepared = active_returns.filtered(lambda picking: picking.state != "done")
        if active_returns and not prepared:
            return
        deliveries = self._get_completed_deliveries()
        if not deliveries:
            return

        bot_user = self._get_bot_user()
        prepared = prepared.with_user(bot_user)
        prepared_by_delivery = prepared.grouped("return_id")
        own_moves = active_returns.move_ids.filtered(lambda move: move.state != "cancel")
        completed_by_origin = own_moves.filtered(
            lambda move: move.state == "done"
        ).grouped("origin_returned_move_id")

        wizard_model = self.env["stock.return.picking"].with_user(bot_user)
        wizards = wizard_model.create([{"picking_id": delivery.id} for delivery in deliveries])
        return_lines = wizards.product_return_moves
        if is_partial:
            return_lines = return_lines.filtered(lambda line: line.move_id.sale_line_id)

        # Plan as if this refund's own returns had not reserved stock. Keep
        # quantities reserved or returned by other refunds out of the plan.
        available = {
            line: line.move_id._get_square_returnable_quantity(line.uom_id, exclude_returns=own_moves)
            for line in return_lines
        }
        if not is_partial:
            for line in return_lines:
                if self.status == "completed":
                    other_pending_returns = line.move_id.returned_move_ids.filtered(
                        lambda move: move.state not in ("done", "cancel")
                    ) - own_moves
                    unreserved = line.move_id._get_square_returnable_quantity(
                        line.uom_id, exclude_returns=own_moves | other_pending_returns
                    )
                    if float_compare(
                        unreserved, available[line], precision_rounding=line.uom_id.rounding
                    ) > 0:
                        raise RefundProcessingDeferred(
                            "Un autre retour réserve les articles de ce remboursement."
                        )
                line.quantity = available[line]
        elif self.refunded_line_ids:
            if not self._configure_partial_return_lines(return_lines, available, prepared):
                return
        else:
            self._configure_proportional_return_lines(return_lines, available)

        for line in return_lines:
            completed_qty = sum(
                move.product_uom._compute_quantity(move.quantity, line.uom_id, round=False)
                for move in completed_by_origin.get(line.move_id, ())
            )
            if completed_qty:
                line.quantity = float_round(
                    max(0, line.quantity - completed_qty), precision_rounding=line.uom_id.rounding
                )

        for wizard in wizards:
            existing = prepared_by_delivery.pop(wizard.picking_id, None)
            planned_lines = wizard.product_return_moves.filtered("quantity")
            if existing:
                if planned_lines and self._prepared_returns_match(existing, planned_lines):
                    continue
                self._cancel_open_returns(existing)
            if planned_lines:
                result = wizard.action_create_returns()
                self.return_picking_ids = [Command.link(result["res_id"])]

        for obsolete in prepared_by_delivery.values():
            self._cancel_open_returns(obsolete)

    def _prepared_returns_match(self, pickings, planned_lines):
        planned = {(line.move_id, line.to_refund): line.quantity for line in planned_lines}
        prepared = defaultdict(float)
        for move in pickings.move_ids.filtered(lambda move: move.state not in ("done", "cancel")):
            source_move = move.origin_returned_move_id
            prepared[source_move, move.to_refund] += move.product_uom._compute_quantity(
                move.product_uom_qty, move.product_id.uom_id, round=False
            )
        return prepared.keys() == planned.keys() and all(
            not float_compare(
                quantity, prepared[move, to_refund], precision_rounding=move.product_id.uom_id.rounding
            )
            for (move, to_refund), quantity in planned.items()
        )

    def _cancel_open_returns(self, pickings):
        for picking in pickings.filtered(lambda picking: picking.state not in ("done", "cancel")):
            completed = picking.move_ids.filtered(lambda move: move.state == "done")
            if completed:
                (picking.move_ids - completed)._action_cancel()
                picking.is_locked = True
            else:
                picking.action_cancel()

    def _configure_partial_return_lines(self, return_lines, available, prepared):
        legacy_lines = return_lines.filtered(lambda line: not line.move_id.sale_line_id.square_line_id)
        square_items = self._get_legacy_refund_items() if legacy_lines else []
        item_ids = {}
        for line in return_lines:
            sale_line = line.move_id.sale_line_id
            square_uid = sale_line.square_line_id
            if not square_uid:
                square_item = self._find_matching_square_line_item(sale_line, square_items)
                square_uid = square_item.get("uid") if square_item else None
            item_ids[line] = square_uid

        if legacy_lines and prepared:
            known_ids = set(self.sale_order_id.order_line.mapped("square_line_id")) | set(item_ids.values())
            if not set(self.refunded_line_ids).issubset(known_ids):
                # An existing explicit return may have been staged using a successful lookup.
                if self.env.context.get("square_refund_details_changed"):
                    raise UserError("Impossible d'identifier les articles du remboursement Square mis à jour")
                return False

        if return_lines == legacy_lines and not square_items:
            self._configure_proportional_return_lines(return_lines, available)
            return True

        for line, square_uid in item_ids.items():
            if square_uid and square_uid in self.refunded_line_ids and available[line]:
                line.write({"quantity": available[line], "to_refund": True})
        return True

    def _get_legacy_refund_items(self):
        order_details = self._fetch_order_details_from_square() or {}
        items = [item for item in order_details.get("line_items", []) if item.get("uid")]
        if items:
            return items

        snapshot = self.sale_order_id._square_order_snapshot()
        if (snapshot.get("id") or snapshot.get("order_id")) != self.square_order_id:
            return []
        items = snapshot.get("line_items")
        if not isinstance(items, list):
            return []
        return [
            item for item in items
            if isinstance(item, dict) and item.get("uid")
        ]

    def _configure_proportional_return_lines(self, return_lines, available):
        refundable_total = 0.0
        for line in return_lines:
            sale_line = line.move_id.sale_line_id
            if sale_line.product_uom_qty > 0:
                qty = line.uom_id._compute_quantity(
                    available[line], sale_line.product_uom, round=False
                )
                refundable_total += sale_line.price_total * qty / sale_line.product_uom_qty
        if refundable_total <= 0:
            return

        ratio = min(self.refund_amount / refundable_total, 1)
        for line in return_lines:
            qty = float_round(available[line] * ratio, precision_rounding=line.uom_id.rounding)
            if qty > 0:
                line.write({"quantity": qty, "to_refund": True})

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
                    or odoo_line.product_id.default_code in (square_item.get("name") or "")
                ):
                    return square_item

        # Fallback: match by product name similarity
        odoo_name = odoo_line.product_id.name.lower()
        for square_item in square_line_items:
            square_name = (square_item.get("name") or "").lower()
            if square_name and (odoo_name in square_name or square_name in odoo_name):
                return square_item

        return None

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

        invoice.ensure_one()
        self.invoice_id = invoice

        # Create credit note for the requested refund
        return self._create_partial_credit_note(invoice)

    def _create_partial_credit_note(self, invoice):
        """
        Create a partial credit note for the specific refund amount
        """
        # Get bot user for operations to ensure proper tracking messages
        bot_user = self._get_bot_user()

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
                actual_returned = single_line._update_returned_quantity(returned_qty)
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
                actual_returned = line._update_returned_quantity(returned_qty)
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
            actual_returned = alloc["line"]._update_returned_quantity(returned_qty)
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
            order_data = square_api._get_order(self.square_order_id)

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
                        actual_returned = matching_line._update_returned_quantity(
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
            actual_returned = line._update_returned_quantity(effective_qty)
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
        """Prorate the reversal lines to the Square refund amount."""
        credit_note = credit_note.with_user(self._get_bot_user())
        currency = credit_note.currency_id
        if not currency.compare_amounts(credit_note.amount_total, self.refund_amount):
            return
        if currency.is_zero(credit_note.amount_total):
            raise UserError("Impossible de calculer le montant de l'avoir Square")

        ratio = self.refund_amount / credit_note.amount_total
        lines = credit_note.invoice_line_ids.filtered(lambda line: line.display_type == "product")
        credit_note.write({
            "invoice_line_ids": [
                Command.update(line.id, {"price_unit": line.price_unit * ratio})
                for line in lines
            ],
        })

        # Use Odoo's invoice rounding for differences from rounding taxes per line.
        credit_note._check_total_amount(self.refund_amount)
        rounding_delta = currency.round(self.refund_amount - credit_note.amount_total)
        if rounding_delta:
            # There is no tax line to hold the difference on an untaxed credit.
            line = lines.filtered(
                lambda line: not line.tax_ids and line.quantity and line.discount != 100
            )[:1]
            if line:
                discounted_qty = line.quantity * (1 - line.discount / 100)
                line.price_unit = (line.price_subtotal + rounding_delta) / discounted_qty

        if currency.compare_amounts(credit_note.amount_total, self.refund_amount):
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
            self._cancel_open_returns(self.return_picking_ids)

            # Cancel credit note if not posted
            if self.credit_note_id and self.credit_note_id.state == "draft":
                self.credit_note_id.button_cancel()

            # Log failure
            self.env["square.integration.log"]._log_square_event(
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
            self._cancel_open_returns(self.return_picking_ids)

            # Cancel credit note if not posted
            if self.credit_note_id and self.credit_note_id.state == "draft":
                self.credit_note_id.button_cancel()

            # Log cancellation
            self.env["square.integration.log"]._log_square_event(
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

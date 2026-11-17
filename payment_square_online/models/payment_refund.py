# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from urllib.parse import quote
from uuid import uuid4

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.fields import Domain

from ..square_api import MINOR_UNIT_DIGITS, SquareAPIError, square_amount_in_minor_units


_logger = logging.getLogger(__name__)

_REFUND_STATES = {
    "PENDING": "pending",
    "COMPLETED": "done",
    "FAILED": "error",
    "REJECTED": "error",
}
_DEFINITIVE_REFUND_REJECTIONS = {
    "PAYMENT_NOT_REFUNDABLE",
    "PAYMENT_NOT_REFUNDABLE_DUE_TO_DISPUTE",
    "REFUND_AMOUNT_INVALID",
    "REFUND_DECLINED",
}


class PaymentTransaction(models.Model):
    _inherit = "payment.transaction"

    square_online_refund_requested_by = fields.Many2one(
        "res.users", string="Refund Requested By", readonly=True, copy=False, ondelete="restrict",
    )
    square_online_refund_origin = fields.Selection(
        [("odoo", "Odoo"), ("square", "Square")], compute="_compute_square_online_refund_origin",
        string="Refund Origin",
    )
    square_online_refund_reason = fields.Char(readonly=True, copy=False)
    square_online_refund_revision = fields.Integer(copy=False, readonly=True)
    square_online_unattributed_refund_minor = fields.Integer(copy=False, readonly=True)

    _square_online_refund_reference_unique = models.UniqueIndex(
        "(square_online_environment, square_online_merchant_id, provider_reference) "
        "WHERE square_online_connection_id IS NOT NULL AND operation = 'refund'",
        "A Square refund can only belong to one Odoo payment transaction.",
    )

    @api.depends("operation", "provider_code", "provider_reference", "square_online_refund_requested_by")
    def _compute_square_online_refund_origin(self):
        for tx in self:
            origin = False
            if tx.provider_code == "square_online" and tx.operation == "refund":
                if tx.square_online_refund_requested_by:
                    origin = "odoo"
                elif tx.provider_reference:
                    origin = "square"
            tx.square_online_refund_origin = origin

    @api.ondelete(at_uninstall=False)
    def _unlink_except_square_online_requested_refund(self):
        if any(self.mapped("square_online_refund_origin")):
            raise AccessError(_("Square refunds must be kept for payment history."))

    def _square_online_check_refund_access(self):
        if not self:
            return
        if not (
            self.env.user.has_group("account.group_account_manager")
            or self.env.user.has_group("base.group_system")
        ) or self.company_id - self.env.companies:
            raise AccessError(_(
                "Only accounting managers and administrators can refund Square payments in their allowed companies."
            ))
        self.check_access("read")

    def action_refund(self, amount_to_refund=None):
        self.filtered(lambda tx: tx.sudo().provider_code == "square_online")._square_online_check_refund_access()
        return super().action_refund(amount_to_refund=amount_to_refund)

    def _square_online_lock_refunds(self):
        self.ensure_one()
        self._square_online_lock_payment()
        # Updating the parent also fences transactions with old REPEATABLE READ snapshots
        # that cannot see a refund child inserted concurrently.
        self.square_online_refund_revision += 1

    def _square_online_refundable_minor(self, exclude=None):
        self.ensure_one()
        if self.operation != "online_direct" or self.state != "done" or not self.provider_reference:
            return 0
        refunds = self.child_transaction_ids.filtered(
            lambda tx: tx != exclude and tx.operation == "refund" and tx.state in ("pending", "done")
        )
        reserved = sum(refunds.mapped("square_online_amount_minor")) + self.square_online_unattributed_refund_minor
        return max(0, self.square_online_amount_minor - reserved)

    def _square_online_refund_amount_minor(self, amount_to_refund, available):
        if available <= 0:
            raise ValidationError(_("There is no Square balance available to refund."))
        if amount_to_refund is None:
            return available
        requested = self._square_online_validate_refund_amount(amount_to_refund)
        if requested > available:
            raise ValidationError(_("This amount exceeds the refundable Square balance. Reopen the refund wizard."))
        return requested

    def _square_online_validate_refund_amount(self, amount_to_refund):
        try:
            requested = square_amount_in_minor_units(amount_to_refund, self.square_online_currency_code)
        except ValueError:
            raise ValidationError(_("Enter an exact amount in the original Square payment currency.")) from None
        if requested <= 0:
            raise ValidationError(_("Enter a positive Square refund amount."))
        return requested

    def _get_specific_create_values(self, provider_code, values):
        specific = super()._get_specific_create_values(provider_code, values)
        if provider_code == "square_online" and values.get("operation") == "refund":
            specific["is_live"] = values.get("square_online_environment") == "production"
        return specific

    def _square_online_create_refund(self, amount_minor, **extra_create_values):
        self.ensure_one()
        digits = MINOR_UNIT_DIGITS[self.square_online_currency_code]
        return self._create_child_transaction(
            amount_minor / 10 ** digits,
            is_refund=True,
            square_online_amount_minor=amount_minor,
            square_online_connection_id=self.square_online_connection_id.id,
            square_online_website_id=self.square_online_website_id.id,
            square_online_journal_id=self.square_online_journal_id.id,
            square_online_order_id=self.square_online_order_id.id,
            square_online_merchant_id=self.square_online_merchant_id,
            square_online_environment=self.square_online_environment,
            square_online_location_id=self.square_online_location_id,
            square_online_currency_code=self.square_online_currency_code,
            **extra_create_values,
        )

    def _refund(self, amount_to_refund=None):
        if self.provider_code != "square_online":
            return super()._refund(amount_to_refund=amount_to_refund)
        self.ensure_one()
        if (
            self.operation != "online_direct"
            or self.state != "done"
            or not self.square_online_dispatch_at
            or not self.square_online_connection_id
            or not self.payment_id
            or self.currency_id.name != self.square_online_currency_code
        ):
            raise ValidationError(_("Refunds require a captured Square payment recorded in Odoo."))
        self._square_online_lock_refunds()
        amount_minor = self._square_online_refund_amount_minor(
            amount_to_refund, self._square_online_refundable_minor(),
        )
        self._square_online_outbound_method()
        key = uuid4().hex
        refund = self._square_online_create_refund(
            amount_minor,
            square_online_operation_key=key,
            square_online_refund_reason=f"Odoo online refund {key}",
            square_online_refund_requested_by=self.env.uid,
        )
        refund._send_refund_request()
        return refund

    def _send_refund_request(self):
        if self.provider_code != "square_online":
            return super()._send_refund_request()
        self.ensure_one()
        self._set_pending(state_message=_("Odoo queued this Square refund."))
        self.env.ref("payment_square_online.cron_square_online_dispatch_refunds").sudo()._trigger()

    @api.model
    def _cron_square_online_dispatch_refunds(self):
        # The wizard commits the pending reservation before this job can find it.
        # Recovery never uses this job or claims an unsubmitted refund.
        domain = Domain([
            ("provider_code", "=", "square_online"),
            ("operation", "=", "refund"),
            ("state", "=", "pending"),
            ("square_online_refund_requested_by", "!=", False),
            ("square_online_dispatch_at", "=", False),
        ])
        cutoff = fields.Datetime.subtract(fields.Datetime.now(), minutes=1)
        domain &= (
            Domain("square_online_recovery_checked_at", "=", False)
            | Domain("square_online_recovery_checked_at", "<=", cutoff)
        )
        cron = self.env["ir.cron"]
        refunds = self.sudo().search(domain, order="id", limit=20)
        if not cron._commit_progress(remaining=len(refunds)):
            return
        for refund in refunds:
            refund = refund.with_prefetch()
            if refund.filtered_domain(domain):
                try:
                    with self.env.cr.savepoint():
                        can_dispatch = refund._square_online_verify_refund_before_dispatch()
                except SquareAPIError as exc:
                    message = _("Odoo could not verify the original Square payment. The refund is still queued.")
                    if exc.status_code in (401, 403):
                        message = _("Square authorization needs attention. Reauthorize the original connection.")
                    refund.write({
                        "square_online_recovery_message": message,
                        "square_online_recovery_checked_at": fields.Datetime.now(),
                    })
                else:
                    if can_dispatch:
                        refund.write({
                            "square_online_dispatch_at": fields.Datetime.now(),
                            "state_message": _("Square is processing this refund."),
                            "square_online_recovery_message": False,
                        })
                        # Commit before calling Square. A retry of the cron after this point
                        # can only recover the outcome by reading the existing operation.
                        cron._commit_progress()
                        refund._square_online_post_refund()
            if not cron._commit_progress(1):
                break

    def _square_online_verify_refund_before_dispatch(self):
        self.ensure_one()
        source = self.source_transaction_id
        connection = self.square_online_connection_id
        original = connection._square_online_get_payment(source.provider_reference)
        if not source._square_online_matches_payment(original, connection) or original["status"] != "COMPLETED":
            raise SquareAPIError()
        reported_minor, refunds = source._square_online_read_refund_balance(original, connection)
        source._square_online_lock_refunds()
        source._square_online_reconcile_refund_balance(reported_minor, refunds)
        if self.state != "pending" or self.square_online_dispatch_at:
            return False
        available = source._square_online_refundable_minor(exclude=self)
        if self.square_online_amount_minor > available:
            self._set_error(_("The refundable Square balance changed before this refund was sent."))
            return False
        return True

    def _square_online_read_refund_balance(self, payment, connection):
        remote_refunded = payment.get("refunded_money")
        if remote_refunded is not None and not (
            isinstance(remote_refunded, dict)
            and remote_refunded.get("currency") == self.square_online_currency_code
            and type(remote_refunded.get("amount")) is int
            and 0 <= remote_refunded["amount"] <= self.square_online_amount_minor
        ):
            raise SquareAPIError()
        refund_ids = payment.get("refund_ids", [])
        if not isinstance(refund_ids, list) or any(not isinstance(refund_id, str) for refund_id in refund_ids):
            raise SquareAPIError()
        refunds = []
        for refund_id in dict.fromkeys(refund_ids):
            refund = connection._square_online_get_refund(refund_id)
            if not self._square_online_refund_data_matches(refund, connection):
                raise SquareAPIError()
            refunds.append(refund)
        return (remote_refunded or {}).get("amount", 0), refunds

    def _square_online_reconcile_refund_balance(self, reported_minor, refunds):
        self.ensure_one()
        for refund in refunds:
            if not self._square_online_apply_locked_refund(refund, discover=True):
                raise SquareAPIError()
        # Square's aggregate includes pending and completed refunds. Reserve only
        # its unexplained remainder, since each verified ID already has a child.
        linked = self.child_transaction_ids.filtered(
            lambda tx: tx.operation == "refund" and tx.provider_reference and tx.state in ("pending", "done")
        )
        unattributed = max(0, reported_minor - sum(linked.mapped("square_online_amount_minor")))
        if self.square_online_unattributed_refund_minor != unattributed:
            self.square_online_unattributed_refund_minor = unattributed

    def _square_online_post_refund(self):
        self.ensure_one()
        source = self.source_transaction_id
        connection = self.square_online_connection_id
        try:
            result = connection._square_online_request("POST", "/v2/refunds", payload={
                "payment_id": source.provider_reference,
                "idempotency_key": self.square_online_operation_key,
                "reason": self.square_online_refund_reason,
                "amount_money": {
                    "amount": self.square_online_amount_minor,
                    "currency": self.square_online_currency_code,
                },
            })
        except SquareAPIError as exc:
            # Balance conflicts can result from an out-of-band refund. Inspect Square
            # without generating a replacement request or releasing ambiguous funds.
            rejected = (
                exc.status_code in (400, 404)
                and exc.codes
                and _DEFINITIVE_REFUND_REJECTIONS.issuperset(exc.codes)
            )
            if rejected:
                source._square_online_lock_refunds()
            self._square_online_refresh_status()
            if rejected and self.state == "pending" and not self.provider_reference:
                message = _("Square rejected this refund.")
                if "REFUND_AMOUNT_INVALID" in exc.codes:
                    message = _("Square rejected this amount. Review the original payment's refundable balance.")
                self._set_error(message)
                self.square_online_recovery_message = False
            elif self.state == "pending" and not self.square_online_recovery_message:
                self.square_online_recovery_message = _("Square has not confirmed the result of this refund.")
            return
        if not source._square_online_apply_refund(result.get("refund"), connection):
            self.square_online_recovery_message = _("Odoo could not verify the Square refund response.")

    def _square_online_refund_data_matches(self, refund, connection):
        self.ensure_one()
        if not (
            isinstance(refund, dict)
            and self.operation == "online_direct"
            and self.provider_code == "square_online"
            and self.state == "done"
            and self.provider_reference
            and self.square_online_dispatch_at
            and self.square_online_order_id
            and connection
            and connection == self.square_online_connection_id
            and connection.company_id == self.company_id
            and connection.environment == self.square_online_environment
            and connection.merchant_id == self.square_online_merchant_id
        ):
            return False
        refund_id = refund.get("id")
        order_id = refund.get("order_id")
        money = refund.get("amount_money")
        status = refund.get("status")
        return (
            isinstance(refund_id, str) and 0 < len(refund_id) <= 255 and refund_id not in (".", "..")
            and (order_id is None or isinstance(order_id, str) and 0 < len(order_id) <= 192)
            and not refund.get("unlinked")
            and refund.get("payment_id") == self.provider_reference
            and refund.get("location_id") == self.square_online_location_id
            and isinstance(status, str) and status in _REFUND_STATES
            and isinstance(money, dict) and type(money.get("amount")) is int
            and 0 < money["amount"] <= self.square_online_amount_minor
            and money.get("currency") == self.square_online_currency_code
        )

    def _square_online_find_refund_child(self, refund):
        self.ensure_one()
        candidates = self.child_transaction_ids.filtered(
            lambda tx: tx.operation == "refund" and (tx.provider_reference or tx.square_online_dispatch_at)
        )
        child = candidates.filtered(lambda tx: tx.provider_reference == refund["id"])
        if not child:
            child = candidates.filtered(
                lambda tx: not tx.provider_reference and tx.square_online_refund_reason
                and tx.square_online_refund_reason == refund.get("reason")
            )
        return child

    def _square_online_apply_refund(self, refund, connection, *, discover=False):
        self.ensure_one()
        if not self._square_online_refund_data_matches(refund, connection):
            return False
        self._square_online_lock_refunds()
        return self._square_online_apply_locked_refund(refund, discover=discover)

    def _square_online_apply_locked_refund(self, refund, *, discover):
        child = self._square_online_find_refund_child(refund)
        amount_minor = refund["amount_money"]["amount"]
        if not child and discover:
            child = self._square_online_create_refund(
                amount_minor, provider_reference=refund["id"],
            )
        if not child or child.square_online_amount_minor != amount_minor:
            return False
        if child.square_online_order_reference and refund.get("order_id") not in (
            None, child.square_online_order_reference,
        ):
            return False
        target = _REFUND_STATES[refund["status"]]
        if child.state not in ("draft", "pending") and target not in (child.state, "pending"):
            return False
        values = {
            "provider_reference": refund["id"],
            "square_online_order_reference": refund.get("order_id") or child.square_online_order_reference,
            "square_online_recovery_message": False,
        }
        child.write({name: value for name, value in values.items() if child[name] != value})
        if child.state not in ("draft", "pending"):
            return True
        child._process("square_online", {"square_online_refund": refund})
        if child.state == "done" and not child.is_post_processed:
            self.env.ref("payment.cron_post_process_payment_tx")._trigger()
        return True

    def _extract_amount_data(self, payment_data):
        if refund := payment_data.get("square_online_refund"):
            digits = MINOR_UNIT_DIGITS[self.square_online_currency_code]
            return {
                "amount": refund["amount_money"]["amount"] / 10 ** digits,
                "currency_code": refund["amount_money"]["currency"],
                "precision_digits": digits,
            }
        return super()._extract_amount_data(payment_data)

    def _apply_updates(self, payment_data):
        if not (refund := payment_data.get("square_online_refund")):
            return super()._apply_updates(payment_data)
        status = refund["status"]
        if status == "PENDING" and self.state == "draft":
            self._set_pending(state_message=_("Square is processing this refund."))
        elif status == "COMPLETED":
            self._set_done()
        elif status in ("FAILED", "REJECTED"):
            self._set_error(_("Square did not complete this refund."))

    @api.model
    def _square_online_handle_webhook(self, connection, event_type, object_data):
        if event_type not in ("refund.created", "refund.updated"):
            return super()._square_online_handle_webhook(connection, event_type, object_data)
        self._square_online_discover_refund(object_data.get("refund"), connection)

    @api.model
    def _square_online_discover_refund(self, refund, connection):
        payment_id = refund.get("payment_id") if isinstance(refund, dict) else None
        if (
            not isinstance(payment_id, str) or not 0 < len(payment_id) <= 192 or payment_id in (".", "..")
            or not connection.location_id or refund.get("location_id") != connection.location_id.square_id
        ):
            return False
        domain = Domain([
            ("provider_code", "=", "square_online"),
            ("operation", "=", "online_direct"),
            ("company_id", "=", connection.company_id.id),
            ("square_online_connection_id", "=", connection.id),
            ("square_online_dispatch_at", "!=", False),
        ])
        source = self.search(domain & Domain("provider_reference", "=", payment_id), limit=1)
        if source.state != "done":
            # The refund can arrive before the capture response. A verified original payment
            # can link it to the exact checkout claim, including while POS shares the location.
            pending = domain & Domain("state", "=", "pending")
            if not source and not self.search_count(pending & Domain("provider_reference", "=", False), limit=1):
                return False
            payment = connection._square_online_get_payment(payment_id)
            if not payment or payment.get("status") != "COMPLETED":
                return False
            reference = payment.get("reference_id")
            if not isinstance(reference, str) or len(reference) != 32:
                return False
            source = source or self.search(
                pending & Domain("square_online_operation_key", "=", reference), limit=1,
            )
            if not source or not source._square_online_apply_payment(payment, connection):
                return False
        return source._square_online_apply_refund(refund, connection, discover=True)

    def _square_online_read_status(self):
        self.ensure_one()
        if self.operation != "refund":
            return super()._square_online_read_status()
        connection = self.square_online_connection_id
        source = self.source_transaction_id
        if self.provider_reference:
            refund = connection._square_online_get_refund(self.provider_reference)
            return source._square_online_apply_refund(refund, connection)
        if not self.square_online_dispatch_at:
            return False

        original = connection._square_online_get_payment(source.provider_reference)
        if not source._square_online_matches_payment(original, connection) or original["status"] != "COMPLETED":
            return False
        source._square_online_recover_linked_refunds(original, connection)
        if self.provider_reference:
            return True

        start = fields.Datetime.subtract(self.square_online_dispatch_at, minutes=5)
        params = {
            "begin_time": start.isoformat(timespec="seconds") + "Z",
            "location_id": self.square_online_location_id,
            "sort_order": "DESC",
        }
        seen_cursors = set()
        while True:
            page = connection._square_online_request("GET", "/v2/refunds", params=params)
            refunds = page.get("refunds", [])
            if not isinstance(refunds, list):
                raise SquareAPIError()
            if any(self._square_online_recover_matching_refund(refund) for refund in refunds):
                return True
            cursor = page.get("cursor")
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                return False
            seen_cursors.add(cursor)
            params = {**params, "cursor": cursor}

    def _square_online_recover_linked_refunds(self, payment, connection):
        reported_minor, refunds = self._square_online_read_refund_balance(payment, connection)
        if reported_minor or refunds or self.square_online_unattributed_refund_minor:
            self._square_online_lock_refunds()
            self._square_online_reconcile_refund_balance(reported_minor, refunds)

    def _square_online_recover_matching_refund(self, refund):
        return (
            isinstance(refund, dict)
            and self.square_online_refund_reason
            and refund.get("reason") == self.square_online_refund_reason
            and self.source_transaction_id._square_online_apply_refund(
                refund, self.square_online_connection_id,
            )
        )

    @api.model
    def _square_online_refreshable_domain(self):
        return super()._square_online_refreshable_domain() | Domain([
            ("provider_code", "=", "square_online"),
            ("operation", "=", "refund"),
            ("provider_reference", "!=", False),
        ])

    def _square_online_outbound_method(self):
        self.ensure_one()
        journal = self.square_online_journal_id
        line = journal.outbound_payment_method_line_ids.filtered(lambda line: line.code == "manual")[:1]
        if (
            not journal.active or journal.type != "bank" or journal.company_id != self.company_id
            or not line or (journal.currency_id and journal.currency_id != self.currency_id)
        ):
            raise UserError(_("Configure an outbound Manual payment method on the original Square bank journal."))
        return line

    def _post_process(self):
        refunds = self.filtered(lambda tx: tx.provider_code == "square_online" and tx.operation == "refund")
        super(PaymentTransaction, self - refunds)._post_process()
        for refund in refunds.filtered(lambda tx: tx.state in ("done", "error")):
            source = refund.source_transaction_id
            source._square_online_lock_payment()
            if refund.is_post_processed:
                continue
            if refund.state == "done" and not source.payment_id:
                source._post_process()
                if not source.payment_id:
                    raise UserError(_("Record the original Square payment before posting its refund."))
            super(PaymentTransaction, refund)._post_process()

    def _create_payment(self, **extra_create_values):
        if self.provider_code == "square_online" and self.operation == "refund":
            method = self._square_online_outbound_method()
            payment = super()._create_payment(
                journal_id=self.square_online_journal_id.id,
                payment_method_line_id=method.id,
                **extra_create_values,
            )
            if payment.move_id.state != "posted":
                raise UserError(_("Configure an outstanding payments account on the original Square bank journal."))
            return payment
        return super()._create_payment(**extra_create_values)


class SquareOnlineConnection(models.Model):
    _inherit = "square.online.connection"

    square_online_transaction_ids = fields.One2many(
        "payment.transaction", "square_online_connection_id", readonly=True, groups="base.group_system",
    )
    square_online_refund_discovery_at = fields.Datetime(readonly=True, copy=False, groups="base.group_system")
    square_online_refund_discovery_checked_at = fields.Datetime(
        readonly=True, copy=False, groups="base.group_system",
    )

    @api.model
    def _square_online_refund_discovery_payment_domain(self):
        return Domain([
            ("provider_code", "=", "square_online"),
            ("operation", "=", "online_direct"),
            ("square_online_dispatch_at", "!=", False),
            ("state", "in", ("pending", "done")),
        ])

    @api.model
    def _cron_square_online_discover_refunds(self):
        cutoff = fields.Datetime.subtract(fields.Datetime.now(), minutes=5)
        domain = Domain([
            ("location_id", "!=", False),
            ("square_online_transaction_ids", "any", self._square_online_refund_discovery_payment_domain()),
        ]) & (
            Domain("square_online_refund_discovery_checked_at", "=", False)
            | Domain("square_online_refund_discovery_checked_at", "<=", cutoff)
        )
        connections = self.sudo().search(
            domain, order="square_online_refund_discovery_checked_at asc nulls first, id", limit=20,
        )
        cron = self.env["ir.cron"]
        if not cron._commit_progress(remaining=self.sudo().search_count(domain)):
            return
        for connection in connections:
            checked_at = fields.Datetime.now()
            try:
                with self.env.cr.savepoint():
                    connection._square_online_discover_recent_refunds(checked_at)
            except SquareAPIError:
                _logger.warning("Odoo could not check for new Square refunds on connection %s.", connection.id)
            else:
                connection.square_online_refund_discovery_at = checked_at
            connection.square_online_refund_discovery_checked_at = checked_at
            if not cron._commit_progress(1):
                break

    def _square_online_discover_recent_refunds(self, checked_at):
        self.ensure_one()
        transactions = self.env["payment.transaction"]
        previous = self.square_online_refund_discovery_at
        if not previous:
            first = transactions.search(
                self._square_online_refund_discovery_payment_domain()
                & Domain("square_online_connection_id", "=", self.id),
                order="square_online_dispatch_at, id", limit=1,
            )
            if not first:
                return
            previous = first.square_online_dispatch_at
        # A small overlap protects against the list's eventual consistency. The
        # cutoff advances only once every page in this window has been retrieved.
        begin = fields.Datetime.subtract(previous, minutes=5)
        params = {
            "begin_time": begin.isoformat(timespec="seconds") + "Z",
            "end_time": checked_at.isoformat(timespec="seconds") + "Z",
            "location_id": self.location_id.square_id,
            "source_type": "CARD",
            "sort_order": "ASC",
        }
        seen_cursors = set()
        while True:
            page = self._square_online_request("GET", "/v2/refunds", params=params)
            refunds = page.get("refunds", [])
            if not isinstance(refunds, list):
                raise SquareAPIError()
            for refund in refunds:
                transactions._square_online_discover_refund(refund, self)
            cursor = page.get("cursor")
            if not cursor:
                return
            if not isinstance(cursor, str) or cursor in seen_cursors:
                raise SquareAPIError()
            seen_cursors.add(cursor)
            params = {**params, "cursor": cursor}

    def _square_online_get_refund(self, refund_id):
        self.ensure_one()
        if not isinstance(refund_id, str) or not 0 < len(refund_id) <= 255 or refund_id in (".", ".."):
            return False
        result = self._square_online_request("GET", "/v2/refunds/" + quote(refund_id, safe=""))
        refund = result.get("refund")
        return refund if isinstance(refund, dict) and refund.get("id") == refund_id else False

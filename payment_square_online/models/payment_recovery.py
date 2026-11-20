# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
from datetime import UTC, datetime

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError
from odoo.fields import Domain

from ..square_api import SquareAPIError


_logger = logging.getLogger(__name__)
_PAYMENT_RECOVERY_PAGES_PER_CHECK = 4
_PAYMENT_RECOVERY_AUTOMATIC_CHECKS = 12


def _payment_created_at(payment):
    if isinstance(payment, dict):
        try:
            created_at = datetime.fromisoformat(payment.get("created_at"))
            if created_at.tzinfo is not None:
                return created_at.astimezone(UTC)
        except (TypeError, ValueError, OverflowError):
            pass
    return None


class PaymentTransaction(models.Model):
    _inherit = "payment.transaction"

    square_online_recovery_message = fields.Char(
        string="Square Status Check", readonly=True, copy=False,
    )
    square_online_recovery_checked_at = fields.Datetime(
        string="Last Square Status Check", readonly=True, copy=False,
    )
    square_online_recovery_checkpoint = fields.Json(readonly=True, copy=False, groups="base.group_system")
    square_online_recovery_attempts = fields.Integer(readonly=True, copy=False)

    @api.model
    def _square_online_handle_webhook(self, connection, event_type, object_data):
        if event_type not in ("payment.created", "payment.updated"):
            return
        payment = object_data.get("payment")
        if not isinstance(payment, dict):
            return
        reference = payment.get("reference_id")
        if not isinstance(reference, str) or len(reference) != 32:
            return
        tx = self.search([
            ("provider_code", "=", "square_online"),
            ("operation", "=", "online_direct"),
            ("square_online_connection_id", "=", connection.id),
            ("square_online_operation_key", "=", reference),
            ("square_online_dispatch_at", "!=", False),
        ], limit=1)
        if not tx:
            return
        if tx._square_online_apply_payment(payment, connection):
            if tx.square_online_recovery_message:
                tx.square_online_recovery_message = False
        else:
            _logger.warning("Square notification did not match payment transaction %s.", tx.id)

    def action_square_online_refresh_status(self):
        self.ensure_one()
        self.check_access("read")
        if not (
            self.company_id in self.env.companies
            and (
                self.env.user.has_group("account.group_account_manager")
                or self.env.user.has_group("base.group_system")
            )
        ):
            raise AccessError(_("Only accounting managers and administrators can refresh Square payments."))
        if not self.filtered_domain(self._square_online_refreshable_domain()):
            raise AccessError(_("Only operations submitted to Square can be refreshed."))
        self.sudo()._square_online_refresh_status()
        return {"type": "ir.actions.client", "tag": "reload"}

    def _square_online_refresh_status(self, *, automatic=False):
        self.ensure_one()
        if self.operation == "online_direct" and not self.provider_reference:
            # Serialize checkout recovery with other readers and signed provider updates.
            self._square_online_lock_payment()
            self.invalidate_recordset([
                "provider_reference", "state", "square_online_recovery_attempts",
                "square_online_recovery_checkpoint",
            ])
        listing = self.operation == "online_direct" and not self.provider_reference
        if automatic and listing:
            if (
                self.state != "pending"
                or self.square_online_recovery_attempts >= _PAYMENT_RECOVERY_AUTOMATIC_CHECKS
            ):
                return
            # Count failed provider calls too, while keeping the claimant pending for review.
            self.square_online_recovery_attempts += 1
        try:
            with self.env.cr.savepoint():
                matched = self._square_online_read_status()
        except SquareAPIError as error:
            if error.status_code in (401, 403):
                message = _("Square authorization needs attention. Reauthorize the original connection.")
            else:
                message = _("Odoo could not retrieve Square status. Check again later.")
        else:
            message = False if matched else _(
                "Odoo could not verify this operation in Square. Check the Square Dashboard and refresh later."
            )
        if (
            message and listing and self.state == "pending"
            and self.square_online_recovery_attempts >= _PAYMENT_RECOVERY_AUTOMATIC_CHECKS
        ):
            message = _("Automatic Square recovery is paused. %(message)s", message=message)
        self.write({
            "square_online_recovery_checked_at": fields.Datetime.now(),
            "square_online_recovery_message": message,
        })

    def _square_online_read_status(self):
        self.ensure_one()
        if self.operation != "online_direct" or not self.square_online_dispatch_at:
            return False
        connection = self.square_online_connection_id
        if not connection:
            return False
        if self.provider_reference:
            payment = connection._square_online_get_payment(self.provider_reference)
            return self._square_online_recover_payment(payment, connection)

        # Square cursors can expire before the next cron check. Resume from the
        # last checked timestamp, including any payments tied at that timestamp.
        start = fields.Datetime.subtract(self.square_online_dispatch_at, minutes=5)
        checkpoint = self.square_online_recovery_checkpoint or {}
        begin_time = checkpoint.get("begin_time") or start.isoformat(timespec="seconds") + "Z"
        end_time = checkpoint.get("end_time") or fields.Datetime.now().isoformat(timespec="seconds") + "Z"
        params = {
            "begin_time": begin_time,
            "end_time": end_time,
            "location_id": self.square_online_location_id,
            "sort_order": "ASC",
        }
        last_created_at = datetime.fromisoformat(begin_time)
        window_end = datetime.fromisoformat(end_time)
        timestamps_ordered = True
        seen_cursors = set()
        for _page in range(_PAYMENT_RECOVERY_PAGES_PER_CHECK):
            page = connection._square_online_request("GET", "/v2/payments", params=params)
            payments = page.get("payments", [])
            if not isinstance(payments, list):
                raise SquareAPIError()
            for payment in payments:
                if (
                    isinstance(payment, dict)
                    and self._square_online_matches_payment(payment, connection)
                ):
                    return self._square_online_recover_payment(payment, connection)
                if timestamps_ordered:
                    created_at = _payment_created_at(payment)
                    if created_at is None or not last_created_at <= created_at <= window_end:
                        timestamps_ordered = False
                    else:
                        last_created_at = created_at
            cursor = page.get("cursor")
            if cursor in (None, ""):
                # ListPayments is eventually consistent. A completed sweep can
                # start over on the next bounded check to find late-visible payments.
                self.square_online_recovery_checkpoint = False
                return False
            if not isinstance(cursor, str) or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
            params = {**params, "cursor": cursor}
        if timestamps_ordered:
            begin_time = last_created_at.isoformat().replace("+00:00", "Z")
        self.square_online_recovery_checkpoint = {"begin_time": begin_time, "end_time": end_time}
        return False

    def _square_online_recover_payment(self, payment, connection):
        if not self._square_online_apply_payment(payment, connection):
            return False
        if payment["status"] == "COMPLETED":
            self._square_online_recover_linked_refunds(payment, connection)
        return True

    @api.model
    def _square_online_refreshable_domain(self):
        return Domain([
            ("provider_code", "=", "square_online"),
            ("square_online_dispatch_at", "!=", False),
        ])

    @api.model
    def _square_online_recovery_domain(self):
        pending = Domain("state", "=", "pending") & (
            Domain("operation", "!=", "online_direct")
            | Domain("provider_reference", "!=", False)
            | Domain("square_online_recovery_attempts", "=", False)
            | Domain("square_online_recovery_attempts", "<", _PAYMENT_RECOVERY_AUTOMATIC_CHECKS)
        )
        return self._square_online_refreshable_domain() & (
            pending
            | Domain([("state", "=", "done"), ("is_post_processed", "=", False)])
            | Domain([("state", "=", "done"), ("square_online_unattributed_refund_minor", ">", 0)])
        )

    @api.model
    def _cron_square_online_recover_status(self):
        cron = self.env["ir.cron"]
        cutoff = fields.Datetime.subtract(fields.Datetime.now(), minutes=5)
        domain = self._square_online_recovery_domain() & (
            Domain("square_online_recovery_checked_at", "=", False)
            | Domain("square_online_recovery_checked_at", "<=", cutoff)
        )
        transactions = self.sudo().search(
            domain, order="square_online_recovery_checked_at asc nulls first, id", limit=20,
        )
        if not cron._commit_progress(remaining=self.sudo().search_count(domain)):
            return
        for tx in transactions:
            tx = tx.with_prefetch()
            if tx.filtered_domain(domain):
                if tx.state == "pending" or tx.square_online_unattributed_refund_minor:
                    tx._square_online_refresh_status(automatic=True)
                if tx.state == "done" and not tx.is_post_processed:
                    # Keep the authoritative status even if Odoo accounting needs attention.
                    try:
                        with self.env.cr.savepoint():
                            tx._post_process()
                    except UserError:
                        tx.square_online_recovery_message = _(
                            "Square confirmed this operation. Review the Odoo accounting configuration."
                        )
                    tx.square_online_recovery_checked_at = fields.Datetime.now()
            if not cron._commit_progress(1):
                break

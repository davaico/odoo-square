# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError
from odoo.fields import Domain

from ..square_api import SquareAPIError


_logger = logging.getLogger(__name__)


class PaymentTransaction(models.Model):
    _inherit = "payment.transaction"

    square_online_recovery_message = fields.Char(
        string="Square Status Check", readonly=True, copy=False,
    )
    square_online_recovery_checked_at = fields.Datetime(
        string="Last Square Status Check", readonly=True, copy=False,
    )

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
        if self.provider_code != "square_online" or not self.square_online_dispatch_at:
            raise AccessError(_("Only dispatched Square operations can be refreshed."))
        self.sudo()._square_online_refresh_status()
        return {"type": "ir.actions.client", "tag": "reload"}

    def _square_online_refresh_status(self):
        self.ensure_one()
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
            return self._square_online_apply_payment(payment, connection)

        # ListPayments is eventually consistent. An absent item cannot release the pending claim.
        start = fields.Datetime.subtract(self.square_online_dispatch_at, minutes=5)
        params = {
            "begin_time": start.isoformat(timespec="seconds") + "Z",
            "location_id": self.square_online_location_id,
            "sort_order": "ASC",
        }
        seen_cursors = set()
        while True:
            page = connection._square_online_request("GET", "/v2/payments", params=params)
            payments = page.get("payments", [])
            if not isinstance(payments, list):
                raise SquareAPIError()
            for payment in payments:
                if (
                    isinstance(payment, dict)
                    and self._square_online_matches_payment(payment, connection)
                ):
                    return self._square_online_apply_payment(payment, connection)
            cursor = page.get("cursor")
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                return False
            seen_cursors.add(cursor)
            params = {**params, "cursor": cursor}

    @api.model
    def _square_online_recovery_domain(self):
        return Domain([
            ("provider_code", "=", "square_online"),
            ("square_online_dispatch_at", "!=", False),
        ]) & (
            Domain("state", "=", "pending")
            | Domain([("state", "=", "done"), ("is_post_processed", "=", False)])
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
                if tx.state == "pending":
                    tx._square_online_refresh_status()
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

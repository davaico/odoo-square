# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import base64
import hashlib
import hmac
import os
import re
import secrets
from datetime import UTC, datetime
from urllib.parse import quote, urlencode, urlsplit

from cryptography.fernet import Fernet, InvalidToken
from psycopg2.errors import SerializationFailure

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.fields import Domain
from odoo.http import request
from odoo.tools import SQL

from ..square_api import (
    CURRENCY_BY_COUNTRY,
    OAUTH_CALLBACK_PATH,
    OAUTH_SCOPES,
    SQUARE_HOSTS,
    SQUARE_ONLINE_WEBHOOK_PATH,
    SquareAPIError,
    square_request,
)


_STATE_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}")
_OAUTH_FIELDS = {
    "oauth_state_hash": False,
    "oauth_user_id": False,
    "oauth_session_hash": False,
    "oauth_expires_at": False,
}
_SYSTEM_FIELDS = {
    **_OAUTH_FIELDS,
    "application_secret_encrypted": False,
    "webhook_signature_key_encrypted": False,
    "access_token": False,
    "refresh_token": False,
    "merchant_id": False,
    "token_expires_at": False,
    "oauth_status_message": False,
    "oauth_authorized_at": False,
    "oauth_refresh_after": False,
    "oauth_refresh_retry_at": False,
    "oauth_reconnect_required": False,
}
_IDENTITY_FIELDS = ("company_id", "environment", "application_id")


def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _credential_cipher():
    try:
        return Fernet(os.environ.get("SQUARE_ONLINE_ENCRYPTION_KEY", "").encode())
    except ValueError:
        raise UserError(_(
            "Set a valid SQUARE_ONLINE_ENCRYPTION_KEY on the Odoo server before connecting Square."
        )) from None


class SquareOnlineConnection(models.Model):
    _name = "square.online.connection"
    _description = "Square Online Connection"
    _check_company_auto = True

    name = fields.Char(required=True)
    company_id = fields.Many2one(
        "res.company", required=True, default=lambda self: self.env.company, index=True,
    )
    environment = fields.Selection(
        [("sandbox", "Sandbox"), ("production", "Production")],
        default="sandbox",
        required=True,
    )
    application_id = fields.Char(required=True)
    application_secret = fields.Char(
        compute="_compute_application_secret",
        inverse="_inverse_application_secret",
        groups="base.group_system",
        help="Enter the application secret from the Square Developer Console.",
    )
    application_secret_encrypted = fields.Char(copy=False, groups="base.group_system")
    access_token = fields.Char(copy=False, groups="base.group_system")
    refresh_token = fields.Char(copy=False, groups="base.group_system")
    token_expires_at = fields.Datetime(copy=False, readonly=True)
    merchant_id = fields.Char(copy=False, readonly=True)
    location_id = fields.Many2one(
        "square.online.location", string="Square Location", check_company=True,
        ondelete="restrict", copy=False,
    )
    location_locked = fields.Boolean(compute="_compute_location_locked")
    available_location_ids = fields.One2many("square.online.location", "connection_id")
    location_country_code = fields.Char(related="location_id.country_code")
    location_currency_code = fields.Char(related="location_id.currency_code")
    oauth_status_message = fields.Char(string="Last authorization result", copy=False, readonly=True)
    oauth_health = fields.Selection(
        [
            ("not_connected", "Not authorized"),
            ("authorized", "Authorized"),
            ("expired", "Access expired — renewal pending"),
            ("reconnect", "Reauthorization required"),
        ],
        string="Authorization Status", compute="_compute_oauth_health",
    )
    oauth_authorized_at = fields.Datetime(copy=False, groups="base.group_system")
    oauth_refresh_after = fields.Datetime(copy=False)
    oauth_refresh_retry_at = fields.Datetime(copy=False)
    oauth_reconnect_required = fields.Boolean(copy=False, readonly=True)
    oauth_state_hash = fields.Char(copy=False, groups="base.group_system", index=True)
    oauth_user_id = fields.Many2one("res.users", copy=False, groups="base.group_system")
    oauth_session_hash = fields.Char(copy=False, groups="base.group_system")
    oauth_expires_at = fields.Datetime(copy=False, groups="base.group_system")
    redirect_uri = fields.Char(string="OAuth Redirect URL", compute="_compute_redirect_uri")

    webhook_notification_url = fields.Char(
        string="Webhook Notification URL", copy=False, groups="base.group_system",
        help="The exact public HTTPS notification URL configured in the Square Developer Console.",
    )
    webhook_signature_key = fields.Char(
        string="Webhook Signature Key", compute="_compute_webhook_signature_key",
        inverse="_inverse_webhook_signature_key", groups="base.group_system",
        help="Enter the signature key for this subscription from the Square Developer Console.",
    )
    webhook_signature_key_encrypted = fields.Char(copy=False, groups="base.group_system")
    webhook_route = fields.Char(string="Webhook Route", compute="_compute_webhook_route")

    @api.model
    def _square_online_encrypt(self, value):
        return _credential_cipher().encrypt(value.encode()).decode()

    @api.model
    def _square_online_decrypt(self, value):
        try:
            return _credential_cipher().decrypt(value.encode()).decode()
        except InvalidToken:
            raise UserError(_("Odoo cannot decrypt the Square credentials. Check the server key.")) from None

    def _compute_application_secret(self):
        # The form accepts a replacement value without ever returning a stored secret.
        self.application_secret = False

    def _inverse_application_secret(self):
        for connection in self.filtered("application_secret"):
            encrypted = self._square_online_encrypt(connection.application_secret)
            super(SquareOnlineConnection, connection).write({
                **_OAUTH_FIELDS, "application_secret_encrypted": encrypted,
            })
            connection.invalidate_recordset(["application_secret"])

    def _compute_webhook_route(self):
        for connection in self:
            connection.webhook_route = (
                f"{SQUARE_ONLINE_WEBHOOK_PATH}/{connection.id}" if connection.id else False
            )

    @api.constrains("webhook_notification_url")
    def _check_webhook_notification_url(self):
        for connection in self.filtered("webhook_notification_url"):
            try:
                url = urlsplit(connection.webhook_notification_url)
                valid = (
                    url.scheme == "https" and url.hostname and not url.username and not url.password
                    and not url.fragment and url.path == connection.webhook_route
                )
            except ValueError:
                valid = False
            if not valid:
                raise ValidationError(_("Enter a public HTTPS webhook URL ending with this connection's route."))

    def _compute_webhook_signature_key(self):
        self.webhook_signature_key = False

    def _inverse_webhook_signature_key(self):
        for connection in self.filtered("webhook_signature_key"):
            encrypted = self._square_online_encrypt(connection.webhook_signature_key)
            super(SquareOnlineConnection, connection).write({"webhook_signature_key_encrypted": encrypted})
            connection.invalidate_recordset(["webhook_signature_key"])

    def _square_online_webhook_authenticated(self, signature, body):
        self.ensure_one()
        if not self.webhook_notification_url or not self.webhook_signature_key_encrypted:
            return False
        try:
            key = self._square_online_decrypt(self.webhook_signature_key_encrypted)
        except UserError:
            return False
        signed = self.webhook_notification_url.encode() + body
        expected = base64.b64encode(hmac.digest(key.encode(), signed, "sha256"))
        return hmac.compare_digest(expected, signature.encode())

    @api.depends("location_id")
    def _compute_location_locked(self):
        for connection in self:
            connection.location_locked = bool(connection._origin.location_id)

    @api.depends("merchant_id", "token_expires_at", "oauth_reconnect_required")
    def _compute_oauth_health(self):
        now = fields.Datetime.now()
        for connection in self:
            if connection.oauth_reconnect_required:
                connection.oauth_health = "reconnect"
            elif not connection.merchant_id:
                connection.oauth_health = "not_connected"
            elif not connection.token_expires_at or connection.token_expires_at <= now:
                connection.oauth_health = "expired"
            else:
                connection.oauth_health = "authorized"

    def _compute_redirect_uri(self):
        base_url = self.env["ir.config_parameter"].sudo().get_param("web.base.url", "")
        self.redirect_uri = base_url.rstrip("/") + OAUTH_CALLBACK_PATH

    @api.model_create_multi
    def create(self, vals_list):
        if any(_SYSTEM_FIELDS.keys() & vals.keys() for vals in vals_list):
            raise AccessError(_("Square manages connection authorization and tokens."))
        return super().create([{**vals, **_SYSTEM_FIELDS} for vals in vals_list])

    def write(self, vals):
        if _SYSTEM_FIELDS.keys() & vals.keys():
            raise AccessError(_("Square manages connection authorization and tokens."))
        self._check_immutable_fields(vals)
        return super().write(vals)

    def _check_immutable_fields(self, vals):
        for connection in self:
            for field_name in _IDENTITY_FIELDS:
                if field_name not in vals:
                    continue
                old_value = connection[field_name]
                if field_name == "company_id":
                    old_value = old_value.id
                if vals[field_name] != old_value:
                    raise ValidationError(_("Create a new Square connection to change its identity."))
            if (
                "location_id" in vals and connection.location_id
                and vals["location_id"] != connection.location_id.id
            ):
                raise ValidationError(_("Create a new Square connection to choose a different location."))

    @api.constrains("application_id", "environment")
    def _check_application_environment(self):
        for connection in self:
            is_sandbox_id = connection.application_id.startswith("sandbox-")
            if is_sandbox_id != (connection.environment == "sandbox"):
                raise ValidationError(_("Use a Square application ID for the selected environment."))

    @api.constrains("location_id")
    def _check_location(self):
        for connection in self.filtered("location_id"):
            location = connection.location_id
            if location.connection_id != connection or not location.available_for_payment:
                raise ValidationError(_("Choose an eligible Square location from this connection."))

    def action_square_online_connect(self):
        self.ensure_one()
        self.check_access("write")
        if not self.env.user.has_group("base.group_system"):
            raise AccessError(_("Only administrators can authorize Square."))
        if not request or not request.session.uid:
            raise UserError(_("Start the Square authorization from the Odoo web interface."))
        if not self.application_secret_encrypted:
            raise UserError(_("Enter the Square application secret before connecting."))
        self._square_online_decrypt(self.application_secret_encrypted)
        parsed = urlsplit(self.redirect_uri)
        local_sandbox = (
            self.environment == "sandbox"
            and parsed.scheme == "http"
            and parsed.hostname == "localhost"
        )
        if (
            not (parsed.scheme == "https" or local_sandbox)
            or not parsed.hostname or parsed.username or parsed.query or parsed.fragment
        ):
            raise UserError(_("Set the Odoo web base URL to the public HTTPS address before connecting Square."))

        state = secrets.token_urlsafe(32)
        super().write({
            "oauth_state_hash": _digest(state),
            "oauth_user_id": self.env.uid,
            "oauth_session_hash": _digest(request.session.sid),
            "oauth_expires_at": fields.Datetime.add(fields.Datetime.now(), minutes=10),
        })
        query = {
            "client_id": self.application_id,
            "scope": " ".join(OAUTH_SCOPES),
            "state": state,
        }
        if self.environment == "production":
            query["session"] = "false"
        return {
            "type": "ir.actions.act_url",
            "url": SQUARE_HOSTS[self.environment] + "/oauth2/authorize?" + urlencode(query),
            "target": "self",
        }

    @api.model
    def _square_online_finish_oauth(self, state, code, error, session_id):
        if not _STATE_PATTERN.fullmatch(state):
            return self.browse()
        state_hash = _digest(state)
        connection = self.search([
            ("oauth_state_hash", "=", state_hash),
            ("oauth_user_id", "=", self.env.uid),
        ], limit=1)
        if not connection:
            return connection

        # Serialize callback consumption so only one request can exchange the code.
        connection._square_online_lock_credentials()
        connection.invalidate_recordset(list(_OAUTH_FIELDS))
        if (
            connection.oauth_state_hash != state_hash
            or connection.oauth_user_id.id != self.env.uid
            or connection.oauth_session_hash != _digest(session_id)
            or not connection.oauth_expires_at
            or connection.oauth_expires_at <= fields.Datetime.now()
        ):
            return self.browse()

        super(SquareOnlineConnection, connection).write(_OAUTH_FIELDS)
        if error == "access_denied":
            message = _("Square authorization was declined.")
        elif error or not code or len(code) > 191:
            message = _("Square did not return a valid authorization code. Try connecting again.")
        else:
            message = connection._square_online_authorize(code)
        super(SquareOnlineConnection, connection).write({"oauth_status_message": message})
        return connection

    def _square_online_authorize(self, code):
        authorized_at = datetime.now(UTC).replace(tzinfo=None)
        try:
            token = square_request(self.environment, "POST", "/oauth2/token", payload={
                "grant_type": "authorization_code",
                "client_id": self.application_id,
                "client_secret": self._square_online_decrypt(self.application_secret_encrypted),
                "code": code,
            })
            token_values = self._square_online_token_values(token)
            merchant_id = token_values["merchant_id"]
            if self.merchant_id and self.merchant_id != merchant_id:
                return _("Square returned a different merchant. Authorize the original merchant or create a new connection.")
            if self.environment == "production":
                self._square_online_check_scopes(merchant_id, token["access_token"])
            locations = self._square_online_fetch_locations(merchant_id, token["access_token"])
        except (SquareAPIError, UserError):
            return _("Odoo could not verify the Square authorization. Check the application credentials and connect again.")

        selected_location = self.location_id
        self._square_online_store_locations(locations)
        super().write({
            **token_values,
            "oauth_authorized_at": authorized_at,
            "oauth_reconnect_required": False,
        })
        if selected_location and not selected_location.available_for_payment:
            return _("Square authorized the merchant. The selected location is unavailable for card payments.")
        if not self.available_location_ids.filtered("available_for_payment"):
            return _("Square authorized the merchant. No eligible card-processing location was found.")
        if not selected_location:
            return _("Square authorized the merchant. Choose a Square location to finish setup.")
        return _("Square authorized the merchant and verified the selected location.")

    def _square_online_token_values(self, token):
        try:
            merchant_id = token["merchant_id"]
            access_token = token["access_token"]
            refresh_token = token["refresh_token"]
            expires_at = datetime.fromisoformat(token["expires_at"])
            token_type = token["token_type"].lower()
        except (KeyError, TypeError, ValueError, AttributeError):
            raise SquareAPIError() from None
        if (
            token_type != "bearer" or expires_at.tzinfo is None
            or expires_at <= datetime.now(UTC)
            or not all(isinstance(value, str) and value for value in (merchant_id, access_token, refresh_token))
        ):
            raise SquareAPIError()
        expires_at = expires_at.astimezone(UTC).replace(tzinfo=None)
        return {
            "merchant_id": merchant_id,
            "access_token": self._square_online_encrypt(access_token),
            "refresh_token": self._square_online_encrypt(refresh_token),
            "token_expires_at": expires_at,
            "oauth_refresh_after": min(
                fields.Datetime.add(fields.Datetime.now(), days=7),
                fields.Datetime.subtract(expires_at, hours=1),
            ),
            "oauth_refresh_retry_at": False,
        }

    def _square_online_check_scopes(self, merchant_id, access_token):
        response = square_request(
            self.environment, "POST", "/oauth2/token/status", token=access_token,
        )
        scopes = response.get("scopes")
        if (
            response.get("merchant_id") != merchant_id
            or response.get("client_id") != self.application_id
            or not isinstance(scopes, list)
            or not all(scope in scopes for scope in OAUTH_SCOPES)
        ):
            raise SquareAPIError()

    def _square_online_fetch_locations(self, merchant_id, access_token):
        response = square_request(
            self.environment, "GET", "/v2/locations", token=access_token,
        )
        locations = response.get("locations", [])
        if not isinstance(locations, list) or any(
            not isinstance(location, dict) or location.get("merchant_id") != merchant_id
            for location in locations
        ):
            raise SquareAPIError()
        return locations

    def _square_online_store_locations(self, locations):
        location_model = self.env["square.online.location"].sudo()
        stored_locations = location_model.search([("connection_id", "=", self.id)])
        by_square_id = {location.square_id: location for location in stored_locations}
        available_locations = location_model
        for data in locations:
            square_id = data.get("id")
            country = data.get("country")
            currency = data.get("currency")
            if not all(isinstance(value, str) and value for value in (square_id, country, currency)):
                continue
            name = data.get("name")
            capabilities = data.get("capabilities")
            available = (
                data.get("status") == "ACTIVE"
                and isinstance(capabilities, list)
                and "CREDIT_CARD_PROCESSING" in capabilities
                and CURRENCY_BY_COUNTRY.get(country) == currency
            )
            location = by_square_id.get(square_id)
            if location:
                # Once selected, retain the original country and currency of the connection.
                if location == self.location_id and (
                    location.country_code != country or location.currency_code != currency
                ):
                    continue
                location.write({
                    "name": name if isinstance(name, str) and name else square_id,
                    "country_code": country,
                    "currency_code": currency,
                    "available_for_payment": available,
                })
            else:
                location = location_model.create({
                    "name": name if isinstance(name, str) and name else square_id,
                    "connection_id": self.id,
                    "square_id": square_id,
                    "country_code": country,
                    "currency_code": currency,
                    "available_for_payment": available,
                })
                by_square_id[square_id] = location
            available_locations |= location

        (stored_locations - available_locations).write({"available_for_payment": False})

    def _square_online_current_access(self, now):
        if self.access_token and self.token_expires_at and self.token_expires_at > now:
            try:
                return self._square_online_decrypt(self.access_token)
            except UserError:
                return False
        return False

    def _square_online_can_renew(self, now):
        return (
            self.merchant_id and not self.oauth_reconnect_required
            and (not self.oauth_refresh_retry_at or self.oauth_refresh_retry_at <= now)
        )

    def _square_online_needs_renewal(self, now):
        return self._square_online_can_renew(now) and (
            not self.oauth_refresh_after or self.oauth_refresh_after <= now
            or not self.token_expires_at or self.token_expires_at <= now
        )

    def _square_online_is_ready(self):
        self.ensure_one()
        if not (self.merchant_id and self.location_id.available_for_payment):
            return False
        now = fields.Datetime.now()
        if self._square_online_current_access(now):
            return True
        if not (
            self._square_online_can_renew(now) and self.refresh_token and self.application_secret_encrypted
        ):
            return False
        try:
            self._square_online_decrypt(self.refresh_token)
            self._square_online_decrypt(self.application_secret_encrypted)
        except UserError:
            return False
        return True

    def _square_online_request(self, method, path, *, payload=None, params=None):
        self.ensure_one()
        if not path.startswith("/v2/"):
            raise ValueError("Expected a Square v2 API path.")
        for attempt in range(2):
            encrypted, token = self._square_online_sync_access()
            try:
                return square_request(
                    self.environment, method, path, token=token, payload=payload, params=params,
                )
            except SquareAPIError as error:
                expired = "ACCESS_TOKEN_EXPIRED" in error.codes
                if expired or error.status_code == 401 or (
                    {"ACCESS_TOKEN_REVOKED", "INSUFFICIENT_SCOPES"} & set(error.codes)
                ):
                    self._square_online_sync_access(rejected_access=encrypted, expired=expired)
                # Only a read can be repeated after Square rejects an access token.
                if not (expired and method == "GET" and attempt == 0):
                    raise

    def _square_online_lock_credentials(self):
        # A payment's foreign key can still reference this connection while it is locked.
        self.env.cr.execute(SQL(
            "SELECT id FROM %s WHERE id = %s FOR NO KEY UPDATE",
            SQL.identifier(self._table), self.id,
        ))

    def _square_online_authorization_revoked(self, revoked_at):
        self.ensure_one()
        try:
            revoked_at = datetime.fromisoformat(revoked_at)
        except (TypeError, ValueError):
            return
        if revoked_at.tzinfo is None:
            return
        self._square_online_lock_credentials()
        revoked_at = revoked_at.astimezone(UTC).replace(tzinfo=None)
        if self.oauth_authorized_at and revoked_at < self.oauth_authorized_at:
            return  # A retry of an old notification must leave newer authorization usable.
        if self.access_token or not self.oauth_reconnect_required:
            self._square_online_require_reconnect(clear_access=True)

    def _square_online_require_reconnect(self, *, clear_access=False):
        values = {"oauth_reconnect_required": True, "oauth_refresh_retry_at": False}
        if clear_access:
            values.update({"access_token": False, "token_expires_at": False})
        super().write(values)

    def _square_online_sync_access(self, *, rejected_access=None, expired=False):
        # Keep credential changes independent of financial requests and their savepoints.
        for _attempt in range(3):
            with self.env.registry.cursor() as cr:
                connection = api.Environment(cr, SUPERUSER_ID, {})[self._name].browse(self.id).exists()
                if not connection:
                    raise SquareAPIError(401)
                now = fields.Datetime.now()
                if rejected_access or connection._square_online_needs_renewal(now):
                    try:
                        connection._square_online_lock_credentials()
                    except SerializationFailure:
                        # A concurrent update won. Read its result in a fresh snapshot.
                        cr.rollback()
                        continue
                    if rejected_access:
                        # The response belongs to the token sent, which may have since changed.
                        if connection.access_token == rejected_access:
                            if expired:
                                super(SquareOnlineConnection, connection).write({
                                    "token_expires_at": now,
                                    "oauth_refresh_after": False,
                                    "oauth_refresh_retry_at": False,
                                })
                            else:
                                connection._square_online_require_reconnect(clear_access=True)
                        return
                    connection._square_online_renew_access()
                token = connection._square_online_current_access(fields.Datetime.now())
                if token:
                    return connection.access_token, token
                status_code = 503 if connection.oauth_refresh_retry_at else 401
            raise SquareAPIError(status_code)
        raise SquareAPIError(503)

    def _square_online_renew_access(self):
        if not (self.refresh_token and self.application_secret_encrypted):
            self._square_online_require_reconnect()
            return
        try:
            token = square_request(self.environment, "POST", "/oauth2/token", payload={
                "grant_type": "refresh_token",
                "client_id": self.application_id,
                "client_secret": self._square_online_decrypt(self.application_secret_encrypted),
                "refresh_token": self._square_online_decrypt(self.refresh_token),
            })
            token_values = self._square_online_token_values(token)
        except UserError:
            self._square_online_require_reconnect()
            return
        except SquareAPIError as error:
            if error.status_code in (400, 401, 403):
                self._square_online_require_reconnect(clear_access="ACCESS_TOKEN_REVOKED" in error.codes)
            else:
                super().write({
                    "oauth_refresh_retry_at": fields.Datetime.add(fields.Datetime.now(), minutes=5),
                })
            return
        if token_values["merchant_id"] != self.merchant_id:
            self._square_online_require_reconnect()
            return
        super().write(token_values)

    @api.model
    def _cron_square_online_renew_access(self):
        now = fields.Datetime.now()
        domain = Domain([
            ("merchant_id", "!=", False),
            ("oauth_reconnect_required", "=", False),
        ]) & (
            Domain("oauth_refresh_retry_at", "=", False) | Domain("oauth_refresh_retry_at", "<=", now)
        ) & (
            Domain("oauth_refresh_after", "=", False) | Domain("oauth_refresh_after", "<=", now)
            | Domain("token_expires_at", "=", False) | Domain("token_expires_at", "<=", now)
        )
        cron = self.env["ir.cron"]
        connections = self.search(domain, limit=20, order="oauth_refresh_after asc nulls first, id")
        if not cron._commit_progress(remaining=self.search_count(domain)):
            return
        for connection in connections:
            try:
                connection._square_online_sync_access()
            except SquareAPIError:
                # The attempt records retry timing or guidance on this connection.
                pass
            if not cron._commit_progress(1):
                break

    def _square_online_get_payment(self, payment_id):
        self.ensure_one()
        if not isinstance(payment_id, str) or not payment_id or payment_id in (".", ".."):
            return False
        result = self._square_online_request("GET", "/v2/payments/" + quote(payment_id, safe=""))
        payment = result.get("payment")
        return payment if isinstance(payment, dict) and payment.get("id") == payment_id else False

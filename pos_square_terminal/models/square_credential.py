import os
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from secrets import compare_digest, token_urlsafe

from cryptography.fernet import Fernet, InvalidToken
from psycopg2.errors import DeadlockDetected, SerializationFailure

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import UserError

from ..const import SQUARE_ENCRYPTION_KEY_ENV
from ..square_api import SquareAPIError, square_request


class SquareCredential(models.Model):
    _name = 'pos.square.credential'
    _description = 'Square Terminal OAuth Credential'

    # No RPC access. Only server-side calls can read the sealed grant.
    connection_id = fields.Many2one('pos.square.connection', required=True, ondelete='cascade')
    application_secret = fields.Char()
    access_token = fields.Char()
    refresh_token = fields.Char()
    expires_at = fields.Datetime()
    refresh_after = fields.Datetime(index=True)
    reauthorization_required = fields.Boolean()
    oauth_state_digest = fields.Char(index='btree_not_null')
    oauth_session_digest = fields.Char()
    oauth_user_id = fields.Many2one('res.users', ondelete='set null')
    oauth_expires_at = fields.Datetime()
    oauth_redirect_uri = fields.Char()

    _sql_constraints = [
        ('unique_connection', 'unique(connection_id)', 'A connection can have one Square OAuth credential.'),
    ]

    @staticmethod
    def _cipher():
        try:
            return Fernet(os.environ.get(SQUARE_ENCRYPTION_KEY_ENV, '').encode())
        except ValueError:
            raise UserError(_("Configure a valid Square Terminal credential encryption key on the Odoo server.")) from None

    def _seal(self, value):
        return self._cipher().encrypt(value.encode()).decode()

    def _unseal(self, value):
        try:
            return self._cipher().decrypt(value.encode()).decode()
        except InvalidToken:
            raise UserError(_("Odoo cannot decrypt the Square Terminal credentials. Restore the original encryption key.")) from None

    def _lock(self):
        self.ensure_one()
        self.env.cr.execute('SELECT id FROM pos_square_credential WHERE id = %s FOR NO KEY UPDATE', [self.id])
        return bool(self.env.cr.fetchone())

    def _authorization_error(self):
        if not self or not self.connection_id.merchant_id or not self.access_token:
            return _("Authorize the Square connection first.")
        if self.reauthorization_required:
            return _("Reconnect the original Square merchant account.")
        if not self.expires_at or (
            self.expires_at <= fields.Datetime.now() and not (self.refresh_token and self.application_secret)
        ):
            return _("Reconnect the original Square merchant account.")
        try:
            self._unseal(self.access_token if self.expires_at > fields.Datetime.now() else self.refresh_token)
        except UserError as error:
            return str(error)
        return False

    def _access_token_for_use(self):
        self.ensure_one()
        error = self._authorization_error()
        if error:
            raise UserError(error)
        if self.expires_at > fields.Datetime.now():
            return self._unseal(self.access_token)
        self._lock()
        if self._refresh_authorization():
            return self._unseal(self.access_token)
        error = self._authorization_error()
        raise UserError(error or _("Odoo could not renew the Square authorization. Try again later."))

    def _refresh_authorization(self):
        connection = self.connection_id
        try:
            tokens = square_request(connection.environment, 'POST', '/oauth2/token', payload={
                'grant_type': 'refresh_token',
                'client_id': connection.application_id,
                'client_secret': self._unseal(self.application_secret),
                'refresh_token': self._unseal(self.refresh_token),
            })
            values = self._token_values(tokens)
        except SquareAPIError as error:
            if error.status == 400 or error.is_authorization_error:
                self._record_authorization_failure()
            return False
        except ValueError:
            self._record_authorization_failure()
            return False
        if tokens['merchant_id'] != connection.merchant_id:
            self._record_authorization_failure()
            return False
        self.write(values)
        return True

    def _record_authorization_failure(self):
        self.reauthorization_required = True
        self.connection_id.authorization_result = 'revoked'

    @api.model
    def _cron_renew_authorizations(self):
        now = fields.Datetime.now()
        domain = [
            ('refresh_token', '!=', False), ('application_secret', '!=', False),
            ('reauthorization_required', '=', False), ('refresh_after', '<=', now),
        ]
        for credential_id in self.search(domain, order='refresh_after, id', limit=10).ids:
            try:
                with self.env.registry.cursor() as cr:
                    credential = self.with_env(api.Environment(cr, SUPERUSER_ID, {})).browse(credential_id)
                    if not credential._lock() or not credential.filtered_domain(domain):
                        continue
                    try:
                        renewed = credential._refresh_authorization()
                    except UserError:
                        renewed = False
                    if not renewed and not credential.reauthorization_required:
                        credential.refresh_after = now + timedelta(hours=1)
            except (SerializationFailure, DeadlockDetected):
                continue

    @staticmethod
    def _digest(value):
        return sha256(value.encode()).hexdigest()

    def _start_authorization(self, sid, redirect_uri, user_id):
        self.ensure_one()
        self._lock()
        if not self.application_secret:
            raise UserError(_("Save your Square application secret before authorizing Square."))
        self._unseal(self.application_secret)
        state = token_urlsafe(32)
        self.write({
            'oauth_state_digest': self._digest(state),
            'oauth_session_digest': self._digest(sid),
            'oauth_user_id': user_id,
            'oauth_expires_at': fields.Datetime.now() + timedelta(minutes=15),
            'oauth_redirect_uri': redirect_uri,
        })
        self.connection_id.authorization_result = False
        return state

    def _clear_oauth_state(self):
        self.write({
            'oauth_state_digest': False, 'oauth_session_digest': False, 'oauth_user_id': False,
            'oauth_expires_at': False, 'oauth_redirect_uri': False,
        })

    @api.model
    def _consume_authorization(self, state, sid):
        credentials = self.sudo()
        if not isinstance(state, str) or len(state) != 43:
            return credentials, None
        digest = self._digest(state)
        credential = credentials.search([('oauth_state_digest', '=', digest)], limit=1)
        if not credential or not credential._lock():
            return credentials, None
        if (
            not compare_digest(credential.oauth_state_digest or '', digest)
            or credential.connection_id.company_id not in self.env.user.company_ids
            or credential.oauth_user_id != self.env.user
            or not compare_digest(credential.oauth_session_digest or '', self._digest(sid))
            or not credential.oauth_expires_at or credential.oauth_expires_at <= fields.Datetime.now()
        ):
            return credentials, None
        redirect_uri = credential.oauth_redirect_uri
        credential._clear_oauth_state()
        return credential, redirect_uri

    def _finish_authorization(self, code, error, redirect_uri):
        self.ensure_one()
        connection = self.connection_id
        if error == 'access_denied':
            connection.authorization_result = 'denied'
            return
        if error or not isinstance(code, str) or not code or len(code) > 191:
            connection.authorization_result = 'failed'
            return
        try:
            tokens = square_request(connection.environment, 'POST', '/oauth2/token', payload={
                'grant_type': 'authorization_code', 'code': code, 'redirect_uri': redirect_uri,
                'client_id': connection.application_id, 'client_secret': self._unseal(self.application_secret),
            })
            values = self._token_values(tokens)
            merchant_id = tokens['merchant_id']
            if connection.merchant_id and connection.merchant_id != merchant_id:
                connection.authorization_result = 'merchant_mismatch'
                return
            locations = square_request(
                connection.environment, 'GET', '/v2/locations', access_token=tokens['access_token'],
            )
            connection._store_locations(locations, merchant_id)
        except (SquareAPIError, UserError, ValueError):
            connection.authorization_result = 'failed'
            return
        self.write({**values, 'reauthorization_required': False})
        connection.write({'merchant_id': merchant_id, 'authorization_result': 'connected'})

    def _token_values(self, tokens):
        for name in ('merchant_id', 'access_token', 'refresh_token', 'expires_at', 'token_type'):
            if not isinstance(tokens.get(name), str) or not tokens[name]:
                raise ValueError('Invalid Square token response')
        expires_at = datetime.fromisoformat(tokens['expires_at'])
        if not expires_at.tzinfo or tokens['token_type'].lower() != 'bearer':
            raise ValueError('Invalid Square token response')
        expires_at = expires_at.astimezone(timezone.utc).replace(tzinfo=None)
        now = fields.Datetime.now()
        if expires_at <= now:
            raise ValueError('Expired Square token')
        return {
            'access_token': self._seal(tokens['access_token']),
            'refresh_token': self._seal(tokens['refresh_token']),
            'expires_at': expires_at,
            'refresh_after': min(now + timedelta(days=6), expires_at - timedelta(hours=1)),
        }

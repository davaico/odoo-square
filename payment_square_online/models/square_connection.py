from datetime import datetime, timedelta, timezone
from hashlib import sha256
import os
from secrets import compare_digest, token_urlsafe

from cryptography.fernet import Fernet, InvalidToken

from odoo import _, fields, models
from odoo.exceptions import UserError

from ..const import SQUARE_ENCRYPTION_KEY_ENV, SQUARE_LOCATION_CURRENCIES
from ..square_api import SquareAPIError, square_request


class SquareConnection(models.Model):
    _name = 'payment.square.connection'
    _description = 'Square Online OAuth Credentials'

    # This model has no RPC access. The provider form only accepts replacement secrets.
    provider_id = fields.Many2one('payment.provider', required=True, ondelete='cascade')
    merchant_id = fields.Char(index=True)
    application_secret = fields.Char()
    webhook_signature_key = fields.Char()
    access_token = fields.Char()
    refresh_token = fields.Char()
    expires_at = fields.Datetime()
    reauthorization_required = fields.Boolean()
    authorization_result = fields.Selection([
        ('connected', 'Square authorization succeeded'),
        ('denied', 'Authorization was denied in Square'),
        ('merchant_mismatch', 'Use the Square account originally connected to this provider'),
        ('failed', 'Square authorization failed. Start authorization again'),
    ])
    oauth_state_digest = fields.Char(index='btree_not_null')
    oauth_session_digest = fields.Char()
    oauth_user_id = fields.Many2one('res.users', ondelete='set null')
    oauth_expires_at = fields.Datetime()
    oauth_redirect_uri = fields.Char()

    _sql_constraints = [
        ('square_provider_unique', 'unique(provider_id)', 'A provider can have only one Square connection.'),
    ]

    @staticmethod
    def _cipher():
        try:
            return Fernet(os.environ.get(SQUARE_ENCRYPTION_KEY_ENV, '').encode())
        except ValueError:
            raise UserError(_("Configure a valid Square credential encryption key on the Odoo server.")) from None

    def _seal(self, value):
        return self._cipher().encrypt(value.encode()).decode()

    def _unseal(self, value):
        try:
            return self._cipher().decrypt(value.encode()).decode()
        except InvalidToken:
            raise UserError(_("Odoo cannot decrypt the Square credentials. Restore the original encryption key.")) from None

    def _lock(self):
        self.ensure_one()
        self.env.cr.execute('SELECT id FROM payment_square_connection WHERE id = %s FOR NO KEY UPDATE', [self.id])

    def _access_token_for_use(self):
        self.ensure_one()
        self._lock()
        if not self.reauthorization_required:
            if self.expires_at and self.expires_at > fields.Datetime.now():
                return self._unseal(self.access_token)
            if self.refresh_token and self.application_secret:
                if self._refresh_authorization():
                    return self._unseal(self.access_token)
                if not self.reauthorization_required:
                    raise UserError(_("Odoo could not renew the Square authorization. Try again later."))
        raise UserError(_("Reconnect the original Square account to recover this transaction."))

    def _refresh_authorization(self):
        """Renew the original grant while the caller holds the connection lock."""
        provider = self.provider_id
        try:
            tokens = square_request(provider.square_environment, 'POST', '/oauth2/token', payload={
                'grant_type': 'refresh_token',
                'client_id': provider.square_application_id,
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
        if values['merchant_id'] != self.merchant_id:
            self._record_authorization_failure()
            return False
        self.write(values)
        provider.invalidate_recordset(['square_connection_status'])
        return True

    def _record_authorization_failure(self):
        self.reauthorization_required = True
        self.provider_id.invalidate_recordset(['square_connection_status'])

    @staticmethod
    def _digest(value):
        return sha256(value.encode()).hexdigest()

    def _start_authorization(self, sid, redirect_uri):
        self.ensure_one()
        self._lock()
        self._unseal(self.application_secret)
        state = token_urlsafe(32)
        self.write({
            'oauth_state_digest': self._digest(state),
            'oauth_session_digest': self._digest(sid),
            'oauth_user_id': self.env.uid,
            'oauth_expires_at': fields.Datetime.now() + timedelta(minutes=15),
            'oauth_redirect_uri': redirect_uri,
            'authorization_result': False,
        })
        return state

    def _clear_oauth_state(self):
        self.write({
            'oauth_state_digest': False, 'oauth_session_digest': False, 'oauth_user_id': False,
            'oauth_expires_at': False, 'oauth_redirect_uri': False,
        })

    def _consume_authorization(self, state, sid):
        connections = self.sudo()
        if len(state) != 43:
            return connections, None
        digest = self._digest(state)
        connection = connections.search([('oauth_state_digest', '=', digest)], limit=1)
        if not connection:
            return connection, None
        connection._lock()
        if (
            not compare_digest(connection.oauth_state_digest or '', digest)
            or connection.provider_id.code != 'square_online'
            or connection.provider_id.company_id not in self.env.user.company_ids
            or connection.oauth_user_id != self.env.user
            or not compare_digest(connection.oauth_session_digest or '', self._digest(sid))
            or not connection.oauth_expires_at or connection.oauth_expires_at <= fields.Datetime.now()
        ):
            return connections, None
        redirect_uri = connection.oauth_redirect_uri
        connection._clear_oauth_state()
        return connection, redirect_uri

    def _finish_authorization(self, code, error, redirect_uri):
        self.ensure_one()
        if error == 'access_denied':
            self.authorization_result = 'denied'
            return
        if error or not code or len(code) > 191:
            self.authorization_result = 'failed'
            return
        provider = self.provider_id
        try:
            tokens = square_request(provider.square_environment, 'POST', '/oauth2/token', payload={
                'grant_type': 'authorization_code', 'code': code, 'redirect_uri': redirect_uri,
                'client_id': provider.square_application_id,
                'client_secret': self._unseal(self.application_secret),
            })
            token_values = self._token_values(tokens)
            if self.merchant_id and self.merchant_id != token_values['merchant_id']:
                self.authorization_result = 'merchant_mismatch'
                return
            locations = square_request(
                provider.square_environment, 'GET', '/v2/locations', access_token=tokens['access_token'],
            )
            location_values = self._location_values(locations, token_values['merchant_id'])
        except (SquareAPIError, UserError, ValueError):
            self.authorization_result = 'failed'
            return
        self.write({
            **token_values, 'reauthorization_required': False, 'authorization_result': 'connected',
        })
        self._store_locations(location_values)

    def _token_values(self, tokens):
        for name in ('merchant_id', 'access_token', 'refresh_token', 'expires_at', 'token_type'):
            if not isinstance(tokens.get(name), str) or not tokens[name]:
                raise ValueError('Invalid Square token response')
        expires_at = datetime.fromisoformat(tokens['expires_at'])
        if not expires_at.tzinfo or tokens['token_type'].lower() != 'bearer':
            raise ValueError('Invalid Square token response')
        expires_at = expires_at.astimezone(timezone.utc).replace(tzinfo=None)
        if expires_at <= fields.Datetime.now():
            raise ValueError('Expired Square token')
        return {
            'merchant_id': tokens['merchant_id'],
            'access_token': self._seal(tokens['access_token']),
            'refresh_token': self._seal(tokens['refresh_token']),
            'expires_at': expires_at,
        }

    @staticmethod
    def _location_values(response, merchant_id):
        result = {}
        locations = response.get('locations', [])
        if not isinstance(locations, list):
            raise ValueError('Invalid Square location response')
        for location in locations:
            if not isinstance(location, dict) or location.get('merchant_id') != merchant_id:
                raise ValueError('Square location belongs to a different merchant')
            country = location.get('country')
            currency = location.get('currency')
            square_id = location.get('id')
            capabilities = location.get('capabilities')
            if not (
                isinstance(square_id, str) and square_id and isinstance(country, str)
                and currency and SQUARE_LOCATION_CURRENCIES.get(country) == currency
                and location.get('status') == 'ACTIVE'
                and isinstance(capabilities, list) and 'CREDIT_CARD_PROCESSING' in capabilities
            ):
                continue
            result[square_id] = {
                'name': f"{location.get('name') or square_id} ({country}, {currency})",
                'country_code': country, 'currency_code': currency, 'is_available': True,
            }
        return result

    def _store_locations(self, location_values):
        locations = self.env['payment.square.location'].search([('provider_id', '=', self.provider_id.id)])
        for location in locations:
            location.write(location_values.pop(location.square_id, {'is_available': False}))
        locations.create([
            {**values, 'square_id': square_id, 'provider_id': self.provider_id.id}
            for square_id, values in location_values.items()
        ])
        self.provider_id.invalidate_recordset([
            'square_connection_status', 'square_merchant_id', 'square_authorization_result',
        ])

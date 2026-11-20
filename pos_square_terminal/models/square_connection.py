# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from urllib.parse import urlencode, urlsplit

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.http import request

from ..const import SQUARE_BASE_URLS, SQUARE_OAUTH_CALLBACK_PATH, SQUARE_OAUTH_SCOPES
from ..square_api import SquareAPIError, square_request


class SquareConnection(models.Model):
    _name = 'pos.square.connection'
    _description = 'Square Terminal Connection'
    _check_company_auto = True

    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', required=True, default=lambda self: self.env.company, index=True,
    )
    environment = fields.Selection(
        [('production', 'Production'), ('sandbox', 'Sandbox')], required=True, default='production',
    )
    application_id = fields.Char(required=True)
    new_application_secret = fields.Char(compute='_compute_new_application_secret', inverse='_inverse_secret')
    merchant_id = fields.Char(readonly=True, copy=False, index=True)
    authorization_result = fields.Selection([
        ('connected', 'Square authorization succeeded'),
        ('denied', 'Authorization was denied in Square'),
        ('merchant_mismatch', 'Reconnect the original merchant account'),
        ('failed', 'Square authorization failed. Check the application credentials and authorize again'),
        ('revoked', 'Square authorization expired or was revoked. Reconnect the original merchant account'),
    ], readonly=True, copy=False)
    callback_url = fields.Char(compute='_compute_callback_url')
    location_ids = fields.One2many('pos.square.location', 'connection_id', readonly=True)

    def _compute_new_application_secret(self):
        for connection in self:
            connection.new_application_secret = False

    def _inverse_secret(self):
        # create/write consume the replacement before the ORM can store or return it.
        pass

    def _compute_callback_url(self):
        for connection in self:
            connection.callback_url = connection.get_base_url().rstrip('/') + SQUARE_OAUTH_CALLBACK_PATH

    _provider_fields = {'merchant_id', 'authorization_result', 'location_ids'}

    @api.model_create_multi
    def create(self, vals_list):
        vals_list = [dict(vals) for vals in vals_list]
        secrets = [vals.pop('new_application_secret', False) for vals in vals_list]
        if not self.env.su:
            if any(
                self._provider_fields & vals.keys() for vals in vals_list
            ) or any(f'default_{field}' in self.env.context for field in self._provider_fields):
                raise AccessError(_("Square verifies the merchant, authorization, and locations."))
            for vals in vals_list:
                vals.update(merchant_id=False, authorization_result=False, location_ids=[])
        connections = super().create(vals_list)
        credential_model = self.env['pos.square.credential'].sudo()
        credential_model.create([
            {'connection_id': connection.id, 'application_secret': credential_model._seal(secret) if secret else False}
            for connection, secret in zip(connections, secrets)
        ])
        return connections

    def write(self, vals):
        vals = dict(vals)
        if not self.env.su:
            if self._provider_fields & vals.keys():
                raise AccessError(_("Square verifies the merchant, authorization, and locations."))
            if 'company_id' in vals and vals['company_id'] not in self.env.companies.ids:
                raise AccessError(_("Select a company available to you."))
        secret = vals.pop('new_application_secret', False)
        scope = {'company_id', 'environment', 'application_id'} & vals.keys()
        if scope or secret:
            for connection in self:
                connection.check_access('write')
                credential = connection._credential()
                credential._lock()
                changed = any(
                    vals[name] != (connection.company_id.id if name == 'company_id' else connection[name])
                    for name in scope
                )
                if changed and connection.merchant_id:
                    raise ValidationError(_("Create a new connection to change the company, environment, or application."))
                if changed or secret:
                    credential._clear_oauth_state()
                if secret:
                    credential.application_secret = credential._seal(secret)
                elif changed:
                    credential.application_secret = False
        return super().write(vals)

    def _credential(self):
        self.ensure_one()
        return self.env['pos.square.credential'].sudo().search([('connection_id', '=', self.id)], limit=1)

    def _authorization_error(self):
        self.ensure_one()
        return self._credential()._authorization_error()

    def _request(self, method, path, **kwargs):
        """Use the original connection even if it has been archived for new sales."""
        self.ensure_one()
        credential = self._credential()
        token = credential._access_token_for_use()
        try:
            return square_request(self.environment, method, path, access_token=token, **kwargs)
        except SquareAPIError as error:
            if error.is_authorization_error:
                credential._record_authorization_failure()
            raise

    def action_authorize(self):
        self.ensure_one()
        self.check_access('write')
        if not request or request.session.uid != self.env.uid:
            raise AccessError(_("Sign in as a Point of Sale administrator to authorize Square."))
        credential = self._credential()
        redirect_uri = self.callback_url
        address = urlsplit(redirect_uri)
        local_sandbox = (
            self.environment == 'sandbox' and address.scheme == 'http' and address.hostname == 'localhost'
        )
        if (
            not address.hostname or address.username or address.query or address.fragment
            or (address.scheme != 'https' and not local_sandbox)
        ):
            raise UserError(_("Set Odoo's web.base.url to the registered HTTPS address. Sandbox also supports HTTP localhost."))
        state = credential._start_authorization(request.session.sid, redirect_uri, self.env.uid)
        params = {
            'client_id': self.application_id, 'scope': ' '.join(SQUARE_OAUTH_SCOPES),
            'state': state, 'redirect_uri': redirect_uri,
        }
        if self.environment == 'production':
            params['session'] = 'false'
        return {
            'type': 'ir.actions.act_url', 'target': 'self',
            'url': f'{SQUARE_BASE_URLS[self.environment]}/oauth2/authorize?{urlencode(params)}',
        }

    def action_refresh_locations(self):
        self.ensure_one()
        self.check_access('write')
        try:
            response = self._request('GET', '/v2/locations')
            self._store_locations(response, self.merchant_id)
        except SquareAPIError as error:
            return {
                'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'type': 'danger', 'sticky': True, 'message': (
                    _("Reconnect the original Square merchant account.")
                    if error.is_authorization_error else _("Square locations could not be refreshed. Try again.")
                )},
            }
        except ValueError:
            raise UserError(_("Square returned locations outside the authorized merchant account.")) from None
        return True

    def _store_locations(self, response, merchant_id):
        self.ensure_one()
        values = self.env['pos.square.location']._square_location_values(response, merchant_id)
        locations = self.env['pos.square.location'].sudo().search([('connection_id', '=', self.id)])
        for location in locations:
            location.write(values.pop(location.square_id, {'is_available': False}))
        locations.create([
            {**location_values, 'square_id': square_id, 'connection_id': self.id}
            for square_id, location_values in values.items()
        ])

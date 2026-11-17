from urllib.parse import urlencode, urlsplit

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.http import request

from odoo.addons.payment import utils as payment_utils

from ..const import SQUARE_BASE_URLS, SQUARE_OAUTH_CALLBACK_PATH, SQUARE_OAUTH_SCOPES


class PaymentProvider(models.Model):
    _inherit = 'payment.provider'

    code = fields.Selection(
        selection_add=[('square_online', 'Square Online Payments')],
        ondelete={'square_online': 'set default'},
    )
    square_environment = fields.Selection(
        [('sandbox', 'Sandbox'), ('production', 'Production')],
        default='sandbox', copy=False, string='Square Environment',
    )
    square_application_id = fields.Char(string='Square Application ID', copy=False)
    square_application_secret = fields.Char(
        string='New Square Application Secret', compute='_compute_square_application_secret',
        inverse='_inverse_square_application_secret', groups='base.group_system',
    )
    square_has_application_secret = fields.Boolean(compute='_compute_square_connection_status')
    square_connection_status = fields.Selection([
        ('configure', 'Enter Square application credentials'),
        ('authorize', 'Authorize with Square'),
        ('expired', 'Square authorization expired. Reconnect to Square'),
        ('credentials_unavailable', 'Restore the Square credential encryption key'),
        ('no_locations', 'Square returned no active locations eligible for card processing'),
        ('select_location', 'Select an eligible Square location'),
        ('select_journal', 'Select a bank journal for this company'),
        ('ready', 'Connected'),
    ], compute='_compute_square_connection_status', string='Square Connection')
    square_merchant_id = fields.Char(compute='_compute_square_connection_status', string='Square Merchant ID')
    square_authorization_result = fields.Selection(
        selection=lambda self: self.env['payment.square.connection']._fields['authorization_result'].selection,
        compute='_compute_square_connection_status', string='Last Authorization Result',
    )
    square_location_id = fields.Many2one(
        'payment.square.location', string='Square Location', check_company=True,
        ondelete='set null', copy=False,
    )
    square_callback_url = fields.Char(compute='_compute_square_callback_url', string='OAuth Redirect URL')

    def _compute_square_application_secret(self):
        self.square_application_secret = False

    def _inverse_square_application_secret(self):
        for provider in self.filtered(lambda p: p.code == 'square_online' and p.square_application_secret):
            connection = provider._square_get_connection(create=True)
            connection._lock()
            connection.application_secret = connection._seal(provider.square_application_secret)
            connection._clear_oauth_state()
        self.invalidate_recordset(['square_application_secret', 'square_has_application_secret', 'square_connection_status'])

    @api.depends('code', 'square_application_id', 'square_location_id', 'company_id', 'journal_id')
    def _compute_square_connection_status(self):
        for provider in self:
            connection = provider._origin._square_get_connection()
            provider.square_has_application_secret = bool(connection.application_secret)
            provider.square_merchant_id = connection.merchant_id
            provider.square_authorization_result = connection.authorization_result
            provider.square_connection_status = (
                provider._square_connection_status(connection) if provider.code == 'square_online' else False
            )

    def _compute_square_callback_url(self):
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url', '')
        self.square_callback_url = base_url.rstrip('/') + SQUARE_OAUTH_CALLBACK_PATH

    def _square_connection_status(self, connection):
        self.ensure_one()
        if not (self.square_application_id and connection.application_secret):
            return 'configure'
        if not connection.access_token:
            return 'authorize'
        if not connection.expires_at or connection.expires_at <= fields.Datetime.now():
            return 'expired'
        try:
            connection._unseal(connection.access_token)
        except UserError:
            return 'credentials_unavailable'
        if not self._square_has_eligible_location():
            if not self.env['payment.square.location'].sudo().search_count([
                ('provider_id', '=', self._origin.id), ('is_available', '=', True),
            ], limit=1):
                return 'no_locations'
            return 'select_location'
        if not self._square_has_eligible_journal():
            return 'select_journal'
        return 'ready'

    def _square_has_eligible_location(self):
        self.ensure_one()
        location = self.square_location_id.sudo()
        return location.is_available and location.provider_id.id == self._origin.id

    def _square_has_eligible_journal(self):
        self.ensure_one()
        journal = self.journal_id.sudo()
        return journal.type == 'bank' and journal.company_id == self.company_id

    @api.constrains('code', 'company_id', 'website_id', 'square_location_id')
    def _check_square_ownership(self):
        for provider in self.filtered(lambda p: p.code == 'square_online'):
            if provider.website_id and provider.website_id.company_id != provider.company_id:
                raise ValidationError(_("The Square provider and its website must belong to the same company."))
            if provider.square_location_id and not provider._square_has_eligible_location():
                raise ValidationError(_("Select an available location from this Square connection."))

    @api.constrains('code', 'square_environment', 'square_application_id')
    def _check_square_application_environment(self):
        for provider in self.filtered(lambda p: p.code == 'square_online'):
            if not provider.square_environment:
                raise ValidationError(_("Select Sandbox or Production for this Square provider."))
            if provider.square_application_id and (
                provider.square_application_id.startswith('sandbox-') != (provider.square_environment == 'sandbox')
            ):
                raise ValidationError(_("The Square application ID must match the selected environment."))

    @api.constrains('code', 'state', 'square_environment', 'square_location_id', 'journal_id')
    def _check_square_configuration(self):
        for provider in self.filtered(lambda p: p.code == 'square_online' and p.state != 'disabled'):
            expected_state = 'test' if provider.square_environment == 'sandbox' else 'enabled'
            if provider.state != expected_state:
                raise ValidationError(_("Use Test Mode with Square Sandbox and Enabled with Square Production."))
            if provider._square_connection_status(provider._square_get_connection()) != 'ready':
                raise ValidationError(_("Connect Square and select an eligible location and bank journal before enabling it."))

    def write(self, values):
        identity = {'code', 'company_id', 'square_environment', 'square_application_id'}
        if identity.intersection(values):
            for provider in self.filtered(lambda p: p.code == 'square_online'):
                connection = provider._square_get_connection()
                if not connection:
                    continue
                connection._lock()
                changed = any(
                    field in values and values[field] != (
                        provider.company_id.id if field == 'company_id' else provider[field]
                    ) for field in identity
                )
                if changed:
                    if connection.merchant_id and (not self.env.su or values.get('code') != 'none'):
                        raise ValidationError(_("Duplicate the provider to use a different company, environment, or application."))
                    if not connection.merchant_id:
                        connection.application_secret = False
                    connection._clear_oauth_state()
        return super().write(values)

    def _square_get_connection(self, create=False):
        self.ensure_one()
        connections = self.env['payment.square.connection'].sudo()
        if self.code != 'square_online' or not self.id:
            return connections
        connection = connections.search([('provider_id', '=', self.id)], limit=1)
        if not connection and create:
            connection = connections.create({'provider_id': self.id})
        return connection

    def action_square_authorize(self):
        self.ensure_one()
        self.check_access('write')
        if not self.env.user.has_group('base.group_system') or not request or request.session.uid != self.env.uid:
            raise AccessError(_("Sign in as an administrator to authorize Square."))
        connection = self._square_get_connection()
        if not self.square_application_id or not connection.application_secret:
            raise UserError(_("Save your Square application ID and secret before authorizing Square."))
        redirect_uri = self.square_callback_url
        base_url = urlsplit(redirect_uri)
        local_sandbox = (
            self.square_environment == 'sandbox' and base_url.scheme == 'http'
            and base_url.hostname == 'localhost'
        )
        if (
            not base_url.hostname or base_url.username or base_url.query or base_url.fragment
            or (base_url.scheme != 'https' and not local_sandbox)
        ):
            raise UserError(_("Set Odoo's web.base.url to the registered HTTPS address. Sandbox also supports HTTP localhost."))
        state = connection._start_authorization(request.session.sid, redirect_uri)
        params = {
            'client_id': self.square_application_id, 'scope': ' '.join(SQUARE_OAUTH_SCOPES),
            'state': state, 'redirect_uri': redirect_uri,
        }
        if self.square_environment == 'production':
            params['session'] = 'false'
        return {
            'type': 'ir.actions.act_url', 'target': 'self',
            'url': f'{SQUARE_BASE_URLS[self.square_environment]}/oauth2/authorize?{urlencode(params)}',
        }

    def _get_default_payment_method_codes(self):
        if self.code == 'square_online':
            return {'card'}
        return super()._get_default_payment_method_codes()

    @api.model
    def _get_compatible_providers(self, company_id, partner_id, amount, currency_id=None, *args, report=None, **kwargs):
        providers = super()._get_compatible_providers(
            company_id, partner_id, amount, currency_id, *args, report=report, **kwargs,
        )
        currency = self.env['res.currency'].browse(currency_id).exists()
        unavailable = providers.filtered(lambda p: p.code == 'square_online' and not (
            p.company_id.id == company_id
            and (not p.website_id or p.website_id.id == kwargs.get('website_id'))
            and p.inline_form_view_id
            and currency.name == p.square_location_id.sudo().currency_code
            and p.square_connection_status == 'ready'
        ))
        payment_utils.add_to_report(
            report, unavailable, available=False,
            reason=_("Square requires a ready connection for this company, website, and currency."),
        )
        return providers - unavailable

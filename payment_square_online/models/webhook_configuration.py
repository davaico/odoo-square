from urllib.parse import urlsplit

from odoo import _, api, fields, models
from odoo.exceptions import ValidationError

from ..const import SQUARE_WEBHOOK_PATH


class PaymentProvider(models.Model):
    _inherit = 'payment.provider'

    square_webhook_url = fields.Char(
        string='Square Notification URL', copy=False, groups='base.group_system',
        help='Enter the exact HTTPS Notification URL registered in the Square Developer Console.',
    )
    square_webhook_signature_key = fields.Char(
        string='New Square Webhook Signature Key', compute='_compute_square_webhook_key',
        inverse='_inverse_square_webhook_key', groups='base.group_system',
    )
    square_has_webhook_signature_key = fields.Boolean(
        string='Webhook Signature Key Saved', compute='_compute_square_webhook_key', groups='base.group_system',
    )
    square_webhook_endpoint = fields.Char(
        string='Webhook Endpoint', compute='_compute_square_webhook_endpoint', groups='base.group_system',
    )

    def _compute_square_webhook_key(self):
        for provider in self:
            connection = (provider._origin or provider)._square_get_connection()
            provider.square_webhook_signature_key = False
            provider.square_has_webhook_signature_key = bool(connection.webhook_signature_key)

    def _inverse_square_webhook_key(self):
        for provider in self.filtered(lambda record: record.code == 'square_online' and record.square_webhook_signature_key):
            connection = provider._square_get_connection(create=True)
            connection._lock()
            connection.webhook_signature_key = connection._seal(provider.square_webhook_signature_key)
        self.invalidate_recordset(['square_webhook_signature_key', 'square_has_webhook_signature_key'])

    def _compute_square_webhook_endpoint(self):
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url', '').rstrip('/')
        for provider in self:
            provider_id = provider._origin.id
            provider.square_webhook_endpoint = (
                f'{base_url}{SQUARE_WEBHOOK_PATH}/{provider_id}'
                if provider.code == 'square_online' and provider_id else False
            )

    @api.constrains('square_webhook_url')
    def _check_square_webhook_url(self):
        for provider in self.filtered('square_webhook_url'):
            url = provider.square_webhook_url
            try:
                parts = urlsplit(url)
                valid = (
                    url.startswith('https://') and parts.hostname and parts.username is None and parts.password is None
                    and not any(character.isspace() or character in '?#' for character in url)
                    and parts.path == f'{SQUARE_WEBHOOK_PATH}/{provider.id}'
                )
            except ValueError:
                valid = False
            if not valid or provider.code != 'square_online':
                raise ValidationError(_("Enter the exact HTTPS URL for this provider's Square webhook endpoint."))

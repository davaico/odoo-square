from urllib.parse import urlsplit

from odoo import _, api, fields, models
from odoo.exceptions import ValidationError

from ..const import SQUARE_TERMINAL_WEBHOOK_PATH


class SquareCredential(models.Model):
    _inherit = 'pos.square.credential'

    webhook_signature_key = fields.Char()


class SquareConnection(models.Model):
    _inherit = 'pos.square.connection'

    webhook_endpoint = fields.Char(compute='_compute_webhook_endpoint')
    webhook_url = fields.Char(
        string='Square Notification URL', copy=False,
        help='Enter the exact HTTPS Notification URL registered in the Square Developer Console.',
    )
    new_webhook_signature_key = fields.Char(
        string='New Webhook Signature Key', compute='_compute_webhook_key', inverse='_inverse_webhook_key',
    )
    has_webhook_signature_key = fields.Boolean(compute='_compute_webhook_key')

    @api.model_create_multi
    def create(self, vals_list):
        vals_list = [dict(vals) for vals in vals_list]
        keys = [vals.pop('new_webhook_signature_key', False) for vals in vals_list]
        connections = super().create(vals_list)
        for connection, key in zip(connections, keys):
            if key:
                connection.new_webhook_signature_key = key
        return connections

    def _compute_webhook_endpoint(self):
        for connection in self:
            record_id = connection._origin.id
            connection.webhook_endpoint = (
                f"{connection.get_base_url().rstrip('/')}{SQUARE_TERMINAL_WEBHOOK_PATH}/{record_id}"
                if record_id else False
            )

    def _compute_webhook_key(self):
        for connection in self:
            connection.new_webhook_signature_key = False
            credential = connection._origin._credential() if connection._origin else False
            connection.has_webhook_signature_key = bool(credential and credential.webhook_signature_key)

    def _inverse_webhook_key(self):
        for connection in self.filtered('new_webhook_signature_key'):
            connection.check_access('write')
            credential = connection._credential()
            credential._lock()
            credential.webhook_signature_key = credential._seal(connection.new_webhook_signature_key)
        self.invalidate_recordset(['new_webhook_signature_key', 'has_webhook_signature_key'])

    @api.constrains('webhook_url')
    def _check_webhook_url(self):
        for connection in self.filtered('webhook_url'):
            url = connection.webhook_url
            try:
                parts = urlsplit(url)
                valid = (
                    url.startswith('https://') and parts.hostname and parts.username is None
                    and not any(character.isspace() or character in '?#' for character in url)
                    and parts.path == f'{SQUARE_TERMINAL_WEBHOOK_PATH}/{connection.id}'
                )
            except ValueError:
                valid = False
            if not valid:
                raise ValidationError(_("Enter this connection's exact HTTPS Square webhook URL."))

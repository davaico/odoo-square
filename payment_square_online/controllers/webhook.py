import hmac
import json
from base64 import b64encode

from odoo.exceptions import UserError
from odoo.http import Controller, request, route

from ..const import SQUARE_WEBHOOK_PATH


_MAX_NOTIFICATION_BYTES = 1024 * 1024


class SquareOnlineWebhook(Controller):

    @route(f'{SQUARE_WEBHOOK_PATH}/<int:provider_id>', type='http', auth='public',
           methods=['POST'], csrf=False, save_session=False, sitemap=False)
    def square_webhook(self, provider_id):
        provider = request.env['payment.provider'].sudo().browse(provider_id).exists()
        if not provider or provider.code != 'square_online' or not provider.square_webhook_url:
            return request.make_response('', status=403)
        connection = provider._square_get_connection()
        if not connection.merchant_id or not connection.webhook_signature_key:
            return request.make_response('', status=403)
        signature = request.httprequest.headers.get('x-square-hmacsha256-signature', '')
        if not signature:
            return request.make_response('', status=403)
        if request.httprequest.headers.get('Content-Length', 0, type=int) > _MAX_NOTIFICATION_BYTES:
            return request.make_response('', status=413)
        body = request.httprequest.data
        if len(body) > _MAX_NOTIFICATION_BYTES:
            return request.make_response('', status=413)
        try:
            key = connection._unseal(connection.webhook_signature_key).encode()
        except UserError:
            return request.make_response('', status=503)
        expected_signature = b64encode(hmac.digest(key, provider.square_webhook_url.encode() + body, 'sha256'))
        if not hmac.compare_digest(expected_signature, signature.encode()):
            return request.make_response('', status=403)

        try:
            payload = json.loads(body)
        except (ValueError, RecursionError):
            return request.make_response('', status=400)
        if not isinstance(payload, dict) or payload.get('merchant_id') != connection.merchant_id:
            return request.make_response('', status=403)
        status = request.env['payment.square.webhook.event'].sudo()._square_receive_event(connection, payload)
        return request.make_response('', status=status)

import hmac
import json
from base64 import b64encode

from odoo.exceptions import UserError
from odoo.http import Controller, request, route

from ..const import SQUARE_TERMINAL_WEBHOOK_PATH


_MAX_NOTIFICATION_BYTES = 1024 * 1024


class SquareTerminalWebhook(Controller):

    @route(f'{SQUARE_TERMINAL_WEBHOOK_PATH}/<int:connection_id>', type='http', auth='public',
           methods=['POST'], csrf=False, save_session=False, sitemap=False)
    def square_terminal_webhook(self, connection_id):
        connection = request.env['pos.square.connection'].sudo().browse(connection_id).exists()
        if not connection or not connection.webhook_url or not connection.merchant_id:
            return request.make_response('', status=403)
        credential = connection._credential()
        signature = request.httprequest.headers.get('x-square-hmacsha256-signature', '')
        if not credential.webhook_signature_key or not signature:
            return request.make_response('', status=403)
        if request.httprequest.headers.get('Content-Length', 0, type=int) > _MAX_NOTIFICATION_BYTES:
            return request.make_response('', status=413)
        body = request.httprequest.data
        if len(body) > _MAX_NOTIFICATION_BYTES:
            return request.make_response('', status=413)
        try:
            key = credential._unseal(credential.webhook_signature_key).encode()
        except UserError:
            return request.make_response('', status=503)

        expected = b64encode(hmac.digest(key, connection.webhook_url.encode() + body, 'sha256'))
        if not hmac.compare_digest(expected, signature.encode()):
            return request.make_response('', status=403)
        try:
            payload = json.loads(body)
        except (ValueError, RecursionError):
            return request.make_response('', status=400)
        if not isinstance(payload, dict) or payload.get('merchant_id') != connection.merchant_id:
            return request.make_response('', status=403)
        status = request.env['pos.square.webhook.event'].sudo()._receive_event(connection, payload)
        return request.make_response('', status=status)

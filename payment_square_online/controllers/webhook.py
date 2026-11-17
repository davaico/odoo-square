# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import json

from werkzeug.exceptions import BadRequest, Forbidden

from odoo.http import Controller, request, route

from ..square_api import SQUARE_ONLINE_WEBHOOK_PATH, SquareAPIError


class SquareOnlineWebhook(Controller):

    @route(
        SQUARE_ONLINE_WEBHOOK_PATH + "/<int:connection_id>",
        type="http", auth="public", csrf=False, methods=["POST"], save_session=False,
    )
    def square_online_webhook(self, connection_id, **kwargs):
        incoming = request.httprequest
        signature = incoming.headers.get("X-Square-Hmacsha256-Signature")
        if not signature:
            raise Forbidden()
        body = incoming.get_data()
        connection = request.env["square.online.connection"].sudo().browse(connection_id).exists()
        if not connection or not connection._square_online_webhook_authenticated(signature, body):
            raise Forbidden()
        try:
            event = json.loads(body)
        except (UnicodeDecodeError, ValueError, RecursionError):
            raise BadRequest() from None
        if not isinstance(event, dict) or not isinstance(event.get("type"), str):
            raise BadRequest()
        if (
            incoming.headers.get("Square-Environment", "").lower() != connection.environment
            or not connection.merchant_id
            or connection.merchant_id != event.get("merchant_id")
            or event.get("location_id") not in (None, connection.location_id.square_id)
        ):
            return request.make_response("")
        data = event.get("data")
        object_data = data.get("object") if isinstance(data, dict) else None
        resource_type = event["type"].partition(".")[0]
        resource = object_data.get(resource_type) if isinstance(object_data, dict) else None
        if isinstance(resource, dict) and resource.get("id") and resource["id"] == data.get("id"):
            try:
                with request.env.cr.savepoint():
                    request.env["payment.transaction"].sudo()._square_online_handle_webhook(
                        connection, event["type"], object_data,
                    )
            except SquareAPIError:
                # A verified event whose Square lookup is temporarily unavailable can be retried.
                return request.make_response("", status=503)
        return request.make_response("")

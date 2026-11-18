# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import logging
import re

from odoo import _, http
from odoo.http import request

from ..square_api import OAUTH_CALLBACK_PATH


_CALLBACK_QUERY = re.compile(rf"({re.escape(OAUTH_CALLBACK_PATH)}/?)\?\S+")
_CALLBACK_HEADERS = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}


class _RedactSquareCallback(logging.Filter):
    def filter(self, record):
        # Odoo's access logger includes the full GET request target. Cancel out
        # the OAuth query string while leaving other access log entries intact.
        message = record.getMessage()
        redacted = _CALLBACK_QUERY.sub(r"\1", message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


logging.getLogger("werkzeug").addFilter(_RedactSquareCallback())


class SquareOnlineOAuth(http.Controller):

    @http.route(
        OAUTH_CALLBACK_PATH, type="http", auth="public", methods=["GET"], sitemap=False,
    )
    def square_online_oauth_callback(self, **kwargs):
        if not request.session.uid or not request.env.user.has_group("base.group_system"):
            return request.make_response(
                _("Sign in as an Odoo administrator to connect Square."),
                headers=_CALLBACK_HEADERS,
                status=403,
            )

        parameters = request.httprequest.args
        connection = request.env["square.online.connection"]
        if all(len(parameters.getlist(key)) <= 1 for key in ("state", "code", "error")):
            connection = connection._square_online_finish_oauth(
                parameters.get("state", ""),
                parameters.get("code"),
                parameters.get("error"),
                request.session.sid,
            )
        if not connection:
            return request.make_response(
                _("This Square authorization link is invalid or has expired. Start again in Odoo."),
                headers=_CALLBACK_HEADERS,
                status=400,
            )
        action = request.env.ref("payment_square_online.square_online_connection_action")
        response = request.redirect(f"/odoo/action-{action.id}/{connection.id}", code=303)
        response.headers.update(_CALLBACK_HEADERS)
        return response

import logging
import re

from odoo import _, http
from odoo.http import request

from ..const import SQUARE_OAUTH_CALLBACK_PATH


class SquareOAuthLogFilter(logging.Filter):
    """Keep OAuth codes and state out of Odoo's built-in HTTP access logs."""

    _query = re.compile(rf'({re.escape(SQUARE_OAUTH_CALLBACK_PATH)})\?\S+')

    def filter(self, record):
        message = record.getMessage()
        if self._query.search(message):
            record.msg = self._query.sub(r'\1', message)
            record.args = ()
        return True


logging.getLogger('werkzeug').addFilter(SquareOAuthLogFilter())


class SquareOAuth(http.Controller):
    _callback_headers = [('Cache-Control', 'no-store'), ('Referrer-Policy', 'no-referrer')]

    @http.route(SQUARE_OAUTH_CALLBACK_PATH, type='http', auth='public', methods=['GET'])
    def square_oauth_callback(self, state='', code='', error='', **kwargs):
        if not request.session.uid or not request.env.user.has_group('point_of_sale.group_pos_manager'):
            return request.make_response(
                _("Sign in as an administrator to authorize Square."), self._callback_headers, status=403,
            )
        connection, redirect_uri = request.env['pos.square.credential']._consume_authorization(
            state, request.session.sid,
        )
        if not connection:
            return request.make_response(
                _("Invalid or expired Square authorization. Start authorization again from Odoo."),
                self._callback_headers, status=400,
            )
        connection._finish_authorization(code, error, redirect_uri)
        action = request.env.ref('pos_square_terminal.action_square_connection')
        response = request.redirect(f'/odoo/action-{action.id}/{connection.connection_id.id}', code=303)
        response.headers.update(self._callback_headers)
        return response

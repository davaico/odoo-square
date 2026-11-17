# -*- coding: utf-8 -*-
import base64
import hmac
import json
from urllib.parse import urlsplit

from odoo.tests.common import HttpCase


class SquareHttpCase(HttpCase):
    """HttpCase that clears ORM cache after each HTTP request.

    The test opener flushes/clears the cursor before dispatch; the worker still
    updates rows that the test Environment may have cached as absent or stale.
    Webhook tests must see DB state after ``url_open``.
    """

    def url_open(self, url, *args, **kwargs):
        res = super().url_open(url, *args, **kwargs)
        self.env.invalidate_all()
        return res

    square_test_key = "test_signature_key"

    def _post_signed_square_webhook(self, url, *, data=None, headers=None, **kwargs):
        if "json" in kwargs:
            data = json.dumps(kwargs.pop("json"))
        raw = data.encode() if isinstance(data, str) else data
        signed_url = f"https://{urlsplit(self.base_url()).netloc}{url}".encode()
        signature = base64.b64encode(hmac.digest(
            self.square_test_key.encode(), signed_url + raw, "sha256",
        )).decode()
        return self.url_open(url, data=raw, headers={
            "Content-Type": "application/json",
            **(headers or {}),
            "X-Square-Hmacsha256-Signature": signature,
        }, **kwargs)

    def _square_rpc(self, model, method, *args):
        result = self.url_open(
            f"/web/dataset/call_kw/{model}/{method}",
            json={
                "jsonrpc": "2.0",
                "method": "call",
                "params": {"model": model, "method": method, "args": args, "kwargs": {}},
                "id": 1,
            },
        )
        self.assertEqual(result.status_code, 200)
        return result.json()

    def _assert_square_rpc_denied(self, model, method, *args):
        result = self._square_rpc(model, method, *args)
        self.assertIn("error", result, (model, method))
        self.assertIn(result["error"]["data"]["name"], (
            "odoo.exceptions.AccessError", "builtins.AttributeError", "werkzeug.exceptions.NotFound",
        ))

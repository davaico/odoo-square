# -*- coding: utf-8 -*-
from base64 import b64encode
from hmac import digest
from urllib.parse import urlsplit

from odoo.tests.common import HttpCase


class SquareHttpCase(HttpCase):
    """HttpCase that clears ORM cache after each HTTP request.

    The test opener flushes/clears the cursor before dispatch; the worker still
    updates rows that the test Environment may have cached as absent or stale.
    Webhook tests must see DB state after ``url_open``.
    """

    def url_open(self, url, data=None, files=None, timeout=12, headers=None, allow_redirects=True, head=False):
        res = super().url_open(
            url,
            data=data,
            files=files,
            timeout=timeout,
            headers=headers,
            allow_redirects=allow_redirects,
            head=head,
        )
        self.env.invalidate_all()
        return res

    def post_square_webhook(self, data, headers=None):
        route = "/square/webhook"
        url = "https://" + urlsplit(self.base_url()).netloc + route
        payload = data.encode("utf-8") if isinstance(data, str) else data
        key = self.env["square.config"].sudo().search([], limit=1).square_webhook_signature_key
        signature = b64encode(digest(key.encode(), url.encode() + payload, "sha256")).decode()
        return self.url_open(route, data=payload, headers={
            "Content-Type": "application/json",
            **(headers or {}),
            "x-square-hmacsha256-signature": signature,
        })

    def pay_invoice(self, invoice, journal=None):
        values = {"journal_id": journal.id} if journal else {}
        self.env["account.payment.register"].with_context(
            active_model="account.move", active_ids=invoice.ids
        ).create(values).action_create_payments()
        self.assertAlmostEqual(invoice.amount_residual, 0, places=2)

# -*- coding: utf-8 -*-
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

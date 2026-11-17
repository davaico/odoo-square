# -*- coding: utf-8 -*-
from uuid import uuid4

from odoo import SUPERUSER_ID, api
from odoo.service.model import retrying
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestSquareRefundRetry(TransactionCase):
    def test_refund_creation_retries_when_another_transaction_inserts_it(self):
        square_order_id = f"refund-race-order-{uuid4()}"
        refund_data = {
            "id": f"refund-race-{uuid4()}",
            "order_id": square_order_id,
            "status": "PENDING",
            "amount_money": {"amount": 120, "currency": "EUR"},
        }
        with self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            order_id = env["sale.order"].create({
                "name": square_order_id,
                "partner_id": env.ref("base.main_partner").id,
                "square_order_id": square_order_id,
            }).id
        self.addCleanup(self._remove_sale_order, order_id)

        refund_domain = [("square_refund_id", "=", refund_data["id"])]
        with self.registry.cursor() as stale_cr:
            stale_env = api.Environment(stale_cr, SUPERUSER_ID, {})
            stale_refunds = stale_env["square.refund"]
            self.assertFalse(stale_refunds.search(refund_domain))

            # Commit a competing insert after the other transaction has taken its snapshot.
            with self.registry.cursor() as winning_cr:
                winning_env = api.Environment(winning_cr, SUPERUSER_ID, {})
                winner_id = winning_env["square.refund"]._create_from_square_data(
                    refund_data, winning_env["sale.order"].browse(order_id)
                ).id

            self.assertFalse(stale_refunds.search(refund_domain))
            completed_data = {**refund_data, "status": "COMPLETED"}
            refund = retrying(
                lambda: stale_refunds._create_from_square_data(
                    completed_data, stale_env["sale.order"].browse(order_id)
                ),
                stale_env,
            )
            self.assertEqual(refund.id, winner_id)
            self.assertEqual(refund.status, "completed")
            self.assertEqual(stale_refunds.search(refund_domain), refund)

    def _remove_sale_order(self, order_id):
        with self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["sale.order"].browse(order_id).unlink()

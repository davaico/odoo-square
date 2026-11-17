# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import _, models


class SquareOrderProcessor(models.Model):
    _inherit = "square.order.processor"

    def _square_online_event_result(
        self, *, order_id=None, payment_id=None, refund_id=None, location_id=None,
        merchant_id=None, environment=None, square_config=None, full_order=None,
    ):
        """Keep verified website activity out of the POS write paths."""
        transactions = self.env["payment.transaction"].sudo()
        if not hasattr(transactions, "_square_online_pos_scope"):
            return None
        config = square_config if square_config is not None else self.env["square.config"].sudo().search(
            [], limit=1,
        )
        if not config:
            return None
        if environment and environment.lower() != config.square_environment:
            return {"status": "ignored", "message": _("Square event is from a different environment.")}

        scope = transactions._square_online_pos_scope(
            config.square_environment, merchant_id, location_id,
        )
        if transactions._square_online_pos_knows(
            scope, order_id=order_id, payment_id=payment_id, refund_id=refund_id,
        ):
            belongs_to_website = True
        elif not transactions.search_count(scope, limit=1):
            return None
        else:
            client = self.env["square.api.client"].sudo()
            if payment_id:
                belongs_to_website = self._square_online_check_payment(
                    transactions, scope, client, config, payment_id,
                )
            elif order_id:
                belongs_to_website = self._square_online_check_order(
                    transactions, scope, client, config, order_id, full_order=full_order,
                )
            else:
                return None

        if belongs_to_website is True:
            return {"status": "ignored", "message": _("Square website activity is handled by Square Online Payments.")}
        if belongs_to_website is None:
            return {"status": "retry", "message": _("Square order or payment ownership could not yet be verified.")}
        return None

    def _square_online_check_payment(self, transactions, scope, client, config, payment_id, order_id=None):
        if not transactions._square_online_pos_has_unknown_charge(scope):
            return False
        payment = client.get_payment(payment_id, square_config=config)
        if not payment or payment.get("id") != payment_id:
            return None
        if order_id and payment.get("order_id") not in (None, order_id):
            return None
        return transactions._square_online_pos_owns_payment(scope, payment)

    def _square_online_check_order(
        self, transactions, scope, client, config, order_id, *, full_order=None, follow_returns=True,
    ):
        order = full_order if full_order is not None else client.get_order(order_id, square_config=config)
        if not order or (order.get("id") or order.get("order_id")) != order_id:
            return None
        if order.get("location_id"):
            scope &= transactions._square_online_pos_scope(
                config.square_environment, location_id=order["location_id"],
            )
        if not transactions.search_count(scope, limit=1):
            return False

        # Square can create a separate return order for an online refund. Such an
        # order may have no tender of its own; follow its original sale instead.
        result = False
        sources = set()
        if follow_returns:
            sources = {
                entry["source_order_id"]
                for entry in order.get("returns", [])
                if entry.get("source_order_id") and entry["source_order_id"] != order_id
            }
            for source_id in sorted(sources):
                if transactions._square_online_pos_knows(scope, order_id=source_id):
                    return True
                source = self._square_online_check_order(
                    transactions, scope, client, config, source_id, follow_returns=False,
                )
                if source is True:
                    return True
                if source is None:
                    result = None

        tenders = order.get("tenders") or []
        for tender in tenders:
            payment_id = tender.get("payment_id") or tender.get("id")
            if not payment_id:
                continue
            if transactions._square_online_pos_knows(scope, payment_id=payment_id):
                return True
            if tender.get("type") not in (None, "CARD"):
                continue
            payment = self._square_online_check_payment(
                transactions, scope, client, config, payment_id, order_id=order_id,
            )
            if payment is True:
                return True
            if payment is None:
                result = None
        if not (tenders or sources) and transactions._square_online_pos_has_unknown_charge(scope):
            # The order may be visible before its website tender is attached.
            return None
        return result

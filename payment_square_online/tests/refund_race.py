# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
# Run with `odoo shell` on a fresh, disposable Odoo 19 database with
# payment_square_online installed. This fixture commits across real connections.
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from time import monotonic, sleep
from unittest.mock import Mock, patch

from psycopg2.errors import SerializationFailure

from odoo import Command, api
from odoo.exceptions import ValidationError

provider = env.ref("payment_square_online.payment_provider_square_online")
company = provider.company_id
assert company.currency_id.name == "USD"
website = env.ref("website.default_website")
partner = env["res.partner"].create({
    "name": "Refund race fixture", "email": "refund-race@example.test",
    "street": "25 Main Street", "city": "Albany", "zip": "12201", "phone": "+1 555 555 0199",
    "country_id": env.ref("base.us").id, "state_id": env.ref("base.state_us_27").id,
})
connection = env["square.online.connection"].create({
    "name": "Refund race sandbox", "company_id": company.id,
    "application_id": "sandbox-refund-race", "application_secret": "mock-race-secret",
})
boundary = "odoo.addons.payment_square_online.square_api.requests.request"


def response(body):
    return Mock(status_code=200, json=Mock(return_value=body))


with patch(boundary, side_effect=[
    response({
        "merchant_id": "MERCHANT-RACE", "access_token": "mock-race-access",
        "refresh_token": "mock-race-refresh", "token_type": "bearer",
        "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
    }),
    response({"locations": [{
        "id": "LOCATION-RACE", "name": "Race seller", "merchant_id": "MERCHANT-RACE",
        "country": "US", "currency": "USD", "status": "ACTIVE",
        "capabilities": ["CREDIT_CARD_PROCESSING"],
    }]}),
]):
    connection._square_online_authorize("mock-race-oauth-code")
connection.location_id = connection.available_location_ids
provider.write({
    "square_online_sandbox_connection_id": connection.id, "state": "test", "is_published": True,
    "journal_id": env["account.journal"].search([
        ("company_id", "=", company.id), ("type", "=", "bank"),
    ], limit=1).id,
})
# OAuth refresh uses a separate connection and must see the committed tokens,
# just as checkout reads a merchant account configured in an earlier request.
env.cr.commit()
product = env["product.product"].create({
    "name": "Refund race service", "type": "service", "list_price": 47,
    "website_published": True, "taxes_id": [Command.clear()],
})
company_id, administrator = company.id, env.ref("base.user_admin").id


def create_capture(reference):
    order = env["sale.order"].create({
        "partner_id": partner.id, "website_id": website.id,
        "order_line": [Command.create({"product_id": product.id, "product_uom_qty": 1})],
    })
    tx = env["payment.transaction"].sudo().create({
        "provider_id": provider.id, "currency_id": order.currency_id.id,
        "amount": order.amount_total, "partner_id": partner.id,
        "payment_method_id": env.ref("payment.payment_method_card").id,
        "operation": "online_direct", "sale_order_ids": [Command.set(order.ids)],
    })
    tx._square_online_prepare_checkout(order, website.id)
    assert tx._square_online_claim_payment(website.id, order.id)
    payment = {
        "id": reference, "source_type": "CARD", "status": "COMPLETED",
        "reference_id": tx.square_online_operation_key, "location_id": "LOCATION-RACE",
        "amount_money": {"amount": 4700, "currency": "USD"},
    }
    assert tx._square_online_apply_payment(payment, connection)
    tx._post_process()
    assert tx.payment_id.move_id.state == "posted"
    return tx.id, payment


cases = [
    (*create_capture("PARTIAL-FIRST"), 18.25, 47),
    (*create_capture("FULL-FIRST"), 47, 18.25),
]
dashboard_id, dashboard_payment = create_capture("DASHBOARD-FIRST")
env.cr.commit()


def parent(cr, transaction_id):
    return api.Environment(cr, administrator, {"allowed_company_ids": [company_id]})[
        "payment.transaction"
    ].browse(transaction_id)


def run_competing_request(transaction_id, amount, started, backend):
    with env.registry.cursor() as cr:
        tx = parent(cr, transaction_id)
        assert tx.payment_id.amount_available_for_refund == 47
        cr.execute("SELECT pg_backend_pid()")
        backend.append(cr.fetchone()[0])
        started.set()
        tx.action_refund(amount_to_refund=amount)


def verify_refund_contention(transaction_id, first_operation, second):
    started = Event()
    backend = []
    with ThreadPoolExecutor(max_workers=1) as pool:
        with env.registry.cursor() as cr:
            tx = parent(cr, transaction_id)
            first_operation(tx)
            cr.execute("SELECT pg_backend_pid()")
            holder = cr.fetchone()[0]
            contender = pool.submit(run_competing_request, transaction_id, second, started, backend)
            assert started.wait(10), "Competing request did not start"
            deadline = monotonic() + 10
            while monotonic() < deadline:
                cr.execute("SELECT %s = ANY(pg_blocking_pids(%s))", (holder, backend[0]))
                if cr.fetchone()[0]:
                    break
                sleep(0.025)
            else:
                raise AssertionError("The two merchant actions never contended for the same payment")
        try:
            contender.result(timeout=10)
        except SerializationFailure:
            pass
        else:
            raise AssertionError("The stale merchant action did not abort")

    try:
        with env.registry.cursor() as cr:
            parent(cr, transaction_id).action_refund(amount_to_refund=second)
    except ValidationError:
        pass
    else:
        raise AssertionError("A fresh retry reserved more than the remaining balance")


for transaction_id, payment, first, second in cases:
    verify_refund_contention(transaction_id, lambda tx: tx.action_refund(amount_to_refund=first), second)
    with env.registry.cursor() as cr:
        tx = parent(cr, transaction_id)
        assert tx.child_transaction_ids.state == "pending"
        assert tx.child_transaction_ids.amount == -first
        assert tx.payment_id.amount_available_for_refund == 47 - first
    print(f"Verified overlap: reserved {first}, blocked {second}, then rejected a fresh retry.")


def apply_dashboard_refund(tx):
    tx = tx.sudo()
    refund = {
        "id": "REFUND-DASHBOARD-FIRST", "payment_id": dashboard_payment["id"],
        "location_id": dashboard_payment["location_id"], "status": "COMPLETED",
        "amount_money": {"amount": 1825, "currency": "USD"},
    }
    tx._square_online_handle_webhook(tx.square_online_connection_id, "refund.updated", {"refund": refund})


with patch(boundary) as external:
    verify_refund_contention(dashboard_id, apply_dashboard_refund, 47)
    external.assert_not_called()
with env.registry.cursor() as cr:
    tx = parent(cr, dashboard_id)
    child = tx.child_transaction_ids
    assert child.state == "done" and child.amount == -18.25 and child.square_online_refund_origin == "square"
    assert tx.payment_id.amount_available_for_refund == 28.75
print("Verified Square dashboard refund overlapped a local request and reduced its available balance.")


payments = {cash["id"]: (tx_id, cash, int(round(first * 100))) for tx_id, cash, first, _ in cases}


def square(method, url, *, headers, json, **kwargs):
    if method == "GET":
        assert "/v2/payments/" in url, url
        return response({"payment": payments[url.rsplit("/", 1)[-1]][1]})
    assert method == "POST" and url.endswith("/v2/refunds"), (method, url)
    transaction_id, payment, amount = payments[json["payment_id"]]
    assert json["amount_money"] == {"amount": amount, "currency": "USD"}, json
    with env.registry.cursor() as cr:
        child = parent(cr, transaction_id).child_transaction_ids
        assert child.state == "pending" and child.square_online_dispatch_at
        assert child.square_online_operation_key == json["idempotency_key"]
        assert child.square_online_refund_reason == json["reason"]
    return response({"refund": {
        "id": "REFUND-" + payment["id"], "payment_id": payment["id"], "location_id": "LOCATION-RACE",
        "reason": json["reason"], "status": "COMPLETED", "amount_money": json["amount_money"],
    }})


with patch(boundary, side_effect=square) as external:
    for _ in range(2):
        with env.registry.cursor() as cr:
            api.Environment(cr, administrator, {}).ref(
                "payment_square_online.cron_square_online_dispatch_refunds"
            ).method_direct_trigger()
    assert [call.args[0] for call in external.call_args_list] == ["GET", "POST", "GET", "POST"]

with env.registry.cursor() as cr:
    api.Environment(cr, administrator, {}).ref("payment.cron_post_process_payment_tx").method_direct_trigger()

for transaction_id, payment, first, _ in cases:
    with env.registry.cursor() as cr:
        child = parent(cr, transaction_id).child_transaction_ids
        assert child.provider_reference == "REFUND-" + payment["id"]
        assert child.payment_id.amount == first and child.payment_id.payment_type == "outbound"
        assert child.payment_id.move_id.state == "posted"
with env.registry.cursor() as cr:
    tx = parent(cr, dashboard_id)
    child = tx.child_transaction_ids
    assert child.payment_id.amount == 18.25 and child.payment_id.payment_type == "outbound"
    assert child.payment_id.move_id.state == "posted"
print("Verified local requests dispatched once, Square ingestion made no request, and each refund posted once.")

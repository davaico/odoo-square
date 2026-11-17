from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {"active_test": False})
    configs = env["square.config"].search([])
    for config in configs:
        mapped_companies = config.location_mapping_ids.warehouse_id.company_id
        if config.payment_journal_id:
            config.company_id = config.payment_journal_id.company_id
        elif len(mapped_companies) == 1:
            config.company_id = mapped_companies

    # Previously writable snapshots have no evidence of authorization.
    cr.execute("""
        UPDATE square_webhook_queue
           SET order_data = '{}',
               state = CASE WHEN state IN ('pending', 'processing') THEN 'failed' ELSE state END,
               error_message = 'Legacy payload removed during the Square POS upgrade'
         WHERE verified IS NOT TRUE
    """)
    cr.execute("""
        UPDATE square_integration_log AS log
           SET company_id = sale.company_id
          FROM sale_order AS sale
         WHERE log.sale_order_id = sale.id
    """)
    cr.execute("UPDATE square_integration_log SET technical_details = NULL")
    cr.execute("UPDATE square_refund SET square_data = NULL")

    bot = env.ref("odoo_square.user_square_bot")
    trusted_users = bot | env.ref("base.user_root") | env.ref("base.user_admin")
    sales = env["sale.order"].search([("square_order_id", "!=", False)])
    for sale in sales:
        if sale.create_uid not in trusted_users:
            sale.write({
                "square_order_id": False,
                "square_payment_id": False,
                "square_order_version": 0,
                "square_order_data": False,
            })
        elif sale.square_order_data:
            sale.write({
                "square_order_version": sale._square_applied_order_version(),
                "square_order_data": False,
            })

    for mapping in configs.location_mapping_ids:
        if not mapping.write_uid.with_user(mapping.write_uid).env.is_system():
            mapping.active = False

    # Old refund data was editable by sales staff, including its identity and
    # document links. Preserve supported completions and release tainted IDs
    # so a new signed event can rebuild the record from Square and Odoo.
    refunds = env["square.refund"].search([])
    for refund in refunds:
        if refund.processed_at and refund._has_settled_refund():
            continue
        if refund.create_uid not in trusted_users or refund.write_uid not in trusted_users:
            refund.unlink()
        elif refund.processed_at:
            refund.processed_at = False

    # This specific log was created by the bot inside the legacy exchange
    # settlement savepoint. Odoo may flush its stored name as the system user.
    exchange_logs = env["square.integration.log"].search([
        ("create_uid", "=", bot.id),
        ("write_uid", "in", [bot.id, SUPERUSER_ID]),
        ("event_type", "=", "order_updated"),
        ("status", "=", "success"),
        ("title", "=like", "Échange traité: %"),
        ("square_order_id", "!=", False),
        ("sale_order_id", "!=", False),
    ])
    receipts = env["square.event.receipt"]
    for log in exchange_logs:
        order = log.sale_order_id
        company_receipts = receipts.with_company(order.company_id)
        key = str(order.id)
        if not company_receipts._was_applied("exchange", log.square_order_id, key):
            company_receipts._record_applied("exchange", log.square_order_id, key)

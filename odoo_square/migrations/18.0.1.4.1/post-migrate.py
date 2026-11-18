from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    bot = env.ref("odoo_square.user_square_bot")
    for config in env["square.config"].with_context(active_test=False).search([]):
        config._authorize_company_for_bot()

    # Pre-upgrade queue bodies could originate from an ordinary RPC write.
    cr.execute("""
        UPDATE square_webhook_queue
           SET order_data = '{}',
               config_id = COALESCE(config_id, (SELECT min(id) FROM square_config)),
               state = CASE WHEN state IN ('pending', 'processing') THEN 'failed' ELSE state END,
               error_message = CASE WHEN state IN ('pending', 'processing')
                   THEN 'Unverified legacy event; waiting for a new signed delivery.' END
    """)

    # Keep settled exchanges corroborated by actual Odoo accounting. Do not
    # treat a legacy diagnostic record as authenticated provider input.
    cr.execute("""
        INSERT INTO square_webhook_event (config_id, event_key)
        SELECT DISTINCT config.id, 'exchange:' || log.square_order_id
          FROM square_integration_log log
          JOIN sale_order sale ON sale.id = log.sale_order_id
          JOIN square_config config ON config.company_id = sale.company_id
         WHERE log.event_type = 'exchange_processed' AND log.status = 'success'
           AND log.create_uid = %s AND log.write_uid = %s AND log.square_order_id IS NOT NULL
           AND (
               EXISTS (SELECT 1 FROM account_move credit
                        WHERE credit.company_id = sale.company_id
                          AND credit.move_type = 'out_refund' AND credit.state = 'posted'
                          AND credit.ref = 'Échange - Avoir: ' || sale.name)
               OR EXISTS (SELECT 1 FROM sale_order replacement
                          JOIN account_move invoice
                            ON invoice.company_id = replacement.company_id
                           AND invoice.invoice_origin = replacement.name
                         WHERE replacement.company_id = sale.company_id
                           AND replacement.square_order_id = log.square_order_id
                           AND replacement.create_uid = %s
                           AND invoice.move_type = 'out_invoice' AND invoice.state = 'posted')
           )
        ON CONFLICT (config_id, event_key) DO NOTHING
    """, (bot.id, bot.id, bot.id))

    cr.execute("""
        UPDATE square_integration_log AS log
           SET technical_details = NULL, description = NULL,
               company_id = COALESCE(
                   (SELECT company_id FROM sale_order WHERE id = log.sale_order_id),
                   (SELECT company_id FROM square_config ORDER BY id LIMIT 1)
               )
    """)
    cr.execute("""
        UPDATE square_refund
           SET square_data = jsonb_build_object('status', upper(status)),
               refund_reason = NULL, processing_errors = NULL,
               legacy_processed = (processed_at IS NOT NULL),
               integrity_hold = (create_uid IS DISTINCT FROM %s OR write_uid IS DISTINCT FROM %s)
        RETURNING sale_order_id
    """, (bot.id, bot.id))
    sale_orders = env["sale.order"].browse(set(sale_id for sale_id, in cr.fetchall()))
    env.add_to_compute(sale_orders._fields["refund_status"], sale_orders)

    # Keep only line identifiers needed to process future refunds on old sales.
    orders = env["sale.order"]
    last_id = 0
    while batch := orders.search([
        ("id", ">", last_id), ("square_order_data", "!=", False),
    ], order="id", limit=500):
        for order in batch:
            order.square_order_data = str(order._square_snapshot_for_storage(order._square_order_snapshot()))
        last_id = batch[-1].id
    env.flush_all()

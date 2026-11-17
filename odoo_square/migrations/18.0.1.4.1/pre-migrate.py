def migrate(cr, version):
    # Keep the company already responsible for existing POS accounting and
    # warehouse records. The addon has a single POS configuration.
    cr.execute("ALTER TABLE square_config ADD COLUMN IF NOT EXISTS company_id integer")
    cr.execute("""
        UPDATE square_config AS cfg
           SET company_id = COALESCE(
               (SELECT company_id FROM account_journal WHERE id = cfg.payment_journal_id),
               (SELECT min(warehouse.company_id)
                  FROM square_location_mapping AS mapping
                  JOIN stock_warehouse AS warehouse ON warehouse.id = mapping.warehouse_id
                 WHERE mapping.config_id = cfg.id
                HAVING count(DISTINCT warehouse.company_id) = 1),
               (SELECT company_id FROM res_users WHERE id = (
                   SELECT res_id FROM ir_model_data
                    WHERE module = 'odoo_square' AND name = 'user_square_bot'
               ))
           )
         WHERE cfg.company_id IS NULL
    """)

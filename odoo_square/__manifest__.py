# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
{
    "name": "Square POS Integration",
    "version": "19.0.1.4.3",
    "category": "Sales",
    "summary": "Complete Square POS integration for orders, refunds, exchanges and inventory sync",
    "description": """
Square POS Integration
======================
A complete integration module connecting Odoo with Square POS system.

Features:
- Webhook processing for orders, refunds, and exchanges
- Send Odoo stock counts to mapped Square locations
- Smart customer matching (email → phone → name → auto-create)
- Complete audit trail with integration logs
- Support for full refunds, equivalent exchanges, and price-difference exchanges
    """,
    "author": "Davai",
    "website": "https://github.com/davaico/odoo-square",
    "depends": ["base", "web", "sale", "sales_team", "account", "stock", "sale_stock", "payment"],
    "external_dependencies": {
        "python": ["requests"],
    },
    "data": [
        "security/square_security.xml",
        "security/ir.model.access.csv",
        "data/square_bot_user.xml",
        "data/square_products.xml",
        "data/square_webhook_queue_cron.xml",
        "data/square_stock_count_cron.xml",
        "views/views.xml",
        "views/square_config_views.xml",
        "views/square_stock_sync_views.xml",
        "views/square_integration_log_views.xml",
        "views/square_webhook_queue_views.xml",
        "views/square_manual_resync_views.xml",
    ],
    "images": ["static/description/square-terminal.png"],
    "installable": True,
    "auto_install": False,
    "application": True,
    "license": "AGPL-3",
}

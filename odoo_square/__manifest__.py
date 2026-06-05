# -*- coding: utf-8 -*-
# Copyright 2024 Davai
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
{
    "name": "Odoo Square Integration",
    "version": "17.0.1.3.0",
    "category": "Sales",
    "summary": "Complete Odoo-Square integration for orders, refunds, exchanges and inventory sync",
    "description": """
Odoo Square Integration
=======================
A complete integration module connecting Odoo with Square POS system.

Features:
- Webhook processing for orders, refunds, and exchanges
- Bidirectional inventory synchronization
- Smart customer matching (email → phone → name → auto-create)
- Complete audit trail with integration logs
- Support for full refunds, equivalent exchanges, and price-difference exchanges
    """,
    "author": "Davai",
    "website": "https://github.com/davaico/odoo-square",
    "depends": ["base", "web", "sale", "sales_team", "account", "stock", "payment"],
    "external_dependencies": {
        "python": ["requests"],
    },
    "data": [
        "security/ir.model.access.csv",
        "data/square_bot_user.xml",
        "data/square_products.xml",
        "views/views.xml",
        "views/square_config_views.xml",
        "views/square_stock_sync_views.xml",
        "views/square_integration_log_views.xml",
        "views/square_webhook_queue_views.xml",
        "views/square_manual_resync_views.xml",
    ],
    "images": ["static/description/icon.png"],
    "installable": True,
    "auto_install": False,
    "application": True,
    "license": "AGPL-3",
}

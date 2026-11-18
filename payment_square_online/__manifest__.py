# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
{
    "name": "Square Online Payments",
    "summary": "Accept Square card payments at Odoo website checkout",
    "description": (
        "Connect your Square merchant account to a self-hosted Odoo 19 Community store. "
        "Customers pay in the embedded Square card form, and accounting managers can "
        "recover payments and issue refunds in Odoo. Square POS is optional.\n\n"
        "Follow the Square Online Payments guide in the repository to configure your "
        "Square application and test checkout in Sandbox."
    ),
    "version": "19.0.1.0.0",
    "category": "Accounting/Payment Providers",
    "author": "Davai",
    "website": "https://github.com/davaico/odoo-square",
    "support": "support@davai.co",
    "images": ["static/description/square-online-payments.png"],
    "depends": ["website_sale"],
    "external_dependencies": {"python": ["cryptography", "requests"]},
    "data": [
        "security/ir.model.access.csv",
        "security/square_online_rules.xml",
        "views/square_online_connection_views.xml",
        "views/payment_provider_views.xml",
        "views/payment_transaction_views.xml",
        "views/payment_form_templates.xml",
        "views/payment_recovery_templates.xml",
        "data/payment_provider_data.xml",
        "data/payment_recovery_data.xml",
        "data/payment_refund_data.xml",
        "data/square_online_connection_data.xml",
    ],
    "assets": {
        "web.assets_frontend": [
            "payment_square_online/static/src/interactions/payment_form.js",
        ],
    },
    "post_init_hook": "post_init_hook",
    "uninstall_hook": "uninstall_hook",
    "license": "AGPL-3",
    "installable": True,
    "application": True,
}

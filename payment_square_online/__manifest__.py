{
    "name": "Square Online Payments",
    "summary": "Accept Square card payments at Odoo website checkout",
    "description": (
        "Connect your Square merchant account to a self-hosted Odoo 18 Community store. "
        "Customers pay through the embedded Square card form, and accounting managers "
        "can recover payments and issue refunds in Odoo. Square POS is optional.\n\n"
        "Follow the Square Online Payments guide in the repository to configure your "
        "Square application and test checkout in Sandbox."
    ),
    "version": "18.0.1.0.0",
    "category": "Accounting/Payment Providers",
    "author": "Davai",
    "website": "https://github.com/davaico/odoo-square",
    "support": "support@davai.co",
    "images": ["static/description/online-card-payment.jpg"],
    "license": "AGPL-3",
    "depends": ["account_payment", "website_sale"],
    "external_dependencies": {"python": ["cryptography", "requests"]},
    "data": [
        "security/ir.model.access.csv",
        "security/payment_square_security.xml",
        "views/payment_square_templates.xml",
        "data/payment_provider_data.xml",
        "data/payment_recovery_data.xml",
        "views/payment_provider_views.xml",
        "views/payment_recovery_views.xml",
    ],
    "assets": {
        "web.assets_frontend": ["payment_square_online/static/src/js/payment_form.js"],
    },
    "post_init_hook": "post_init_hook",
    "uninstall_hook": "uninstall_hook",
    "installable": True,
}

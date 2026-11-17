{
    "name": "Square Online Payments",
    "summary": "Connect a Square account to Odoo website payments",
    "version": "18.0.1.0.0",
    "category": "Accounting/Payment Providers",
    "license": "AGPL-3",
    "depends": ["account_payment", "website_sale"],
    "external_dependencies": {"python": ["cryptography", "requests"]},
    "data": [
        "security/ir.model.access.csv",
        "security/payment_square_security.xml",
        "data/payment_provider_data.xml",
        "views/payment_provider_views.xml",
    ],
    "post_init_hook": "post_init_hook",
    "uninstall_hook": "uninstall_hook",
    "installable": True,
}

{
    "name": "Square Terminal for Point of Sale",
    "summary": "Take Odoo Point of Sale payments on Square Terminal",
    "version": "18.0.1.0.0",
    "category": "Sales/Point of Sale",
    "license": "AGPL-3",
    "depends": ["point_of_sale"],
    "external_dependencies": {"python": ["cryptography", "requests"]},
    "data": [
        "security/ir.model.access.csv",
        "security/square_company_rules.xml",
        "data/square_credential_cron.xml",
        "data/square_reconciliation_cron.xml",
        "views/square_terminal_views.xml",
        "views/pos_payment_method_views.xml",
        "views/square_reconciliation_views.xml",
    ],
    "assets": {
        "web.assets_tests": [
            "pos_square_terminal/static/tests/payment_readiness_tour.js",
            "pos_square_terminal/static/tests/terminal_checkout_tour.js",
        ],
        "point_of_sale._assets_pos": [
            "pos_square_terminal/static/src/payment_readiness.js",
            "pos_square_terminal/static/src/payment_readiness.xml",
            "pos_square_terminal/static/src/square_payment.js",
            "pos_square_terminal/static/src/square_payment.xml",
            "pos_square_terminal/static/src/square_register_close.js",
        ],
    },
    "installable": True,
}

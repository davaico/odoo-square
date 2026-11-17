SQUARE_BASE_URLS = {
    'sandbox': 'https://connect.squareupsandbox.com',
    'production': 'https://connect.squareup.com',
}
SQUARE_API_VERSION = '2026-09-16'
SQUARE_OAUTH_CALLBACK_PATH = '/payment/square/online/oauth/callback'
SQUARE_OAUTH_SCOPES = (
    'MERCHANT_PROFILE_READ', 'ORDERS_READ', 'ORDERS_WRITE', 'PAYMENTS_READ', 'PAYMENTS_WRITE',
)
SQUARE_ENCRYPTION_KEY_ENV = 'ODOO_SQUARE_ONLINE_ENCRYPTION_KEY'
SQUARE_LOCATION_CURRENCIES = {
    'AU': 'AUD',
    'CA': 'CAD',
    'ES': 'EUR',
    'FR': 'EUR',
    'GB': 'GBP',
    'IE': 'EUR',
    'JP': 'JPY',
    'US': 'USD',
}

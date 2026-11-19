SQUARE_BASE_URLS = {
    'sandbox': 'https://connect.squareupsandbox.com',
    'production': 'https://connect.squareup.com',
}
SQUARE_API_VERSION = '2026-09-16'
SQUARE_OAUTH_CALLBACK_PATH = '/pos/square/terminal/oauth/callback'
SQUARE_OAUTH_SCOPES = (
    'MERCHANT_PROFILE_READ', 'DEVICE_CREDENTIAL_MANAGEMENT', 'DEVICES_READ',
    'ORDERS_READ', 'ORDERS_WRITE', 'PAYMENTS_READ', 'PAYMENTS_WRITE', 'ITEMS_READ',
)
SQUARE_ENCRYPTION_KEY_ENV = 'ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY'
SQUARE_LOCATION_CURRENCIES = {
    'AU': 'AUD', 'CA': 'CAD', 'ES': 'EUR', 'FR': 'EUR',
    'GB': 'GBP', 'IE': 'EUR', 'JP': 'JPY', 'US': 'USD',
}
SQUARE_SANDBOX_DEVICE_ID = '9fa747a2-25ff-48ee-b078-04381f7c828f'
SQUARE_TERMINAL_WEBHOOK_PATH = '/pos/square/terminal/webhook'

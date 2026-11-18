# Square Online Payments for Odoo 18

Install **Square Online Payments** (`payment_square_online`) on a self-hosted Odoo Community 18 online store. Add it to an Odoo addons path, update the Apps list, and install it from Apps. The add-on installs its Website, eCommerce, and payment accounting dependencies. Square POS Integration is optional. Odoo Online, the hosted service at odoo.com, does not support this custom add-on.

## Connect your Square Sandbox account

1. Generate an encryption key privately with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Set `ODOO_SQUARE_ONLINE_ENCRYPTION_KEY` in the Odoo server environment and restart all workers. In this repository's development Compose setup, set it in your private `.env` file. Use the same key on all workers and back it up securely outside the database backup.
2. Set the Odoo system parameter `web.base.url` to the HTTPS origin you use to sign in. Set `web.base.url.freeze` to `True` to keep the registered origin stable. For local Sandbox testing, use the same HTTP localhost origin in Odoo and the browser, for example `http://localhost:8069`.
3. As an Odoo settings administrator, open **Website → Configuration → Payment Providers → Square Online Payments**. Verify the company and select **Sandbox** on the **Credentials** tab. Leave **Website** blank for all stores in that company or select one of its websites.
4. Create an application for your business in the [Square Developer Console](https://developer.squareup.com/apps). Select **Sandbox** and register the exact Odoo **OAuth Redirect URL** on the application's OAuth page. The path is `/payment/square/online/oauth/callback`. Omit this route's query strings from reverse proxy logs because they contain authorization codes. [Square OAuth walkthrough](https://developer.squareup.com/docs/oauth-api/walkthrough)
5. Enter the Sandbox application ID in **Square Application ID** and the Sandbox secret in **New Square Application Secret**. Save and confirm **Application Secret Saved**. Open the Dashboard for the intended Sandbox test seller in the same browser. If you need to create a test seller for OAuth consent, use **Sandbox test accounts → New sandbox test account** and clear automatic authorization for existing apps. In Odoo click **Authorize or Reconnect with Square**, then approve the requested access to locations, orders, payments, and refunds. When Odoo returns to the provider, verify the merchant ID and authorization result. [Square OAuth permissions](https://developer.squareup.com/docs/oauth-api/square-permissions)
6. Select an active **Square Location** with card processing under **Credentials** and an active bank journal from the same company under **Configuration**. Confirm that **Square Connection** says **Connected**. Set the provider to **Test Mode** and publish it on the test storefront using Odoo's **Published** control.

Square processes the full order total, including tax and shipping, in the selected location's currency. Set the storefront's checkout pricelist accordingly. Supported seller markets are Australia (AUD), Canada (CAD), France, Ireland and Spain (EUR), Japan (JPY), the United Kingdom (GBP), and the United States (USD). Amounts use whole yen for JPY and at most two decimal places for the other five currencies. International buyers can pay with eligible cards, subject to Square and issuer approval. The card issuer handles any currency conversion for the buyer. [Square international development](https://developer.squareup.com/docs/international-development)

## Set up Square webhooks

1. Under **Credentials → Square Webhooks** in the saved provider, copy **Webhook Endpoint**. For direct delivery from Square it must be publicly reachable over HTTPS and route to this database. The path is `/payment/square/online/webhook/<provider-id>`.
2. In the matching environment and application in the Square Developer Console, open **Webhooks → Subscriptions**. Create a subscription with the exact Notification URL, API version `2026-09-16`, and the events `payment.created`, `payment.updated`, `refund.created`, and `refund.updated`.
3. In Odoo enter the same **Square Notification URL**, paste the subscription key into **New Square Webhook Signature Key**, and save. Verify **Webhook Signature Key Saved**. Preserve the raw POST body and signature header through your reverse proxy. Update the key in Odoo whenever you rotate it in Square.
4. After the test payment and refund below, check Square's webhook delivery history for successful responses and confirm the corresponding Odoo transactions. Keep the subscription active while payments on this provider may need recovery. Clear **Square Notification URL** in Odoo to stop accepting deliveries.

You can run Sandbox checkout and use **Refresh status** on HTTP localhost. To test direct delivery from Square, use the public HTTPS endpoint for your test database. [Square webhook subscriptions](https://developer.squareup.com/docs/webhooks/step2subscribe) · [Square notification signatures](https://developer.squareup.com/docs/webhooks/step3validate)

## Test checkout and refunds

1. Open the configured storefront over HTTPS or use `http://localhost:8069` with your local Sandbox port. If you set a Content Security Policy, allow the Square SDK script, secure card frame, and network endpoints for the environment. [Square CSP instructions](https://developer.squareup.com/docs/web-payments/content-security-policy)
2. Add a product to your cart, fill in delivery and billing, and select **Card** under **Secured by Square Online Payments** on the payment step. Enter a Square Sandbox test card and click **Pay now**. Supply the postcode if prompted. Try a test card that requires authentication and a declined test card. Retry checkout after a definitive decline. If Odoo shows processing, check that payment's status before trying again. [Square Sandbox test cards and challenge codes](https://developer.squareup.com/docs/devtools/sandbox/payments)
3. Compare the totals on the confirmed Odoo order, its accounting payment, and the Square Sandbox payment. Check that Odoo used the selected bank journal. Invoices follow your normal Odoo invoicing policy, and warehouse staff handle deliveries through the usual fulfillment workflow.
4. As an accounting manager or settings administrator, open the original payment under **Invoicing → Customers → Payments** and click **Refund**. Use the prefilled amount to refund the remaining balance or enter a partial amount in the original currency. Square returns the money to the original payment method.
5. When Square confirms the refund, verify that its amount matches the linked outbound Odoo payment. Open **Refunds** on the original payment transaction to see its status. If it is pending, use **Refresh status** on that refund before submitting another request. Create and reconcile any required credit note in Invoicing and process any goods return separately in Inventory.

If the card option is missing, check publication, company and website assignment, the Square connection, and the order currency. If the card form fails to load on a local Sandbox checkout, use the hostname `localhost` and check the browser's Content Security Policy errors.

## Enable Production

1. Set `web.base.url` to the public HTTPS origin and keep it frozen. Duplicate the Sandbox provider to create a separate configuration. Keep the new provider **Disabled** and unpublished while you configure it. Select **Production** and verify the company and website.
2. Register the exact HTTPS OAuth redirect in the Square Developer Console's Production environment. Enter the Production application ID and secret in Odoo, authorize your production Square seller, and select its location and the correct Odoo bank journal.
3. Set up a separate Production webhook subscription using the new provider's endpoint and key. Update your Content Security Policy for the Production SDK and confirm it still allows the storefront to load.
4. When ready to accept payments, set the Production provider to **Enabled** and publish it. Disable the Sandbox provider if it was published on that storefront. Keep any provider with payment history available in Odoo so its original Square transactions can still be reviewed or recovered.

## Review pending payments and restore access

A buyer can close checkout while a payment is processing and return to their order in the portal. Odoo uses signed Square notifications and scheduled checks to recover unresolved payments. Accounting managers and settings administrators can open **Invoicing → Configuration → Online Payments → Square Payment History** and click **Refresh status** on the original transaction or refund. The action looks up existing Square operations. If Square collects a payment for a cancelled Odoo order, review the warning and use a separate refund if the money needs to be returned.

If staff change an order after payment starts, a later successful payment is recorded for its original amount. Odoo flags the mismatch and leaves confirmation and invoicing for authorized review. Check the order and payment before fulfilling the order or deciding on a refund.

Square Dashboard refunds also appear against the original website payment. **Maximum Refund Allowed** on that payment accounts for both pending and completed refunds, including those from Square. A confirmed failure releases the pending amount. Each refund transaction shows its status, origin, Square reference once available, and which user requested it from Odoo, where applicable.

Keep **Square Online Payments: Renew OAuth access** active under Odoo's Scheduled Actions. If **Square Connection** asks you to reconnect, click **Authorize or Reconnect with Square** and sign in with the original Square merchant. Save a replacement application secret first if you rotated it. Restore the original encryption key if Odoo reports that it cannot decrypt the credentials. Changes to the selected location apply to new checkouts. Existing payments keep their original merchant and location for recovery and refunds.

## Use the provider with Square POS Integration

When installing or upgrading both add-ons in one database, update both together. Keep the POS credentials, journals, and warehouse mappings in **Square POS Integration** settings. Register separate online and POS (`/square/webhook`) subscriptions with their own keys. Both can use the same Square merchant and location. Grant the POS access token read access to Orders, Payments and Refunds, and Locations so POS webhook processing, replay, and manual resync can identify website activity.

Genuine Square POS sales, refunds, and exchanges continue through POS import. Website payments and refunds follow their Odoo order and accounting workflows. If Square is temporarily unavailable while POS checks whether a resource belongs to website checkout, the webhook requests another delivery. Retry manual resync after Square access recovers.

For Odoo 18 regression testing with the test Compose configuration, use `ODOO_INIT_MODULES=payment_square_online,sale_stock,odoo_square` and `ODOO_TEST_TAGS=standard/payment_square_online,standard/odoo_square,square_coexistence`. To test the provider alone, use `ODOO_INIT_MODULES=payment_square_online,sale_stock` and `ODOO_TEST_TAGS=standard/payment_square_online`.

CI runs separate POS-only, standalone online, combined, and concurrent-refund jobs. For local online tests, set `ODOO_TEST_DOCKERFILE=config/Dockerfile.test` before the build in the [root test commands](../README.md#testing). The test image uses Chrome Stable from Google's signed package repository and the Python WebSocket dependency to run the embedded card browser test. Check for a nonzero passing test summary and no skipped addon tests. `ODOO_TEST_IMAGE` can select an already-built compatible image.

The refund concurrency fixture uses independent database connections and commits test records. Run it in its own disposable project from Bash:

```bash
(
  set -euo pipefail
  export DB_PASSWORD=ci_odoo_test DB_NAME=odoo_square_v18_test
  export ODOO_TEST_DOCKERFILE=config/Dockerfile.test
  compose=(docker compose -p "odoo-square-v18-test-race-$(date +%s)-$$" -f docker-compose.test.yml)
  trap '"${compose[@]}" down --volumes' EXIT
  "${compose[@]}" build --pull
  "${compose[@]}" run --rm -T odoo odoo --database="$DB_NAME" \
    --init=payment_square_online,sale_stock --stop-after-init --no-http
  printf '%s\n' \
    'from odoo.addons.payment_square_online.tests.test_refund_concurrency import run' \
    'run(env.registry)' \
    | "${compose[@]}" run --rm -T odoo odoo shell --database="$DB_NAME" --no-http
)
```

The fixture must report two passing tests. These tests simulate Square responses; use the Sandbox checks above to qualify the actual Square integration.

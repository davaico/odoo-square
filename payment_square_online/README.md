# Square Online Payments for Odoo 19 Community

## Install

Place `payment_square_online` on the Odoo addons path, restart Odoo, update the Apps list, and install **Square Online Payments**. The addon uses Odoo eCommerce and Accounting. The Square POS integration is optional.

The repository's Compose setup mounts the addon. On a fresh database, run:

```bash
docker compose run --rm odoo odoo --database=odoo_square_v19 --init=payment_square_online --stop-after-init
docker compose up -d
```

Set `SQUARE_ONLINE_ENCRYPTION_KEY` in the Odoo service environment before adding credentials. For the included Compose setup, put it in the private `.env` file. Generate a Fernet key with:

```bash
python3 -c 'import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())'
```

Use the same key for all Odoo processes serving the database. Back it up privately alongside your Odoo recovery materials. Restoring an Odoo database requires its original key to read the saved Square credentials.

## Create and authorize your Square application

1. Open the [Square Developer Console](https://developer.squareup.com/apps) and create an application owned by your business. Open its **OAuth** page and choose **Sandbox**.
2. In Odoo, go to **Website → Configuration → eCommerce → Payment Providers → Square Online → Credentials**. Select **Manage Square Connections** and create a connection. Set its company, select **Sandbox**, and enter the corresponding application ID and secret from Square.
3. Copy the connection's **OAuth Redirect URL** into the Square application's **OAuth → Redirect URL** setting. The path is `/payment/square_online/oauth/callback`. Odoo builds the URL from its `web.base.url` system parameter. Use HTTPS on a public host. For OAuth testing through a local browser, Square also allows a Sandbox URL such as `http://localhost:8070/payment/square_online/oauth/callback`. Open Odoo with that same hostname and port when starting authorization.
4. Open the Sandbox Square Dashboard for a test seller in a separate tab in the same browser. Return to the connection form and select **Authorize with Square**. Grant access to the intended seller account. Odoo displays the result and verified merchant ID when Square returns.
5. Choose a **Square Location**. The location selector shows active locations from the authorized account with card processing and a supported processing currency. The form shows the selected country's code and currency.

[Square OAuth walkthrough](https://developer.squareup.com/docs/oauth-api/walkthrough)

## Configure the payment provider

1. Return to **Square Online → Credentials** and choose your **Sandbox Connection**. Under **Configuration**, choose a bank **Payment Journal** from the same Odoo company. If the journal has a fixed currency, it must match the location. You can also use a journal with no fixed currency.
2. Set the provider to **Test Mode**. Select a **Website** beside the provider state if the provider should only appear on that website. Odoo shows this field when the user has multi-website access. Use the normal Odoo **Published** control to make the provider available to shoppers.
3. When ready to use live payments, configure the application's **Production** OAuth settings separately with an HTTPS URL and production credentials. Create a separate **Production** connection, authorize the production seller, choose its location, and assign it as the provider's **Production Connection**. Review the company, journal, website, and location before switching the provider to **Enabled**. To stop offering Square, set the provider to **Disabled**.

Square processes in the location's currency and the checkout currency must match. Square's card-processing markets are Australia (AUD), Canada (CAD), France, Ireland and Spain (EUR), Japan (JPY), the United Kingdom (GBP), and the United States (USD). Card approval for international shoppers depends on Square and their card issuers. [Square international development](https://developer.squareup.com/docs/international-development)

## Renew and reconnect Square authorization

Odoo renews the merchant's OAuth access automatically, generally every seven days, and checks again when it needs a Square API token. This works independently for Sandbox and Production, including connections kept for historical payments. A temporary Square outage can delay renewal. Odoo keeps using the current access token while it is valid.

- In **Manage Square Connections**, check **Authorization Status** and **Token Expires At**. If Odoo shows **Reauthorization required**, open that connection, select **Authorize with Square**, and sign in as its original merchant in the same environment. An expired authorization link requires a new attempt in the same Odoo administrator session that started it.
- A different merchant, application, environment, or location needs a new connection. Assign it to the provider for future checkout. Keep the original connection and access to its Square merchant for past payments, status checks, and refunds. A payment already processing stays tied to the original connection and location.
- If you no longer have access to the original merchant, ask that merchant's administrator to restore authorization on the original connection. Use **Refresh status** on the existing payment once access is restored. A new connection cannot retrieve operations for the previous merchant.
- If Square denies authorization or the account has no eligible location, review **Last authorization result** in the connection form. In Sandbox, open the test seller dashboard before starting authorization.
- Configure reverse proxies and external request loggers to omit query strings for the OAuth callback path. Odoo redacts that path's query string from its Werkzeug access log. Use normal production log levels to keep RPC inputs and credentials out of diagnostics.

## Make a sandbox payment

1. With **Square Online** in **Test Mode** and published, open the store in a shopper session. Use an HTTPS checkout origin. For testing on the same computer as Odoo, Square also supports the browser's secure localhost context.
2. Put a product in the cart, enter the billing and shipping details, and select a delivery method if Odoo asks for one. Check that the order currency matches the Sandbox location. Select **Card** secured by **Square Online**.
3. Use the [Square Sandbox test cards](https://developer.squareup.com/docs/devtools/sandbox/payments) in the embedded card form. Select **Pay now** and complete the bank verification prompt when the chosen test card requires it.
4. Confirm that the existing Odoo order is confirmed and the charge appears in the Square Sandbox seller's transactions. Check the payment in Odoo Accounting and the order's invoice if automatic invoicing is enabled. Odoo handles delivery through its normal fulfillment workflow.
5. Try a Sandbox decline card to see how checkout handles a definitive failure. If a payment remains processing after an interruption, keep the order reference for support and check its status before trying to pay again.

Square requires a Content Security Policy on the checkout page. Configure the checkout host to allow the selected environment's Square SDK and its frame, network, stylesheet, and font origins. Check that the policy also allows your Odoo assets. [Square Web Payments SDK security policy](https://developer.squareup.com/docs/web-payments/content-security-policy)

The automated checkout tests simulate Square's SDK and API. Use Square Sandbox to qualify your store's actual card form, authentication, and capture.

## Set up Square webhooks

1. Open a saved Square connection in Odoo. Copy its **Webhook Route** and add the public HTTPS origin that routes to this Odoo database. For example, join `https://shop.example.com` with `/payment/square_online/webhook/1`.
2. In the Square Developer Console, select the application's environment and open **Webhooks → Subscriptions**. Create a subscription with that notification URL in the same Sandbox or Production environment as the connection. Choose Square API version `2026-09-16` and select `payment.created`, `payment.updated`, and `oauth.authorization.revoked`.
3. Enter the exact subscription URL in Odoo's **Webhook Notification URL** field and paste the subscription's **Webhook Signature Key**. Save the connection. Odoo checks the signature against that exact URL and the unmodified request body, so preserve the POST body through the reverse proxy. Copy the replacement key to Odoo when rotating the subscription key.
4. Complete a Sandbox checkout and verify both the event delivery in Square and the corresponding transaction in Odoo. Keep the subscriptions for historical connections while their transactions may still need reconciliation. To turn off an endpoint in Odoo, clear its notification URL.

[Square webhook subscriptions](https://developer.squareup.com/docs/webhooks/step2subscribe) · [Square notification signatures](https://developer.squareup.com/docs/webhooks/step3validate)

## Recover and review online payments

A processing payment can complete after the shopper closes the page. Odoo records the original attempt before contacting Square. A signed Square notification can update it, and a scheduled check runs every five minutes. A payment stays processing if Odoo cannot verify its outcome. The shopper can check the payment status in their checkout session or on their own portal order.

Accounting managers and administrators can open **Invoicing → Configuration → Online Payments → Square Payment History** for their companies. Open a transaction to see the order, captured amount, payment status, and latest status check. Use **Refresh status** to look up the existing payment with its original Square connection and location. Refresh and the scheduled check retrieve existing payment data. They can renew OAuth access to perform those reads. If the check requests reauthorization, use **Authorize with Square** on the original connection for the same merchant, then refresh again.

If Square captured the payment after the Odoo order was cancelled or changed, Odoo records the incoming payment and flags the transaction and order for review. Leave fulfillment on hold while reviewing the payment and order. Resolve the amount collected with the shopper and, if money needs to be returned, handle that as a separate refund.

The deterministic recovery tests simulate Square API responses and sign representative notifications with a test key. Use a Square Sandbox account and the public HTTPS endpoint to verify actual checkout and webhook delivery.

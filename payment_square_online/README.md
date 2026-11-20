# Square Online Payments for Odoo 19 Community

## Install

Use a self-hosted Odoo 19 Community database for your Odoo online store. Odoo Online hosting is incompatible with this custom addon. The addon installs Odoo eCommerce and its accounting dependencies. Square POS is optional. [Odoo Online hosting](https://www.odoo.com/documentation/19.0/administration/odoo_online.html)

For an existing self-hosted installation, place `payment_square_online` on the addons path, ensure the Odoo Python environment includes `cryptography` and `requests`, restart Odoo, update the Apps list, and install **Square Online Payments**.

For a fresh local database with Docker Compose, copy `.env.example` to the private `.env` file. Set `DB_PASSWORD`, `DB_NAME=odoo_square_v19`, and `ODOO_PORT=8070`. The loopback port 8070 lets this instance run separately from an Odoo 18 instance on port 8069. Use the same database name in the install command and the environment file.

```bash
docker compose --project-name odoo-square-v19 run --rm odoo \
  odoo --database=odoo_square_v19 --init=payment_square_online --stop-after-init
docker compose --project-name odoo-square-v19 up -d
```

Open [Odoo at localhost port 8070](http://localhost:8070), sign in with the initial `admin` login and password, and change the password. The database and filestore remain in this Compose project's volumes. PostgreSQL is reachable on the project's internal network.

Before saving Square credentials, set `SQUARE_ONLINE_ENCRYPTION_KEY` in the Odoo service environment. With Compose, add the key to the private `.env` file and run `docker compose --project-name odoo-square-v19 up -d odoo` to apply it. Generate a Fernet key with the following command.

```bash
python3 -c 'import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())'
```

Use the same key for all Odoo processes serving the database. Back it up privately alongside your Odoo recovery materials. Restoring an Odoo database requires its original key to read the saved Square credentials.

### Prepare the company and store

1. Set up the chart of accounts for the company that owns the website. Open **Invoicing → Configuration → Journals** and configure that company's bank journal. For sandbox refunds, enable its outgoing **Manual** payment method with an **Outstanding Payments Account**. Check the currency against the [currency rules](#companies-websites-and-currencies).
2. In **Website → Configuration → Settings**, select the intended website. Enable pricelists if needed and assign a pricelist in the Sandbox seller's currency. Publish a test product with a nonzero price. A service product lets you run the first checkout without configuring stock or shipping.
3. Enable **Automatic Invoice** in the website settings if your product's invoicing policy allows invoicing ordered quantities and you want online payment to generate an invoice. With delivered-quantity invoicing, use Odoo's standard delivery and invoicing workflow. [Odoo eCommerce order handling](https://www.odoo.com/documentation/19.0/applications/websites/ecommerce/order_handling.html) · [Odoo invoicing policies](https://www.odoo.com/documentation/19.0/applications/sales/sales/invoicing/invoicing_policy.html)

## Create and authorize your Square application

1. Open the [Square Developer Console](https://developer.squareup.com/apps) and create an application owned by your business. Open its **OAuth** page and choose **Sandbox**.
2. In Odoo, go to **Website → Configuration → eCommerce → Payment Providers → Square Online → Credentials**. Select **Manage Square Connections** and create a connection. Set its company, select **Sandbox**, and enter the corresponding application ID and secret from Square.
3. Copy the connection's **OAuth Redirect URL** into the Square application's **OAuth → Redirect URL** setting. Odoo takes the origin from `web.base.url`, which you can set in developer mode under **Settings → Technical → Parameters → System Parameters**. Use HTTPS for public hosts. For a loopback Sandbox test, set the origin to `http://localhost:8070` and register `http://localhost:8070/payment/square_online/oauth/callback`. Open Odoo on that exact origin before starting authorization. Set `web.base.url.freeze` to `True` if administrators also visit this database on another origin. If you test another Odoo instance, give it a separate Sandbox application so each application can point to the correct callback.
4. Open the Sandbox Square Dashboard for a test seller in a separate tab in the same browser. Return to the connection form and select **Authorize with Square**. Grant access to the intended seller account. Odoo displays the result and verified merchant ID when Square returns.
5. Choose a **Square Location**. The location selector shows active locations from the authorized account with card processing and a supported processing currency. The form shows the selected country's code and currency.

[Square OAuth walkthrough](https://developer.squareup.com/docs/oauth-api/walkthrough)

## Configure the payment provider

1. Return to **Square Online → Credentials** and choose your **Sandbox Connection**. Under **Configuration**, choose a bank **Payment Journal** from the same Odoo company. Follow the currency rules below when selecting the journal.
2. Set the provider to **Test Mode**. Select a **Website** beside the provider state if the provider should only appear on that website. Odoo shows this field when the user has multi-website access. Use the normal Odoo **Published** control to make the provider available to shoppers.

## Companies, websites, and currencies

For another company or a website that needs a different Square location, open an existing **Square Online** provider and choose **Duplicate** from the action menu. Set the duplicate's **Company** and **Website** before assigning its connection and journal. The connection and journal must belong to the same company as the provider. Leave **Website** blank to allow eligible checkouts across that company's websites, or select one website to restrict the provider to it.

Square supports card processing for sellers in Australia (AUD), Canada (CAD), France, Ireland and Spain (EUR), Japan (JPY), the United Kingdom (GBP), and the United States (USD). Buyers can shop internationally using cards accepted by Square and approved by their issuer. Leave the provider's **Countries** field blank to offer Square to eligible buyers regardless of billing country. [Square international development](https://developer.squareup.com/docs/international-development)

Square charges the full checkout total, including tax and shipping, in the location's processing currency. Activate that currency in Odoo and use a website pricelist that uses it. Checkout hides Square when the cart currency differs from the selected location's currency. Customers can use a compatible pricelist or choose another payment method. If the currency matches and Square is still missing, check that the provider is published, active for the correct environment, and available on the shopper’s website and its Odoo company.

A bank journal with a fixed currency requires that currency to match the Square location. Leave the journal currency blank if it will handle multiple active currencies. The Odoo company can keep a different base currency. Odoo records the original payment currency and converts balances into the company's base currency using its configured exchange rates. [Odoo multi-currency accounting](https://www.odoo.com/documentation/19.0/applications/finance/accounting/get_started/multi_currency.html)

## Renew and reconnect Square authorization

Odoo renews the merchant's OAuth access automatically, generally every seven days, and checks again when it needs a Square API token. This works independently for Sandbox and Production, including connections kept for historical payments. A temporary Square outage can delay renewal. Odoo keeps using the current access token while it is valid.

- In **Manage Square Connections**, check **Authorization Status** and **Token Expires At**. If Odoo shows **Reauthorization required**, open that connection, select **Authorize with Square**, and sign in as its original merchant in the same environment. An expired authorization link requires a new attempt in the same Odoo administrator session that started it.
- A different merchant, application, environment, or location needs a new connection. Assign it to the provider for future checkout. Keep the original connection and access to its Square merchant for past payments, status checks, and refunds. A payment already processing stays tied to the original connection and location.
- If you no longer have access to the original merchant, ask that merchant's administrator to restore authorization on the original connection. Use **Refresh status** on the existing payment once access is restored. A new connection cannot retrieve operations for the previous merchant.
- If Square denies authorization or the account has no eligible location, review **Last authorization result** in the connection form. In Sandbox, open the test seller dashboard before starting authorization.
- Configure reverse proxies and external request loggers to omit query strings for the OAuth callback path. Odoo redacts that path's query string from its Werkzeug access log. Use normal production log levels to keep RPC inputs and credentials out of diagnostics.

## Make a sandbox payment

1. With **Square Online** in **Test Mode** and published, open the store in a shopper session. Use an HTTPS checkout origin with the [checkout security policy](#configure-checkout-security). For testing on the same computer as Odoo, use the browser's secure localhost context and apply the policy to that origin too.
2. Put a product in the cart, enter the billing and shipping details, and select a delivery method if Odoo asks for one. Check that the order currency matches the Sandbox location. Select **Card** secured by **Square Online**.
3. Pay on desktop with a documented success test card, then repeat on a phone or mobile emulation with Square's authentication-required test card. Enter Square's documented test verification code when prompted. Use the success, Strong Customer Authentication, and decline cases in Square's test card tables. [Square Sandbox test cards](https://developer.squareup.com/docs/devtools/sandbox/payments)
4. Check the capture in the Sandbox seller's transactions, the confirmed Odoo order, and the posted payment under **Invoicing → Customers → Payments**. Check the invoice when automatic invoicing is enabled. Odoo handles deliveries through its normal fulfillment workflow.
5. Try the Sandbox decline case and confirm the failed attempt leaves the order unpaid. To check interruption recovery, interrupt the shopper's connection after submitting a test payment, then compare the order's Square transaction and Odoo payment history. If Odoo still shows processing, keep the order reference and use **Refresh status** before allowing a new attempt.
6. Use the [native refund workflow](#refund-a-square-online-payment) and confirm the outcome in both systems. A partial refund followed by the remaining amount exercises the refundable balance after each refund.

### Configure checkout security

Set a Content Security Policy (CSP) on the checkout host. Merge Square's environment-specific script, frame, network, stylesheet, and font origins into the policy that serves your Odoo assets. The SDK loads from `https://sandbox.web.squarecdn.com` in Sandbox and `https://web.squarecdn.com` in Production. In a Sandbox checkout on Odoo 19, Chrome also needed `https://api.squareupsandbox.com` in `frame-src` to display the 3-D Secure challenge. [Square Web Payments SDK security policy](https://developer.squareup.com/docs/web-payments/content-security-policy)

Check console violations while loading the card form and completing issuer verification on your own checkout origin. Odoo scripts, inline styles, fonts, and Owl template compilation can have additional requirements depending on your website assets. Allow the specific resources your checkout needs and validate the resulting policy in each deployment environment.

## Set up Square webhooks

1. Open a saved Square connection in Odoo. Copy its **Webhook Route** and add the public HTTPS origin that routes to this Odoo database. For example, join `https://shop.example.com` with `/payment/square_online/webhook/1`.
2. In the Square Developer Console, select the application's environment and open **Webhooks → Subscriptions**. Create a subscription with that notification URL in the same Sandbox or Production environment as the connection. Choose Square API version `2026-09-16` and select `payment.created`, `payment.updated`, `refund.created`, `refund.updated`, and `oauth.authorization.revoked`.
3. Enter the exact subscription URL in Odoo's **Webhook Notification URL** field and paste the subscription's **Webhook Signature Key**. Save the connection. Odoo checks the signature against that exact URL and the unmodified request body, so preserve the POST body through the reverse proxy. Copy the replacement key to Odoo when rotating the subscription key.
4. With the HTTPS route reachable by Square, complete a Sandbox checkout and refund. In the Developer Console's webhook event logs, check for a successful HTTP 2xx delivery to the exact subscribed URL. In Odoo, check the payment and linked refund in **Square Payment History**. A loopback-only checkout can recover status by polling or **Refresh status**. Qualify actual notification delivery separately on the public HTTPS endpoint.
5. Keep the subscriptions for historical connections while their transactions may still need reconciliation. To turn off an endpoint in Odoo, clear its notification URL.

[Square webhook subscriptions](https://developer.squareup.com/docs/webhooks/step2subscribe) · [Square notification signatures](https://developer.squareup.com/docs/webhooks/step3validate)

## Enable production payments

Complete the Sandbox checkout, refund, and HTTPS notification checks before publishing live payments. Configure the production checkout origin and its CSP, then register the exact HTTPS OAuth callback in the application's **Production** settings. Create a separate **Production** connection in Odoo, authorize the production seller, select its location, and register the corresponding Production webhook subscription.

Select the **Production Connection** on the provider. Review the company, website, payment journal, location, and checkout currency. Use **Enabled** and **Published** to offer Square to eligible customers. Use **Disabled** to stop offering it at checkout. Keep the original Sandbox and Production connections for historical recovery and refunds.

## Run alongside Square POS

Install both **Square Online Payments** and **Square POS Integration** in the same Odoo database if you need both checkout channels. Follow this guide for website payments and the [POS configuration guide](../README.md#square-configuration) for POS.

| Activity | Configured in Odoo | Square notification endpoint |
| --- | --- | --- |
| Website checkout, website payment recovery, and refunds for website payments | **Square Online** payment provider and its merchant-authorized OAuth connection, company, website, and bank journal | `/payment/square_online/webhook/<connection_id>` |
| Import Square POS sales, payments, refunds, exchanges, and stock activity | **Square → Configuration**, with its own Square API token, payment journal, and location-to-warehouse mappings | `/square/webhook` |

Create a separate subscription for each addon and save its signature key with the matching credentials and environment. Both addons can share the same merchant and location. The importer’s credentials need permission to retrieve that merchant’s orders and payments. If you also use Square at the Odoo register, follow the [Square Terminal setup guide](../pos_square_terminal/README.md).

The website addon records the payment against the existing Odoo order. Odoo handles website invoicing and fulfillment through its normal workflow. A website refund posts the outgoing payment; create any credit note or goods return separately. POS imports genuine Square sales, refunds, and exchanges with its usual accounting and stock effects.

The importer skips website payments and their related Square return orders during webhook processing and manual resync. If ownership is unclear, Square retries the webhook and manual resync can recover other identified sales. Check the importer’s Square API access if a notification keeps retrying. For a website payment still in doubt, use **Refresh status** in **Square Payment History**.

## Recover and review online payments

A processing payment can complete after the shopper closes the page. Odoo records the original attempt before contacting Square. A signed Square notification can update it, and a scheduled check runs every five minutes. A payment stays processing if Odoo cannot verify its outcome. The shopper can check the payment status in their checkout session or on their own portal order.

Accounting managers and administrators can open **Invoicing → Configuration → Online Payments → Square Payment History** for their companies. Open a transaction to see the order, captured amount, payment status, and latest status check. Use **Refresh status** to look up the existing payment with its original Square connection and location. Refresh and the scheduled check retrieve existing payment data. They can renew OAuth access to perform those reads. If the check requests reauthorization, use **Authorize with Square** on the original connection for the same merchant, then refresh again.

If Square captured the payment after the Odoo order was cancelled or changed, Odoo records the incoming payment and flags the transaction and order for review. Leave fulfillment on hold while reviewing the payment and order. Resolve the amount collected with the shopper and, if money needs to be returned, handle that as a separate refund.

The deterministic recovery tests simulate Square API responses and sign representative notifications with a test key. Use a Square Sandbox account and the public HTTPS endpoint to verify actual checkout and webhook delivery.

## Refund a Square online payment

Accounting managers and administrators can refund captured Square payments in their allowed Odoo companies. Keep the outbound **Manual** payment method available on the original bank journal and configure its outstanding payments account.

1. In **Invoicing → Customers → Payments**, open the posted payment for the website order and select **Refund**. The wizard shows the completed refunds and the remaining amount available after both completed and pending refunds. The refund amount defaults to the available balance. Enter a smaller positive amount for a partial refund, in the original payment currency. JPY amounts must be whole yen.
2. Confirm **Refund**, then open **Square Payment History** and select the linked refund transaction. The history shows the amount, original transaction, requesting user, status, and verified Square refund ID as each becomes available.
3. Odoo reserves the requested amount and checks the original payment's balance in Square before dispatch. If you need to return an additional amount, open a new wizard and check the balance shown there. If an existing refund still shows **Pending** after dispatch, use **Refresh status** on that refund to check its outcome. The reservation stays in place while the outcome is unknown and is released when failure is confirmed. If Odoo requests reauthorization, reconnect the original Square account.
4. Each completed refund creates a posted outbound customer payment in the original journal and currency. Confirm the refund in Square Sandbox and check the related outbound payment in Odoo. Create and reconcile any required credit note separately in Accounting. Process any return of goods separately in Inventory.

Cancelling an order leaves any decision to refund with Accounting.

### Refunds initiated in Square

When you refund a website payment in the Square Dashboard, Odoo links the refund to the original payment and identifies its origin as **Square** in **Square Payment History**. Odoo receives signed notifications and runs a scheduled check for missed notifications. Use **Refresh status** on the original transaction to check its linked refunds sooner, or on a known refund to check its current state.

Pending Square refunds reduce the available balance shown in Odoo. Completed Square refunds post an outbound payment in the original journal and currency. Synchronization retrieves the existing refund from Square. Create any required credit note in Accounting and process any goods return separately in Inventory.

## Run the automated Odoo 19 tests

Use the online, combined, Terminal combined, or concurrent-refund entry in the [repository test instructions](../README.md#testing). The concurrent-refund selection also needs a disposable database with a USD company and bank journal. Use the [Sandbox checkout and refund checks](#make-a-sandbox-payment) to verify your Square configuration.

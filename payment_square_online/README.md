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

1. Return to **Square Online → Credentials** and choose your **Sandbox Connection**. Under **Configuration**, choose a bank **Payment Journal** from the same Odoo company. Follow the currency rules below when selecting the journal.
2. Set the provider to **Test Mode**. Select a **Website** beside the provider state if the provider should only appear on that website. Odoo shows this field when the user has multi-website access. Use the normal Odoo **Published** control to make the provider available to shoppers.
3. When ready to use live payments, configure the application's **Production** OAuth settings separately with an HTTPS URL and production credentials. Create a separate **Production** connection, authorize the production seller, choose its location, and assign it as the provider's **Production Connection**. Review the company, journal, website, and location before switching the provider to **Enabled**. To stop offering Square, set the provider to **Disabled**.

## Companies, websites, and currencies

For another company or a website that needs a different Square location, open an existing **Square Online** provider and choose **Duplicate** from the action menu. Set the duplicate's **Company** and **Website** before assigning its connection and journal. The connection and journal must belong to the same company as the provider. Leave **Website** blank to allow eligible checkouts across that company's websites, or select one website to restrict the provider to it.

Square supports card processing for sellers in Australia (AUD), Canada (CAD), France, Ireland and Spain (EUR), Japan (JPY), the United Kingdom (GBP), and the United States (USD). Buyers can shop internationally using cards accepted by Square and approved by their issuer. Leave the provider's **Countries** field blank to offer Square to eligible buyers regardless of billing country. [Square international development](https://developer.squareup.com/docs/international-development)

Square charges the full checkout total, including tax and shipping, in the location's processing currency. Activate that currency in Odoo and use a website pricelist that uses it. Checkout hides Square when the cart currency differs from the selected location's currency. Customers can use a compatible pricelist or choose another payment method. If the currency matches and Square is still missing, check that the provider is published, active for the correct environment, and available on the shopper’s website and its Odoo company.

A bank journal with a fixed currency requires that currency to match the Square location. Leave the journal currency blank if it will handle multiple active currencies. The Odoo company can keep a different base currency. Odoo records the original payment currency and converts balances into the company's base currency using its configured exchange rates. [Odoo multi-currency accounting](https://www.odoo.com/documentation/19.0/applications/finance/accounting/get_started/multi_currency.html)

## Troubleshoot authorization

- An invalid or expired link requires a new **Authorize with Square** attempt. Complete it in the same Odoo administrator session that started it.
- If Square denies authorization or the account has no eligible location, review the result in the connection form. In Sandbox, open the test seller dashboard before starting the authorization.
- To authorize the same merchant again, open the existing connection and select **Authorize with Square**. Create a new connection for a different merchant, application, environment, or location.
- Configure reverse proxies and external request loggers to omit query strings for the OAuth callback path. Odoo redacts that path's query string from its Werkzeug access log. Use normal production log levels to keep RPC inputs and credentials out of diagnostics.

## Make a sandbox payment

1. With **Square Online** in **Test Mode** and published, open the store in a shopper session. Use an HTTPS checkout origin. For testing on the same computer as Odoo, Square also supports the browser's secure localhost context.
2. Put a product in the cart, enter the billing and shipping details, and select a delivery method if Odoo asks for one. Check that the order currency matches the Sandbox location. Select **Card** secured by **Square Online**.
3. Use the [Square Sandbox test cards](https://developer.squareup.com/docs/devtools/sandbox/payments) in the embedded card form. Select **Pay now** and complete the bank verification prompt when the chosen test card requires it.
4. Confirm that the existing Odoo order is confirmed and the charge appears in the Square Sandbox seller's transactions. Check the payment in Odoo Accounting and the order's invoice if automatic invoicing is enabled. Odoo handles delivery through its normal fulfillment workflow.
5. Try a Sandbox decline card to see how checkout handles a definitive failure. If a payment remains processing after an interruption, keep the order reference for support and check its status before trying to pay again.

Square requires a Content Security Policy on the checkout page. Configure the checkout host to allow the selected environment's Square SDK and its frame, network, stylesheet, and font origins. Check that the policy also allows your Odoo assets. [Square Web Payments SDK security policy](https://developer.squareup.com/docs/web-payments/content-security-policy)

The automated checkout tests simulate Square's SDK and API. Use Square Sandbox to qualify your store's actual card form, authentication, and capture.

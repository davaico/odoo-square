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

## Troubleshoot authorization

- An invalid or expired link requires a new **Authorize with Square** attempt. Complete it in the same Odoo administrator session that started it.
- If Square denies authorization or the account has no eligible location, review the result in the connection form. In Sandbox, open the test seller dashboard before starting the authorization.
- To authorize the same merchant again, open the existing connection and select **Authorize with Square**. Create a new connection for a different merchant, application, environment, or location.
- Configure reverse proxies and external request loggers to omit query strings for the OAuth callback path. Odoo redacts that path's query string from its Werkzeug access log. Use normal production log levels to keep RPC inputs and credentials out of diagnostics.

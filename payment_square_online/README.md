# Square Online Payments for Odoo 18

Install **Square Online Payments** (`payment_square_online`) on an Odoo Community 18 installation you host. Place the add-on in an Odoo addons path, update the Apps list, and install it from Apps. It loads Odoo's Website, eCommerce, and payment accounting dependencies. The Square POS add-on is optional. Odoo Online, the hosted service at odoo.com, does not support installing this custom add-on.

## Prepare Odoo

1. Generate a Fernet key in a private terminal with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Set `ODOO_SQUARE_ONLINE_ENCRYPTION_KEY` to that key in the Odoo server environment and restart Odoo. With this repository's development Compose setup, set it in the private `.env` file. All Odoo workers for this database need the same key. Keep a secure backup separately from database backups.
2. Set `web.base.url` in Odoo's system parameters to the HTTPS origin you use to sign in to Odoo. Set `web.base.url.freeze` to `True` so entering Odoo from another host does not update the registered redirect. A local Sandbox connection can use `http://localhost:8069` if you open Odoo at that same address. Enter the exact scheme, host, and port you use in your browser.
3. In **Website → Configuration → Payment Providers**, open **Square Online Payments** as an Odoo settings administrator. Confirm the owning company and choose **Sandbox** in **Credentials**. To restrict the provider to one website, select that website in the provider configuration. The website must belong to the provider's company.

## Create and authorize your Square application

1. Sign in to the [Square Developer Console](https://developer.squareup.com/apps) with your own account, create an application for your business, and select **Sandbox**. In the application's OAuth page register the exact **OAuth Redirect URL** shown on the Odoo provider's **Credentials** tab. The path is `/payment/square/online/oauth/callback`. Keep callback query strings out of reverse proxy access logs.
2. Copy the **Sandbox Application ID** and the **Sandbox Application Secret** from the Console to Odoo and save. The secret entry clears after saving and **Application Secret Saved** confirms it is stored. Square requests permission to read locations, read and write orders, and read and write payments and refunds. [Square OAuth permissions](https://developer.squareup.com/docs/oauth-api/square-permissions)
3. Open the Square Dashboard for your Sandbox test account in the same browser before authorizing. In Odoo click **Authorize or Reconnect with Square** and approve the request in Square. Odoo returns you to the provider form and shows the result and the connected Square merchant ID. [Square OAuth walkthrough](https://developer.squareup.com/docs/oauth-api/walkthrough)
4. Select the Square location under **Credentials** and the bank journal under **Configuration**. Odoo lists the connected merchant's active locations with card processing in a supported market. The journal must belong to the provider's Odoo company. Keep the website checkout currency aligned with the selected Square location. Supported locations are in Australia (AUD), Canada (CAD), France, Ireland and Spain (EUR), Japan (JPY), the United Kingdom (GBP), and the United States (USD). [Square international development](https://developer.squareup.com/docs/international-development)
5. Set the Odoo provider to **Test Mode** for Sandbox. Use **Disabled** to take the provider out of service. Odoo's **Published** control determines whether website customers can see the provider.

## Connect Production

Duplicate the Odoo provider to create a separate, disabled configuration. Select **Production** and use your Square application's Production OAuth redirect, ID, and secret. Register an HTTPS redirect in the Console and authorize the production merchant. Select the location, website if needed, and bank journal. Set the provider to **Enabled** when it is ready to publish.

If authorization is rejected or expires, return to the provider form and choose **Authorize or Reconnect with Square** using the original merchant account. Replace the application secret in Odoo if you rotate it in Square. If Odoo reports that it cannot decrypt the credentials, restore the original server encryption key. To connect a different merchant, company, application, or environment, configure a separate provider.

## Make a Sandbox checkout payment

1. Open the storefront you configured using HTTPS. For a local Sandbox test on the Odoo host, use `http://localhost:8069` or the port you assigned. If your reverse proxy sets a Content Security Policy, allow the Sandbox Web Payments SDK, its secure card frame, and its network endpoints using [Square's CSP instructions](https://developer.squareup.com/docs/web-payments/content-security-policy). Apply the Production domains when you switch to Production.
2. Add a product to your cart, complete delivery and billing, and open **Payment**. Select **Card** under **Secured by Square Online Payments**. The selected Square location must process the order currency. Card details are entered in Square's embedded form.
3. Enter a [Square Sandbox test card](https://developer.squareup.com/docs/devtools/sandbox/payments) and click **Pay now**. Fill in the postcode if Square displays that field. Complete the simulated verification challenge if Square asks. Use a test card that requires Strong Customer Authentication to exercise the challenge, and a decline test card to verify that checkout allows a safe retry.
4. Compare the total on the Odoo order, the Odoo payment, and the Square Sandbox payment. Confirm that Odoo used the chosen bank journal and its normal invoice settings. Shippable items should still have an open delivery. If the checkout says Square is processing the payment, check the original order before trying again.

## Set up Square webhooks

1. In your saved Odoo payment provider, copy **Webhook Endpoint** from the **Square Webhooks** section of **Credentials**. Use the public HTTPS address that routes to this Odoo database. The URL ends in `/payment/square/online/webhook/<provider-id>`.
2. In the same environment for this application in the Square Developer Console, open **Webhooks → Subscriptions**. Create a subscription with the endpoint as its Notification URL, API version `2026-09-16`, and the `payment.created` and `payment.updated` events.
3. In Odoo, enter the exact registered **Square Notification URL** and paste the subscription's key into **New Square Webhook Signature Key**. Save. The key entry clears and **Webhook Signature Key Saved** confirms it is stored. Preserve the POST body through your reverse proxy. Enter a replacement key in Odoo whenever you rotate it in Square.
4. Complete a Sandbox checkout and check webhook delivery in Square and the transaction in Odoo. Keep the subscription active while payments from this provider may need to be recovered. Clear the Notification URL in Odoo to stop accepting notifications at that endpoint.

[Square webhook subscriptions](https://developer.squareup.com/docs/webhooks/step2subscribe) · [Square notification signatures](https://developer.squareup.com/docs/webhooks/step3validate)

## Recover and review online payments

A customer can close checkout while Square is processing a payment. They can return to their order in the portal to see the outcome. Odoo uses signed Square notifications and checks unresolved payments in the background. If Square has yet to confirm an outcome, the original payment stays processing and the order is protected against an accidental second charge.

Accounting managers and settings administrators can open **Invoicing → Configuration → Online Payments → Square Payment History**. The transaction shows the payment status, original Square location, and any status check message. Click **Refresh status** to look up that payment at Square. A manual or scheduled status check does not issue a charge. If the original connection needs attention, use **Authorize or Reconnect with Square** on that provider and select the original merchant account, then refresh the transaction.

If Square collected payment for a cancelled Odoo order, Odoo records the incoming payment and shows a warning on the transaction and the customer's order. Review the payment and order. If the money needs to be returned, handle it as a separate refund.

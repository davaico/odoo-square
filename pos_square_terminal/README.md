# Square Terminal setup for Odoo 18

Install **Square Terminal for Point of Sale** (`pos_square_terminal`) in Odoo 18 with the addon directory on your Odoo addons path. It depends on Point of Sale and uses the Python packages `cryptography` and `requests`. A Point of Sale administrator configures the connection and devices.

## Connect your Square application

1. Generate a key with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Save it in your secret store and set `ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY` in the Odoo server environment. Use the same key for all Odoo workers and keep a backup so a restored database can still read its Square credentials.
2. Under **Point of Sale → Configuration → Square Connections**, create a connection in the company that runs the register. Select **Production** for physical hardware or **Sandbox** for simulated checkouts.
3. In your [Square Developer Console](https://developer.squareup.com/apps), create or select an application owned by your merchant. In the matching environment's OAuth settings, register the **OAuth Redirect URL** shown on the Odoo connection. Set Odoo's `web.base.url` to the browser-visible HTTPS address if the displayed URL differs. Sandbox can use HTTP on `localhost`.
4. Enter the matching **Application ID** and **New Application Secret** in Odoo and save. Select **Authorize or Reconnect with Square** and sign in to the intended Square merchant account. On return, confirm the merchant ID and the intended location in the list. The location must be active, enabled for card processing and use the register's currency. [Supported Square regions](https://developer.squareup.com/docs/in-person-payment-options)

## Pair the terminal and assign it to a register

1. Under **Point of Sale → Configuration → Square Terminals**, create a record with a recognizable name such as **Counter**. Select the authorized connection and the Square location for that terminal.
2. On a physical Square Terminal, connect to the internet and open the device sign-in screen. Sign out of any existing session if needed. In Odoo, select **Generate Pairing Code** and enter the code on the device before the displayed expiry. Use the code generated in Odoo to enter Terminal API connected mode. [Square pairing instructions](https://developer.squareup.com/docs/terminal-api/integrate-square-terminal)
3. Select **Check Readiness** in Odoo. The device must report **Ready** at the selected location. If it needs an update, install Square Terminal software 6.62 or later for linked orders. If Square reports the device is unavailable, confirm its connection and check again. Square's device status can take time to update. Terminal API payments require internet connectivity. [Device status](https://developer.squareup.com/docs/terminal-api/terminal-device-monitoring) · [Linked orders](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders)
4. Create a POS payment method such as **Square Counter**. Select an active bank journal with the appropriate outstanding account, leave **Identify Customer** off, choose **Terminal** for **Integration**, and choose **Square Terminal** under **Integrate with**. Assign the paired device and add the payment method to the register. Confirm **Square Readiness** before opening the session. Other configured payment methods remain available if this terminal is incompatible.

For Sandbox, select **Use Sandbox Simulator** instead of generating a code. The supplied Square test device simulates a successful card payment within Square's amount limits. It needs a separate Sandbox authorization and location. [Sandbox device behavior](https://developer.squareup.com/docs/devtools/sandbox/testing#terminal-api-checkouts)

For a replacement device, add a new terminal record and switch the payment method after closing the register session. Create a new connection if the company, application or environment changes, and keep the original connection available for historical payment checks. Reauthorize that original connection with the same merchant if Square revokes its grant.

## Take a single card payment

1. Add the items in Odoo and open the POS payment screen. Select the named Square payment method for the full balance, then select **Send**.
2. Ask the customer to pay on the selected Square Terminal. Keep the original sale open while Odoo verifies the payment. If Square's result is uncertain, keep the original sale available for investigation before taking another payment.
3. Issue the itemized Odoo receipt when the sale completes. If the register requires manual validation, select **Validate** after the payment shows as successful.

Odoo uses the configured basket prices and sends the selected card amount automatically. For two Square cards, enter the first card’s share before selecting **Send**. After Square authorizes it, select Square again for the remaining balance. Cash-only sales use the existing cash method.

## Enable tips on the terminal

In **Point of Sale → Configuration → Settings**, select the register and enable **Square Terminal Tips**. Under **Tips**, select an active, tax-free service product for the receipt. Choose this product for each register that prompts for tips. For a split card payment, each cardholder can tip on their own share. The Odoo receipt shows one line for the combined tip and each card payment shows what that cardholder paid.

## Recover and review payments

In the [Square Developer Console](https://developer.squareup.com/apps), add a webhook subscription in the connection's environment. Copy **Webhook Endpoint** from the saved Odoo connection. Register this HTTPS URL for `terminal.checkout.created`, `terminal.checkout.updated`, `payment.created`, `payment.updated`, `refund.created`, and `refund.updated`. Copy the exact registered URL and the subscription's signature key into **Square Notifications** on that Odoo connection. Keep the original subscription and authorization available when you replace or archive a connection. [Square webhook setup](https://developer.squareup.com/docs/webhooks/step2subscribe)

Odoo continues checking unresolved payments and looking for Square refunds after the register browser closes. In **Point of Sale → Orders → Square Terminal Payments**, open an attempt and choose **Check Square Payment** to read its current status from Square. You can also find the attempts under **Square Payments** on the original POS order. Use the last successful check, verified amounts, and Square references when investigating discrepancies. If authorization has expired or been revoked, open and reauthorize the original connection listed on the attempt.

The original payment shows each confirmed Square refund amount and reference. Review those entries to determine the accounting correction or goods return to handle in Odoo.

## Finish and close the register

Keep the original order when Square has verified payment but Odoo needs more information. If an invoice needs a customer, reopen the order and select the customer on the payment screen. Odoo completes checkout when its usual requirements are met and automatic terminal validation is enabled.

When a Square payment is unresolved or its verified sale is still in draft, Odoo identifies the order, amount, and latest known Square status when you try to close the register. Review the original tender or finish the original sale before closing.

For walk-in checkout, completing the sale at the Odoo counter records the handover time. Odoo sends the timestamp as a custom attribute on the original Square order. Managers can see pending handovers in **Square Terminal Payments**. Odoo retries on the original Square connection after session closure. [Square order custom attributes](https://developer.squareup.com/docs/orders-custom-attributes-api/custom-attributes)

Use Square Transactions to review the payment. Use the linked Square order and payment IDs in Odoo for reconciliation. Availability in Square Dashboard Orders depends on Square's order visibility rules. [Square order visibility](https://developer.squareup.com/docs/orders-api/create-orders#view-orders-in-the-square-dashboard-or-square-products)

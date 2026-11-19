# Square Terminal for Odoo 18 Point of Sale

Use this addon to take an in-person card payment on a Square Terminal for a sale started in Odoo. Odoo keeps the itemized receipt. Square receives a linked order and the verified tenders.

## Connect your Square application

1. Put the `pos_square_terminal` addon on the Odoo 18 addons path and install **Square Terminal for Point of Sale** as a Point of Sale administrator. It requires **Point of Sale** and the Python packages `cryptography` and `requests`. The Square sales importer and website payment addon are optional. If **Square POS Integration** (`odoo_square`) is already installed, upgrade it from the same release to load its stock-count schema changes.
2. Generate a Fernet key with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Set `ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY` in the Odoo server environment. Use the same key on all workers and keep it in your secret store so a database restore can still read the encrypted credentials.
3. Under **Point of Sale → Configuration → Square Connections**, create a connection for the register's company. Select **Production** for physical hardware or **Sandbox** for simulated checkouts.
4. In the merchant's [Square Developer Console](https://developer.squareup.com/apps), select or create the merchant-owned application. Register the **OAuth Redirect URL** displayed on the Odoo connection in the matching Square environment. Use the browser-visible HTTPS address as Odoo's `web.base.url`. Sandbox can use HTTP on localhost.
5. Enter the **Application ID** and **New Application Secret** for that environment. Save and choose **Authorize or Reconnect with Square**. Sign in to the intended merchant account. Back in Odoo, verify the **Merchant ID** and the intended **Square Location**. The location must be active, support card processing, and use the same currency as the Odoo register and bank journal. Use **Refresh Locations** after changing a location in Square. [Square's supported regions](https://developer.squareup.com/docs/in-person-payment-options)
6. In the same Square environment, create a webhook subscription using the saved connection's **Webhook Endpoint**. Subscribe to `terminal.checkout.created`, `terminal.checkout.updated`, `payment.created`, `payment.updated`, `refund.created`, and `refund.updated`. In **Square Notifications** on the Odoo connection, save the exact HTTPS URL and the subscription's signature key. Verify **Webhook Key Saved**. [Square webhook setup](https://developer.squareup.com/docs/webhooks/step2subscribe)

## Pair and select a terminal

1. Open **Point of Sale → Configuration → Square Terminals** and create a record named for the counter, such as **Counter**. Select the authorized connection and its Square location.
2. Connect the Square Terminal to the internet and open the device sign-in screen. If a change of pairing is needed, first let any pending offline payments upload in Square. Select **Generate Pairing Code** in Odoo, then sign in on the device using that code before the displayed expiry. [Pairing guide](https://developer.squareup.com/docs/terminal-api/integrate-square-terminal) · [Checking offline payments](https://squareup.com/help/fr/fr/article/8551-view-offline-payments)
3. Choose **Check Readiness** and confirm **Ready**, the device ID, and the application version. Linked orders need Square Terminal app version 6.62 or later. If the device is unavailable, check its internet connection and location, then check readiness again after the device status updates. [Device monitoring](https://developer.squareup.com/docs/terminal-api/terminal-device-monitoring) · [Linked-order requirements](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders)
4. Under **Point of Sale → Configuration → Payment Methods**, create a name the cashier will recognize, such as **Square Counter**. Select an active bank journal with the appropriate outstanding account, leave **Identify Customer** off, set **Integration** to **Terminal**, and choose **Square Terminal** under **Integrate with**. Select the paired device and add the method to the register's **Payment Methods**. Confirm **Ready for Square Terminal** before opening the session. If **Square Readiness** shows a problem, the cashier can still use other configured methods.

For Sandbox, use a separate Sandbox authorization and location, then select **Use Sandbox Simulator**. Square uses test device IDs and simulated responses for this setup. [Square's Sandbox Terminal test values and limits](https://developer.squareup.com/docs/devtools/sandbox/testing#terminal-api-checkouts)

To replace a device, close the register session, create a new terminal record and update the payment method. Create a new connection when changing the company, merchant application or environment. Retain access to the original connection and webhook subscription for historical payments.

## Take and finish a counter sale

1. Ring up the basket and open **Payment**. For a walk-in sale, leave **Customer** blank and **Invoice** off. Select the named Square method, confirm or enter the card share on its payment line, then choose **Send**. Verify the amount shown on the selected terminal before the customer presents their card.
2. Wait for Odoo to verify the result on the original sale. When the full payment is verified, Odoo can complete the sale automatically. If the register requires manual validation, choose **Validate**. If Odoo needs a customer to invoice, select the customer on that same order and finish checkout with the existing verified payment.
3. Issue the Odoo receipt. Square uses a matching catalog variation or a named item when possible. For a basket such as a complex combo, Square can show a receipt summary linked to the itemized Odoo receipt. Odoo blocks dispatch on a pricing or currency mismatch.

For a split, enter exact shares using Odoo cash and one or more Square cards. For example, record €20 cash on a €60 sale, then select **Square Counter**, check the €40 balance and choose **Send**. A card share can show **Authorized by Square** while the original sale awaits the remaining shares. If the last share is cash, enter it and select **Validate**. Odoo settles all covered shares on the same Square order. For a cash-only sale, choose the Odoo cash method from the start. If Odoo flags an unsupported tender combination, correct it before dispatch.

### Tips

- **On the terminal.** In **Point of Sale → Configuration → Settings**, select the register, enable **Square Terminal Tips**, and select an active, tax-free service product as the **Tip Product** under **Tips**. Leave the Odoo tip empty for that sale. Each cardholder can tip on their own share. Odoo includes each verified tip in its card total and prints one receipt tip for the combined amount.
- **On the register.** Enable **Tips** for the register and select the same kind of **Tip Product**. On the payment screen, choose **Tip**, enter the amount in **Add Tip**, and confirm it before entering the tender shares or sending a card request. Enter each share including its share of the tip. For €60 of goods and a €6 tip, enter €22 cash and send €44 by card. Odoo allocates €2 of tip to cash and €4 to card, puts any rounding remainder on the final tender, and prints one €6 tip. The Terminal takes €44 and skips its tip prompt.

### Recover a payment on the original sale

| What the cashier sees | What to do |
| --- | --- |
| Waiting, timed out, uncertain, or a lost response | Keep the original order. After a reload, find it under **Orders**, reopen **Payment**, and select **Check status** on the original line. Keep that share reserved until Square confirms the outcome. |
| Device busy and Odoo says no payment was sent | Remove the unsent line to choose another ready method, or use **Send** again when the terminal is free. |
| Card declined while the original checkout is still active | Follow the original Terminal prompt or request cancellation in Odoo. Use **Retry** when Odoo confirms that Square collected nothing and offers that action. |
| A card is authorized and another card declines | Keep the authorized share and recover or replace only the unpaid share. |
| The customer changes their mind | Use **Cancel payment** or **Abandon split** and wait for confirmation. Use **Continue cancellation** if offered. Once Square verifies cancellation and release, correct the basket or switch to cash. If a card succeeded during cancellation, Odoo keeps that payment and shows the actual balance. |

## Reconcile payments and handover

- Open **Point of Sale → Orders → Square Terminal Payments**, or choose **Square Payments** on the original POS order. Look for the requested amount, verified base and tip, original connection, Square order, checkout and payment IDs, and **Last Successful Check**. **Check Square Payment** refreshes the original payment. If Square revoked access, reauthorize the original connection with the same merchant. Scheduled checks and verified webhooks continue after the register closes.
- Unresolved payments and verified sales still in draft block session closure. Use the order, amount, and last Square status in the close message to finish the original sale. Other sales can continue.
- Completing native Odoo checkout records the counter handover time. Under **Counter Handover**, **Recorded in Square** means Square confirmed the timestamp on the original order as an order custom attribute. **Pending Square handover** permits receipt issuance and session closure for the completed sale. Odoo retries the update on the original connection. [Square order custom attributes](https://developer.squareup.com/docs/orders-custom-attributes-api/custom-attributes)
- Refunds initiated in Square appear under **Confirmed Square Refunds** on the original attempt. Use the refund amount and reference to decide which accounting correction or return to record in Odoo.
- When the optional Square sales importer is installed, genuine Square POS checkouts still import through that addon. The linked Square order and its refunds for a sale started in Odoo stay with the original POS order. For shared inventory, use the importer's location-to-warehouse mapping and SKU configuration. [Square importer configuration](../README.md#square-configuration)

### Where to look in Square

In France, start in the Square Dashboard under **Commandes et paiements → Transactions**. Some accounts show **Factures et paiements** or **Paiements** as the parent menu. Filter by date and location, then open the matching transaction. Use the amount and original Square payment ID in Odoo to reconcile it. [Square France transaction search](https://squareup.com/help/fr/fr/article/5145-transaction-search)

Square documents that Dashboard Orders displays paid orders with fulfillment and that in-store fulfillment is a closed beta. An anonymous walk-in sale in Odoo uses a linked Square order without a pickup booking. It can be reconciled with the original order and payment IDs even if it is absent from Dashboard Orders. [Square order visibility](https://developer.squareup.com/docs/orders-api/create-orders#view-orders-in-the-square-dashboard-or-square-products) · [Fulfillment limitations](https://developer.squareup.com/docs/orders-api/fulfillments#requirements-and-limitations)

Square lists linked orders and itemization for the EU. Its delayed-capture and pre-entered-tip release notes name Australia, Canada, Japan and the UK, and the linked-order guide describes splits in regions with order IDs. Confirm split and tip behavior for your own location and terminal before offering those workflows. [Square hardware and region matrix](https://developer.squareup.com/docs/in-person-payment-options) · [Linked-order splits](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders#optional-take-a-split-tender-payment) · [Checkout feature release notes](https://developer.squareup.com/docs/changelog/connect-logs/2025-04-16)

## Qualify the French terminal on hardware

After automated acceptance, use the merchant's existing physical Square Terminal in France for the agreed anonymous €0.01 purchases. The automated suite exercises the €60 sale with €20 cash and €40 card, along with simulated recovery and handover. Hardware qualification requires observations from the actual Production account.

1. Follow the connection, pairing and payment method steps above. If the approved production device is already paired, check readiness on that record and record that the previous pairing was reused. Record the Odoo revision, Square API version, Terminal app version, France/EUR location, selected method and readiness result.
2. Use a POS-enabled service item priced at €0.01 in the register's active pricelist, with no tax, discount, or inventory tracking. Disable all-tender rounding and device tipping for the test register. Confirm the basket total is €0.01, with **Customer** blank and **Invoice** off. Enter one Square card share for €0.01.
3. Choose **Send** once. Confirm the correct device shows €0.01 before presenting the merchant-approved card. If the terminal offers receipt delivery, choose a paper receipt or skip delivery to keep the purchase anonymous. If the outcome is unclear, use **Check status** on the original order and reconcile it before attempting any further purchase. Record any Square refusal of the amount as observed.
4. Compare the original Odoo receipt, the verified Odoo payment, and the linked Square order and payment. Confirm one completed €0.01 EUR card payment per test sale, with zero tip. Record the receipt reference, Square order and payment IDs, and charged amount.
5. Record whether the payment appeared in **Transactions** and whether the sale appeared in **Commandes**, using the same date and location filters. In Odoo, record the counter handover time and whether **Recorded in Square** appears after the scheduled handover check. If pending, record the latest error and the outcome of the retry. Read back the original order's `odoo-pos-in-store-handover` custom attribute when provider-side evidence is available. Keep evidence free of credentials, pairing codes, customer data and card details.

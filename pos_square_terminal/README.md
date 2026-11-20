# Square Terminal for Odoo 19 Point of Sale

Use this addon to take an in-person card payment on a Square Terminal for a sale started in Odoo. Odoo keeps the itemized receipt. Square receives a linked order and the verified tenders.

## Connect your Square application

1. Put `pos_square_terminal` on the Odoo 19 Community addons path. Update the Apps list, clear the **Apps** filter, and install **Square Terminal for Point of Sale**. The Odoo Python environment needs `cryptography` and `requests`. If the Square sales importer is installed, [upgrade it before resuming stock sync](../README.md#operational-notes).
2. Generate a Fernet key with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Set `ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY` on all Odoo workers and restart them. With the supplied Compose stack, set it in the private `.env` file and recreate the Odoo service. Keep the key in your secret store for database restores.
3. Under **Point of Sale → Configuration → Square Connections**, create a connection for the register's company. Select **Production** for physical hardware or **Sandbox** for simulated checkouts.
4. Open the merchant’s **Square Developer Console** and select or create the Terminal application. If you also use **Square Online Payments**, use a separate application for Terminal so each addon keeps its own OAuth redirect URL. Set Odoo’s `web.base.url` to its public HTTPS address and open Odoo through that address. Sandbox OAuth also accepts HTTP on localhost. Register the **OAuth Redirect URL** displayed on the Odoo connection in the matching Square environment. [Developer Console](https://developer.squareup.com/apps) · [Square OAuth setup](https://developer.squareup.com/docs/oauth-api/walkthrough)
5. Enter the **Application ID** and **New Application Secret** for that environment and save. In Sandbox, open the test seller’s Dashboard in another tab of the same browser before choosing **Authorize or Reconnect with Square** in Odoo. In Production, choose that action and sign in as the intended merchant. Back in Odoo, verify the **Merchant ID** and find the intended location under **Square Locations**. The location must be active, support card processing, and use the register and bank journal currency. Use **Refresh Locations** after a location changes in Square. [Square's supported regions](https://developer.squareup.com/docs/in-person-payment-options)
6. For Square to deliver notifications, the saved connection’s **Webhook Endpoint** must be reachable from Square over HTTPS. Create a subscription in the same environment for `terminal.checkout.created`, `terminal.checkout.updated`, `payment.created`, `payment.updated`, `refund.created`, and `refund.updated`. In **Square Notifications** on the Odoo connection, save the exact subscribed HTTPS URL and its signature key. Verify **Webhook Key Saved**. [Square webhook setup](https://developer.squareup.com/docs/webhooks/step2subscribe)

## Pair and select a terminal

1. Open **Point of Sale → Configuration → Square Terminals** and create a record named for the counter, such as **Counter**. Select the authorized connection and its Square location.
2. Connect the Square Terminal to the internet and open the device sign-in screen. If a change of pairing is needed, first let any pending offline payments upload in Square. Select **Generate Pairing Code** in Odoo, then sign in on the device using that code before the displayed expiry. [Pairing guide](https://developer.squareup.com/docs/terminal-api/integrate-square-terminal) · [Checking offline payments](https://squareup.com/help/fr/fr/article/8551-view-offline-payments)
3. Choose **Check Readiness** and confirm **Ready**, the device ID, and the application version. Linked orders need Square Terminal app version 6.62 or later. If the device is unavailable, check its internet connection and location, then check readiness again after the device status updates. [Device monitoring](https://developer.squareup.com/docs/terminal-api/terminal-device-monitoring) · [Linked-order requirements](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders)
4. Under **Point of Sale → Configuration → Payment Methods**, create a name the cashier will recognize, such as **Square Counter**. Select an active bank journal with the appropriate outstanding account, leave **Identify Customer** off, set **Integration** to **Terminal**, and choose **Square Terminal** under **Integrate with**. Select the paired device and add the method to the register's **Payment Methods**. Confirm **Ready for Square Terminal** before opening the session. If **Square Readiness** shows a problem, the cashier can still use other configured methods.

For Sandbox, use a separate Sandbox authorization and location, then select **Use Sandbox Simulator**. Square uses test device IDs and simulated responses for this setup. [Square's Sandbox Terminal test values and limits](https://developer.squareup.com/docs/devtools/sandbox/testing#terminal-api-checkouts)

To replace a device, close the register session, create a new terminal record and update the payment method. Create a new connection when changing the company, merchant application or environment. Retain access to the original connection and webhook subscription for historical payments.

## Take and finish a counter sale

1. Ring up the basket and open **Payment**. For a walk-in sale, leave **Customer** blank and **Invoice** off. Select the named Square method, confirm or enter the card share on its payment line, then choose **Send**. Verify the amount shown on the selected terminal before the customer presents their card.
2. Wait for Odoo to verify the payment on the original sale. With automatic validation enabled, Odoo can complete a fully paid sale while the register is disconnected when all checkout requirements are met. Reopen the original sale in **Orders** to print the receipt or finish any remaining checkout steps. Choose **Validate** on a manual register or select a customer if Odoo needs one for an invoice.
3. Issue the Odoo receipt. Square uses a catalog variation when its SKU, selling unit, and quantity precision match. Otherwise Odoo sends a named item with the Odoo selling unit, quantity, and price when Square can represent them. For a basket such as a complex combo, Square can show a receipt summary linked to the itemized Odoo receipt. Odoo blocks dispatch on a pricing or currency mismatch.

If the register uses Odoo Global Discounts, Square receives the receipt summary with the verified discounts and taxes. With **Restrict Price Modifications to Managers** enabled, use an Odoo Point of Sale manager login to send a manually priced basket. A manager selected through the employee switcher also needs an authenticated manager login for Square Terminal checkout.

Use Square Terminal for counter sales with exact tenders. A change amount, a shipping date, or a register configured to round all payment methods blocks **Send**. If the register uses cash rounding, limit it to cash payments.

Configured options, custom text, and items with valid lot or serial numbers can use a receipt summary. Custom text and lot or serial details stay in Odoo. Entering custom text requires Odoo’s **Products → Create** access right. For a tracked basket, enter one lot per line or one unique serial number per unit.

Combo selections can include paid extras within the group’s configured limits. Odoo’s combo form puts product options on the components. Square Terminal blocks priced dynamic component variants because Odoo 19 can apply a different surcharge after repricing the same saved selection.

For a split, enter exact shares using Odoo cash and one or more Square cards. For example, record €20 cash on a €60 sale, then select **Square Counter**, check the €40 balance and choose **Send**. A card share can show **Authorized by Square** while the original sale awaits the remaining shares. If the last share is cash, collect it and select **Validate**. That starts settlement of the split on the original Square order. Odoo can resume settlement after a disconnect once it has saved the request. For a cash-only sale, choose the Odoo cash method from the start. If Odoo flags an unsupported tender combination, correct it before dispatch.

### Tips

- **On the terminal.** In **Point of Sale → Configuration → Settings**, select the register, enable **Square Terminal Tips**, and select an active, tax-free service product as the **Tip Product** under **Tips**. Leave the Odoo tip empty for that sale. Each cardholder can tip on their own share. Odoo includes each verified tip in its card total and prints one receipt tip for the combined amount.
- **On the register.** Enable **Tips** for the register and select the same kind of **Tip Product**. Mark a custom tip product used only for Odoo tips as **Available in POS**. On the payment screen, choose **Tip**, enter the amount in **Add Tip**, and confirm it before entering the tender shares or sending a card request. Enter each share including its share of the tip. For €60 of goods and a €6 tip, enter €22 cash and send €44 by card. Odoo allocates €2 of tip to cash and €4 to card, puts any rounding remainder on the final tender, and prints one €6 tip. The Terminal takes €44 and skips its tip prompt.

### Recover a payment on the original sale

| What the cashier sees | What to do |
| --- | --- |
| Waiting, timed out, uncertain, or a lost response | After a reload, find the original order under **Orders**, reopen **Payment**, and select **Check status** on the original line. If it remains unresolved, ask a manager to verify the original payment or confirm cancellation before collecting the share again. |
| Device busy or draft save failed before sending | Keep the original sale and use **Send** once the terminal is free or the register reconnects. You can remove the unsent line to choose another ready method. |
| Ready to send after reconnecting | Choose **Send** on the original line to use the existing reservation, or cancel the unsent payment. |
| Card declined while the original checkout is still active | Follow the original Terminal prompt or request cancellation in Odoo. Use **Retry** when Odoo confirms that Square collected nothing and offers that action. |
| A card is authorized and another card declines | Keep the authorized share and recover or replace only the unpaid share. |
| The customer changes their mind | Use **Cancel payment** or **Abandon split** and wait for confirmation. Use **Continue cancellation** if offered. Once Square verifies cancellation and release, correct the basket, switch to cash, or discard the sale from the till. Odoo keeps the cancelled sale’s Square references. If a card succeeded during cancellation, Odoo keeps that payment and shows the actual balance. |

Square deletes completed checkouts after 30 days. For older payments, use the Square payment ID or the original order’s tender references. [Square Terminal checkout retention](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders#limitations)

## Reconcile payments and handover

- Open **Point of Sale → Orders → Square Terminal Payments**, or choose **Square Payments** on the original POS order. Look for the requested amount, verified base and tip, original connection, Square order, checkout and payment IDs, and **Last Successful Check**. **Check Square Payment** refreshes the original payment. If Square revoked access, reauthorize the original connection with the same merchant. Scheduled checks and verified webhooks continue after the register closes.
- Odoo keeps processed notification IDs on their original connection for audit and replay protection. After seven days it clears the provider resource details from those receipts. A signed replay of a retained ID is acknowledged without another payment check. [Square notification retries](https://developer.squareup.com/docs/webhooks/overview#notification-retries)
- Unresolved payments and verified sales still in draft block session closure. Use the order, amount, and last Square status in the close message to finish the original sale. Other sales can continue.
- Completing native Odoo checkout records the counter handover time. Under **Counter Handover**, **Recorded in Square** means Square confirmed the timestamp on the original order as an order custom attribute. **Pending Square handover** permits receipt issuance and session closure for the completed sale. Odoo retries the update on the original connection. Each replacement application can record handover on its own orders under its own Square attribute definition. [Square order custom attributes](https://developer.squareup.com/docs/orders-custom-attributes-api/custom-attributes)
- Refunds initiated in Square appear under **Confirmed Square Refunds** on the original attempt. Use the refund amount and reference to decide which accounting correction or return to record in Odoo.
- When the optional Square sales importer is installed, genuine Square POS checkouts still import through that addon. The linked Square order and its refunds for a sale started in Odoo stay with the original POS order. For shared inventory, map the Square location to the register’s warehouse and align SKUs and selling units across both catalogs. [Square importer configuration](../README.md#square-configuration)

### Where to look in Square

In France, start in the Square Dashboard under **Commandes et paiements → Transactions**. Some accounts show **Factures et paiements** or **Paiements** as the parent menu. Filter by date and location, then open the matching transaction. Use the amount and original Square payment ID in Odoo to reconcile it. [Square France transaction search](https://squareup.com/help/fr/fr/article/5145-transaction-search)

Square documents that Dashboard Orders displays paid orders with fulfillment and that in-store fulfillment is a closed beta. An anonymous walk-in sale in Odoo uses a linked Square order without a pickup booking. It can be reconciled with the original order and payment IDs even if it is absent from Dashboard Orders. [Square order visibility](https://developer.squareup.com/docs/orders-api/create-orders#view-orders-in-the-square-dashboard-or-square-products) · [Fulfillment limitations](https://developer.squareup.com/docs/orders-api/fulfillments#requirements-and-limitations)

Square lists linked orders and itemization for the EU. Its delayed-capture and pre-entered-tip release notes name Australia, Canada, Japan and the UK, and the linked-order guide describes splits in regions with order IDs. Confirm split and tip behavior for your own location and terminal before offering those workflows. [Square hardware and region matrix](https://developer.squareup.com/docs/in-person-payment-options) · [Linked-order splits](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders#optional-take-a-split-tender-payment) · [Checkout feature release notes](https://developer.squareup.com/docs/changelog/connect-logs/2025-04-16)

## Qualify the French terminal on hardware

For Odoo 19 release acceptance, use the merchant-approved Production device and card for the agreed anonymous €0.01 EUR purchase. Record hardware results separately from the automated €60 mixed-sale test.

1. Record the Odoo revision, Square API and Terminal app versions, France/EUR location, payment method, and readiness. Reuse an existing ready pairing on the approved terminal.
2. Use one tax-free €0.01 POS service item. Turn off all-tender rounding and device tipping. Leave **Customer** blank and **Invoice** off. Send once and check the amount on the device before presenting the card. Choose a paper receipt or skip delivery. If the result is unclear, check the original payment before another purchase. Record any refusal of the amount.
3. Match the Odoo receipt and original Square order and payment IDs to one €0.01 EUR payment with zero tip. Check **Transactions** and **Commandes** with the same date and location filters and record what the account shows.
4. Record the Odoo handover time and **Recorded in Square** status, or the pending error and retry outcome. Where available, compare the handover time with the original Square order’s `odoo-pos-in-store-handover` attribute. [Private reporting and redaction guidance](../SECURITY.md#sensitive-data)

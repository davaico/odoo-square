# Square Terminal for Odoo 18 Point of Sale

Odoo POS keeps the sale and itemized receipt. A named payment method sends the cashier’s card share to a paired Square Terminal.

## Connect your Square application

1. Put `pos_square_terminal` on the Odoo 18 addons path, update the Apps list, clear the **Apps** filter and install **Square Terminal for Point of Sale**. The server needs the Python packages `cryptography` and `requests`. If the Square sales importer is already installed, [upgrade it before resuming stock sync](../README.md#operational-notes).
2. Generate a Fernet key privately with `python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Set `ODOO_SQUARE_TERMINAL_ENCRYPTION_KEY` for all Odoo workers and restart them. For the supplied Compose stack, set it in your private `.env` and recreate the Odoo service. Back up the key separately from the database.
3. As a POS administrator, open **Point of Sale → Configuration → Square Connections**. Create a connection for the register’s company. Choose **Production** for physical hardware or **Sandbox** for the simulator.
4. Set Odoo’s `web.base.url` to its browser-visible HTTPS origin (HTTP localhost works in Sandbox). In the merchant’s Square application, register the connection’s exact **OAuth Redirect URL** in the matching environment. [Square Developer Console](https://developer.squareup.com/apps)
5. In Odoo, save the **Application ID** and **New Application Secret**, choose **Authorize or Reconnect with Square**, and sign in as the intended merchant. Confirm the **Merchant ID** and an active location with card processing. Its currency must match the register and bank journal. Use **Refresh Locations** after changes in Square.
6. In the same Square application and environment, subscribe the connection’s public HTTPS **Webhook Endpoint** to `terminal.checkout.created`, `terminal.checkout.updated`, `payment.created`, `payment.updated`, `refund.created`, and `refund.updated`. Under **Square Notifications** in Odoo, save the identical URL and subscription signature key. Confirm **Webhook Key Saved**. [Square webhook setup](https://developer.squareup.com/docs/webhooks/step2subscribe)

If you installed an earlier PR #38 candidate, stop the Odoo workers, deploy this revision and run `odoo -d <database> -u pos_square_terminal --stop-after-init` before starting the workers again. The update adds storage for Square payment evidence used during recovery. [Compose module update example](../README.md#development)

## Pair and select a terminal

1. In **Point of Sale → Configuration → Square Terminals**, create a terminal for the counter. Select its connection and Square location.
2. Pair the terminal for its environment.
   - **Production.** Before changing an existing pairing, let pending offline payments upload. Connect the terminal to the internet. Choose **Generate Pairing Code** in Odoo and enter it on the device’s sign-in screen before expiry. Choose **Check Readiness** in Odoo. Confirm **Ready**, the expected device ID and Terminal app version 6.62 or later. [Pairing guide](https://developer.squareup.com/docs/terminal-api/integrate-square-terminal) · [Offline payments](https://squareup.com/help/fr/fr/article/8551-view-offline-payments) · [Linked-order requirements](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders)
   - **Sandbox.** On a Sandbox connection, choose **Use Sandbox Simulator** for a simulated device. [Sandbox test values and limits](https://developer.squareup.com/docs/devtools/sandbox/testing#terminal-api-checkouts)
3. Under **Point of Sale → Configuration → Payment Methods**, name the method for the counter. Select an active bank journal with the appropriate outstanding account. Leave **Identify Customer** off. Set **Integration** to **Terminal**, choose **Square Terminal** under **Integrate with**, and select the device. Add the method to the register and confirm **Ready for Square Terminal**. If unavailable, check **Square Readiness**. Other payment methods remain usable.

To replace hardware, close the session, create a new terminal record and update the payment method. For a change of company, merchant application or environment, create a new connection. Keep the original connection and webhook subscription available for historical payments.

## Take and finish a counter sale

1. Open **Payment** on the Odoo sale. For a walk-in customer, leave **Customer** blank and **Invoice** off. Select the named Square method, enter the card share and choose **Send**. Check the device and displayed amount before the customer taps.
2. For a split, use exact shares of Odoo cash and one or more Square cards. On a €60 sale with €20 cash, send €40 to the terminal. **Authorized by Square** means a share is reserved while the order awaits the balance. A cash-only sale stays local when you use the Odoo cash method from the start. If cash is the final share, enter it and choose **Validate**.
3. Once fully paid, finish any remaining Odoo step on the same sale, such as selecting an invoice customer or choosing **Validate**, and issue the receipt. Global discounts and complex combos can appear as a receipt summary in Square. Odoo keeps the itemized basket and blocks dispatch on pricing or currency discrepancies.

When **Restrict Price Modifications to Managers** is enabled, manually priced baskets require an authenticated Odoo POS manager login. Selecting a manager through the employee switcher alone is insufficient.

Enter required lot or serial numbers in Odoo before sending the payment. Odoo keeps those details with the sale and its stock records. Choose priced product options in Odoo so the register includes their configured surcharges in the verified total. Square can show a receipt summary while Odoo keeps the native sale details.

### Tips

In **Point of Sale → Configuration → Settings**, select the register and configure an active, tax-free service product under **Tips → Tip Product**.

- **Device tips.** Enable **Square Terminal Tips**. Leave the Odoo tip empty so each cardholder can tip on their share. The Odoo receipt shows one combined tip.
- **Register tips.** Enable **Tips** and make a custom register-only tip product **Available in POS**. Enter the tip before entering shares or sending a card request. For €60 of goods plus a €6 tip, enter €22 cash and €44 card. The shares include €2 and €4 of tip respectively. The terminal skips the tip prompt.

Confirm splits and pre-entered tips on the merchant’s device before offering them. Square lists EU support for linked orders and describes splits where order IDs are supported. Its delayed-capture and pre-entered-tip release notes name Australia, Canada, Japan and the UK. [Regional capabilities](https://developer.squareup.com/docs/in-person-payment-options) · [Linked-order splits](https://developer.squareup.com/docs/terminal-api/take-payments-for-orders#optional-take-a-split-tender-payment) · [Checkout release notes](https://developer.squareup.com/docs/changelog/connect-logs/2025-04-16)

### Recover a payment on the original sale

| What the cashier sees | What to do |
| --- | --- |
| **Save the sale before sending it to Square Terminal** | Reconnect the register and choose **Send** again on the original line. |
| Waiting, timeout or lost response | After a reload, find the sale under **Orders**, reopen **Payment** and choose **Check status** on the original line. Keep the share reserved until Square confirms the outcome. If the checkout has expired, ask a manager to check the original Square order and payment references before collecting again. |
| Busy device, and Odoo confirms nothing was sent | Remove the unsent line or send it when the terminal is free. If Odoo shows **Ready to send the original Square payment** after reconnecting, use **Send to Square Terminal** on that line. |
| Decline while the terminal checkout remains active | Follow the prompt on that terminal or request cancellation. Use **Retry** when Odoo confirms nothing was collected. Keep any other authorized share. |
| **No Square payment was collected** after sending | Correct the setup issue and choose **Retry**. To change the basket or take another tender, cancel the Square payment and wait for confirmation first. If merchant access was lost, ask a POS manager to reconnect the original Square connection. |
| Customer changes the basket or switches to cash | Use **Cancel payment** or **Abandon split**. Choose **Continue cancellation** if offered. Wait for confirmed release before editing or collecting again. If a card succeeded during cancellation, continue with the balance shown. |

## Reconcile payments and handover

- As a POS manager, open **Point of Sale → Orders → Square Terminal Payments**, or **Square Payments** on the POS order. Use **Check Square Payment** to refresh the original payment. Compare the verified base and tip, original Square IDs, **Last Successful Check** and any discrepancy. If access was revoked, reconnect the original merchant. Webhooks and scheduled checks continue after the register closes.
- Scheduled reconciliation checks unresolved payments ahead of routine rechecks of verified payments. If a run makes progress and reaches its limit with more overdue work, Odoo schedules another run one minute later.
- Unresolved payments and verified sales still in draft block session closure. Use the order, amount and status in the close message to finish the sale. **Pending Square handover** alone allows receipt issuance and closure. Odoo retries sending the counter handover time until Square confirms it.
- For a refund issued in Square, use the amount and reference under **Confirmed Square Refunds** to decide which credit, accounting correction or goods return to record in Odoo.
- The optional importer keeps terminal sales and refunds on their original POS order. To synchronize stock, map the Square location to the register’s warehouse and configure the same selling unit in both catalogs. Square Terminal uses a SKU-matched variation only when the selling unit can be verified. For example, an Odoo sale in kilograms can use a Square variation measured in kilograms. If the variation is measured in pounds, Square receives a named line or a receipt summary. [Importer configuration](../README.md#square-configuration)

### Where to look in Square

In France, open **Commandes et paiements → Transactions**. Some accounts show **Factures et paiements** or **Paiements** as the parent menu. Filter by date and location, then match the transaction to the amount and payment reference in Odoo. Square documents that Dashboard Orders requires fulfillment and payment. An anonymous counter sale has no pickup booking and may be absent from **Commandes**. [Square France transaction search](https://squareup.com/help/fr/fr/article/5145-transaction-search) · [Dashboard order visibility](https://developer.squareup.com/docs/orders-api/create-orders#view-orders-in-the-square-dashboard-or-square-products)

## Qualify the French terminal on hardware

After automated acceptance, use the merchant-approved Production device and card for the agreed anonymous €0.01 purchase.

1. Record the Odoo revision, Square API and Terminal app versions, France/EUR location, payment method and readiness. Reuse a ready pairing on the intended device.
2. Use one tax-free €0.01 POS service item. Turn off all-tender rounding and device tipping. Leave **Customer** blank and **Invoice** off. Send the €0.01 share once and check the device display before presenting the card. Choose a paper receipt or skip delivery. If the outcome is unclear, check the original payment before another purchase. Record any refusal of the amount.
3. Match the original receipt and Square order and payment IDs to one completed €0.01 EUR card payment with zero tip. Check **Transactions** and **Commandes** with the same date and location filters. Record what the account actually shows.
4. Check the Odoo counter handover time and **Recorded in Square**. If pending, record the error and retry outcome. If available, compare the original Square order’s `odoo-pos-in-store-handover` attribute. Keep credentials, pairing codes, customer information and card details out of the evidence.

import { registry } from "@web/core/registry";
import * as Chrome from "@point_of_sale/../tests/tours/utils/chrome_util";
import * as Dialog from "@point_of_sale/../tests/tours/utils/dialog_util";
import * as NumberPopup from "@point_of_sale/../tests/tours/utils/number_popup_util";
import * as PaymentScreen from "@point_of_sale/../tests/tours/utils/payment_screen_util";
import * as PartnerList from "@point_of_sale/../tests/tours/utils/partner_list_util";
import * as ProductConfigurator from "@point_of_sale/../tests/tours/utils/product_configurator_util";
import * as ProductScreen from "@point_of_sale/../tests/tours/utils/product_screen_util";
import * as ReceiptScreen from "@point_of_sale/../tests/tours/utils/receipt_screen_util";
import { inLeftSide, refresh } from "@point_of_sale/../tests/tours/utils/common";
import { setOfflineMode, setOnlineMode } from "@point_of_sale/../tests/tours/utils/offline_util";

function openDeskPadPaymentScreen() {
    return [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
    ];
}

function sendDeskPadToSquare() {
    return [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
    ];
}

function reopenOrderAfterReload(storageKey) {
    return [
        refresh(),
        { trigger: "body:not(:has(.pos-loader))" },
        Chrome.clickMenuOption("Orders"),
        {
            trigger: ".ticket-screen .order-row",
            async run(helpers) {
                const uuid = sessionStorage.getItem(storageKey);
                const order = window.posmodel.models["pos.order"].getBy("uuid", uuid);
                const rows = [...document.querySelectorAll(".ticket-screen .order-row")].filter((row) =>
                    [...row.querySelectorAll(".col")].some((column) => column.textContent.trim() === order?.pos_reference));
                if (rows.length !== 1) {
                    throw new Error("Original Square order is missing from the native Orders screen");
                }
                await helpers.dblclick(rows[0]);
            },
        },
    ];
}

registry.category("web_tour.tours").add("SquareTerminalCardCheckout", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.addOrderline("Monitor Stand"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "15.53"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Waiting for Square Terminal')" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("15.53"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
        { trigger: ".pos-receipt .orderline:contains('Monitor Stand')" },
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalCashOnly", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        { trigger: '.paymentmethod:not([aria-disabled="true"]):contains("Square Counter")' },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "12.34"),
        ...PaymentScreen.clickValidate(),
        ...ReceiptScreen.receiptIsThere(),
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalRetryOfflineDraft", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        setOfflineMode(),
        {
            trigger: ".payment-screen .send_payment_request",
            run() {
                const order = window.posmodel.get_order();
                sessionStorage.setItem("squareOfflineDraft", order.uuid);
                sessionStorage.setItem("squareOfflineTender", order.payment_ids[0].uuid);
                this.anchor.click();
            },
        },
        {
            trigger: ".modal:contains('Save the sale before sending it to Square Terminal.') .btn-primary, " +
                ".payment-screen .electronic_status:contains('Square payment outcome needs verification')",
            run() {
                if (!this.anchor.matches(".btn-primary")) {
                    throw new Error("A failed draft sync must leave the original tender ready to send");
                }
                this.anchor.click();
            },
        },
        setOnlineMode(),
        {
            trigger: ".payment-screen .send_payment_request",
            run() {
                const order = window.posmodel.get_order();
                if (order.uuid !== sessionStorage.getItem("squareOfflineDraft") ||
                    order.payment_ids[0]?.uuid !== sessionStorage.getItem("squareOfflineTender")) {
                    throw new Error("Retry must use the original Square order and tender");
                }
                this.anchor.click();
            },
        },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalDiscountedFraction", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad", 1.5),
        ...inLeftSide([
            ...ProductScreen.addDiscount(10),
            ...ProductScreen.selectedOrderlineHasDirect("Desk Pad", 1.5, "16.20"),
        ]),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "16.20"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("16.20"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalCatalogSellingUnit", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad", 1.5),
        ...ProductScreen.addOrderline("Monitor Stand", 2),
        ...ProductScreen.addOrderline("Letter Tray", 1.5),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "32.09"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("32.09"),
    ],
});

registry.category("web_tour.tours").add("SquareTerminalTrackedItems", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.clickDisplayedProduct("Desk Pad"),
        ...ProductScreen.enterLotNumber("DESK-101"),
        ...ProductScreen.clickDisplayedProduct("Monitor Stand"),
        ...ProductScreen.enterLotNumber("MONITOR-101"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "15.53"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("15.53"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad'):contains('SN DESK-101')" },
        { trigger: ".pos-receipt .orderline:contains('Monitor Stand'):contains('Lot Number MONITOR-101')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalConfiguredOption", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.clickDisplayedProduct("Desk Pad"),
        ...ProductConfigurator.pickRadio("Lacquered"),
        Dialog.confirm(),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "12.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.00"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad'):contains('Lacquered')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalGlobalDiscount", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.addOrderline("Monitor Stand"),
        ...ProductScreen.clickControlButton("Discount"),
        Dialog.confirm(),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "15.30"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("15.30"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
        { trigger: ".pos-receipt .orderline:contains('Monitor Stand')" },
        { trigger: ".pos-receipt .orderline:contains('Discount')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalManualPrice", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad", 1, 11, "11.00"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "11.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("11.00"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalMixedComboRecovery", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Lunch Combo"),
        ...ProductScreen.addOrderline("Whiteboard Pen"),
        ...inLeftSide([
            ...ProductScreen.addDiscount(10),
            ...ProductScreen.selectedOrderlineHasDirect("Whiteboard Pen", 1, "18.00"),
        ]),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        PaymentScreen.enterPaymentLineAmount("Checkout Cash", "20"),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "40.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        {
            trigger: ".payment-screen .electronic_status:contains('Check the original Square settlement')",
            run() {
                const order = window.posmodel.get_order();
                sessionStorage.setItem("squareMixedOrder", order.uuid);
                sessionStorage.setItem("squareMixedTenders", order.payment_ids.map((line) => line.uuid).sort().join());
            },
        },
        ...reopenOrderAfterReload("squareMixedOrder"),
        {
            trigger: ".payment-screen .square-check-status:contains('Check status')",
            run() {
                const order = window.posmodel.get_order();
                if (order.uuid !== sessionStorage.getItem("squareMixedOrder") ||
                    order.payment_ids.map((line) => line.uuid).sort().join() !== sessionStorage.getItem("squareMixedTenders")) {
                    throw new Error("The register must recover the original sale and both native tenders");
                }
                this.anchor.click();
            },
        },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("60.00"),
        { trigger: ".pos-receipt .orderline:contains('Lunch Combo')" },
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
        { trigger: ".pos-receipt .orderline:contains('Monitor Stand')" },
        { trigger: ".pos-receipt .orderline:contains('Whiteboard Pen')" },
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash'):contains('20.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter'):contains('40.00')" },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalCardsThenCash", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.clickNumpad("5"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "5.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        ...PaymentScreen.clickNumpad("6"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "5.00"),
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('7.00')" },
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.clickNumpad("4"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "4.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('3.00')" },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "3.00"),
        ...PaymentScreen.clickNumpad("2"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "2.00"),
        ...PaymentScreen.clickPaymentline("Square Counter", "4.00"),
        ...PaymentScreen.clickPaymentline("Checkout Cash", "2.00"),
        ...PaymentScreen.clickNumpad("3"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "3.00"),
        {
            trigger: ".payment-screen .validation-button.highlight",
            async run() {
                const order = window.posmodel.get_order();
                const card = order.payment_ids.find((line) => line.square_attempt_state === "authorized");
                const cash = order.payment_ids.find((line) => line.payment_method_id.name === "Checkout Cash");
                const status = await window.posmodel.data.call("pos.order", "square_check_payment", [order.uuid, card.uuid]);
                if (status.state !== "authorized" || status.cash_tenders[cash.uuid]) {
                    throw new Error("The new cash line must remain local until the cashier validates");
                }
                this.anchor.click();
            },
        },
        ...PaymentScreen.clickPaymentline("Square Counter", "4.00"),
        { trigger: ".payment-screen .electronic_status:contains('Check the original cash recording')" },
        {
            trigger: ".payment-screen .paymentline:has(.payment-infos:contains('Checkout Cash')):not(:has(.delete-button))",
        },
        { trigger: ".payment-screen .square-check-status:contains('Check status')", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.75"),
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash'):contains('3.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter'):contains('5.50')" },
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter'):contains('4.25')" },
        { trigger: ".pos-receipt .orderline:contains('Terminal Gratuity'):contains('0.75')" },
    ].flat(),
});

function enterOdooTip(keys) {
    return [
        { trigger: ".payment-screen button:has(.fa-heart):contains('Tip')", run: "click" },
        ...NumberPopup.enterValue(keys),
        { trigger: ".modal:contains('Add Tip') .btn-primary", run: "click" },
    ];
}

registry.category("web_tour.tours").add("SquareTerminalOdooTipRecovery", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...enterOdooTip("6"),
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        PaymentScreen.enterPaymentLineAmount("Checkout Cash", "22"),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "44.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        {
            trigger: ".payment-screen:has(button:disabled:contains('Tip')) " +
                ".electronic_status:contains('Check the original Square settlement')",
            run() {
                const order = window.posmodel.get_order();
                sessionStorage.setItem("squareOdooTipTenders", order.payment_ids.map((line) => line.uuid).sort().join());
            },
        },
        refresh(),
        { trigger: "body:not(:has(.pos-loader))" },
        Chrome.clickMenuOption("Orders"),
        { trigger: ".ticket-screen .order-row", run: "dblclick" },
        {
            trigger: ".payment-screen .square-check-status:contains('Check status')",
            run() {
                const order = window.posmodel.get_order();
                if (order.payment_ids.map((line) => line.uuid).sort().join() !==
                    sessionStorage.getItem("squareOdooTipTenders")) {
                    throw new Error("Reload must preserve the original cash and card tenders");
                }
                this.anchor.click();
            },
        },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("66.00"),
        { trigger: ".pos-receipt .orderline:contains('Terminal Gratuity'):contains('6.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash'):contains('22.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter'):contains('44.00')" },
        {
            trigger: ".pos-receipt",
            async run() {
                const pos = window.posmodel;
                const order = pos.get_order();
                const payment = order.payment_ids.find((line) => line.payment_method_id.name === "Square Counter");
                await pos.data.call("pos.order", "square_check_payment", [order.uuid, payment.uuid]);
                const tips = order.lines.filter((line) => line.product_id.display_name === "Terminal Gratuity");
                if (tips.length !== 1 || order.get_tip() !== 6) {
                    throw new Error("The original Odoo tip must appear once on the receipt");
                }
            },
        },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalOdooTipFinalCard", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...enterOdooTip("0.01"),
        ...Array.from({ length: 3 }, () => [
            ...PaymentScreen.clickPaymentMethod("Square Counter"),
            ...PaymentScreen.clickNumpad("2"),
            ...PaymentScreen.clickNumpad("5"),
            ...PaymentScreen.selectedPaymentlineHas("Square Counter", "25.00"),
            { trigger: ".payment-screen .send_payment_request", run: "click" },
            { trigger: ".payment-screen .electronic_payment .electronic_status:contains('Authorized by Square')" },
            { trigger: '.paymentmethod:not(.opacity-50):contains("Square Counter")' },
        ]).flat(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "25.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Check the original Square settlement')" },
        { trigger: ".payment-screen .square-check-status:contains('Check status')", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("100.00"),
        { trigger: ".pos-receipt .orderline:contains('Terminal Gratuity'):contains('0.01')" },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalOdooTipRemainder", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...enterOdooTip("0.02"),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.clickNumpad("4"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "4.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.clickNumpad("4"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "4.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('4.02')" },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "4.02"),
        ...PaymentScreen.clickValidate(),
        ...PaymentScreen.clickPaymentline("Square Counter", "4.00"),
        { trigger: ".payment-screen .electronic_status:contains('Check the original cash recording')" },
        { trigger: ".payment-screen .square-check-status:contains('Check status')", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.02"),
        { trigger: ".pos-receipt .orderline:contains('Terminal Gratuity'):contains('0.02')" },
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash'):contains('4.02')" },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalSingleOdooTip", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...enterOdooTip("2"),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "14.34"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("14.34"),
        { trigger: ".pos-receipt .orderline:contains('Terminal Gratuity'):contains('2.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('14.34')" },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalRejectsVoucher", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Store Voucher"),
        ...PaymentScreen.clickNumpad("2"),
        ...PaymentScreen.selectedPaymentlineHas("Store Voucher", "2.00"),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "10.34"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        {
            trigger: ".modal:contains('Square Terminal only supports combining Odoo cash and Square cards')" +
                ":contains('Remove Store Voucher') .btn-primary",
            run: "click",
        },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalPreviewDiscrepancy", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "12.34"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('The Square order total differs from the Odoo sale.')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalCancelRacesPayment", {
    steps: () => [
        ...sendDeskPadToSquare(),
        { trigger: ".payment-screen .electronic_status:contains('Waiting for Square Terminal')" },
        {
            trigger: ".payment-screen .square-cancel-payment",
            run() {
                if (!document.querySelector(".payment-screen .back-button").disabled) {
                    throw new Error("The basket must stay locked while Square owns the tender");
                }
                this.anchor.click();
            },
        },
        { trigger: ".payment-screen .electronic_status:contains('Payment Successful')" },
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('0.00')" },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        { trigger: ".modal:contains('The balance is reserved by the original Square payment.') .btn-primary", run: "click" },
        ...PaymentScreen.clickValidate(),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalLostCancellation", {
    steps: () => [
        ...sendDeskPadToSquare(),
        { trigger: ".payment-screen .electronic_status:contains('Waiting for Square Terminal')" },
        { trigger: ".payment-screen .square-cancel-payment", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Waiting for Square to confirm cancellation')" },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        { trigger: ".modal:contains('The balance is reserved by the original Square payment.') .btn-primary", run: "click" },
        { trigger: ".payment-screen .square-check-status:contains('Check status')", run: "click" },
        { trigger: ".payment-screen .back-button:not(:disabled)", run: "click" },
        ...ProductScreen.addOrderline("Monitor Stand"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "15.53"),
        ...PaymentScreen.clickValidate(),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("15.53"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
        { trigger: ".pos-receipt .orderline:contains('Monitor Stand')" },
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalLostCheckoutResponse", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        {
            trigger: ".payment-screen .send_payment_request",
            run() {
                const order = window.posmodel.get_order();
                sessionStorage.setItem("squareRecoveryOrder", order.uuid);
                sessionStorage.setItem("squareRecoveryTender", order.payment_ids[0].uuid);
                this.anchor.click();
                this.anchor.click();
            },
        },
        { trigger: ".payment-screen .electronic_status:contains('Square payment outcome needs verification')" },
        ...reopenOrderAfterReload("squareRecoveryOrder"),
        {
            trigger: ".product-screen, .payment-screen",
            async run(helpers) {
                if (document.querySelector(".product-screen")) {
                    await helpers.click(".product-screen .pay-order-button");
                }
            },
        },
        {
            trigger: ".payment-screen",
            run() {
                const order = window.posmodel.get_order();
                if (order.uuid !== sessionStorage.getItem("squareRecoveryOrder") ||
                    order.payment_ids[0]?.uuid !== sessionStorage.getItem("squareRecoveryTender")) {
                    throw new Error("Restore the original Square order and tender from the register");
                }
                if (document.querySelector(".payment-screen .send_force_done, .payment-screen .send_payment_request")) {
                    throw new Error("The original reserved Square tender must show Check status");
                }
            },
        },
        { trigger: ".payment-screen .square-check-status:contains('Check status')" },
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        { trigger: ".modal:contains('The balance is reserved by the original Square payment.') .btn-primary", run: "click" },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        { trigger: ".modal:contains('The balance is reserved by the original Square payment.') .btn-primary", run: "click" },
        { trigger: ".payment-screen .square-check-status", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Square reported a timeout')" },
        { trigger: ".payment-screen .square-check-status:contains('Check status')", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalSplitRecovery", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        PaymentScreen.enterPaymentLineAmount("Square Counter", "5"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "5.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('7.34')" },
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "7.34"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Card declined')" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        { trigger: ".payment-screen .paymentline.selected + .electronic_payment .square-check-status:contains('Check status')", run: "click" },
        { trigger: ".payment-screen .square-check-status:contains('Retry')", run: "click" },
        {
            trigger: ".payment-screen:has(.paymentmethod:not(.opacity-50):contains('Square Counter')) " +
                ".electronic_status:contains('Check the original Square settlement')",
            run() {
                const order = window.posmodel.get_order();
                sessionStorage.setItem("squareSplitOrder", order.uuid);
                sessionStorage.setItem("squareSplitTenders", order.payment_ids.map((line) => line.uuid).sort().join());
            },
        },
        ...reopenOrderAfterReload("squareSplitOrder"),
        {
            trigger: ".payment-screen .square-check-status:contains('Check status')",
            run() {
                const order = window.posmodel.get_order();
                if (order.uuid !== sessionStorage.getItem("squareSplitOrder") ||
                    order.payment_ids.map((line) => line.uuid).sort().join() !== sessionStorage.getItem("squareSplitTenders")) {
                    throw new Error("The register must recover the original Square split and both tenders");
                }
                this.anchor.click();
            },
        },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
        { trigger: ".pos-receipt .paymentlines:contains('5.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('7.34')" },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalImmediateSplit", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        PaymentScreen.enterPaymentLineAmount("Square Counter", "0.01"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        {
            trigger: ".payment-screen .electronic_status:contains('Collected by Square')",
            run() {
                sessionStorage.setItem("squareImmediateOrder", window.posmodel.get_order().uuid);
            },
        },
        ...reopenOrderAfterReload("squareImmediateOrder"),
        { trigger: ".payment-screen .electronic_status:contains('Collected by Square')" },
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('0.01')" },
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "0.01"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Card declined')" },
        { trigger: ".payment-screen .paymentline.selected + .electronic_payment .square-check-status:contains('Check status')", run: "click" },
        { trigger: ".payment-screen .square-check-status:contains('Retry')", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("0.02"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalImmediateMixed", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        PaymentScreen.enterPaymentLineAmount("Checkout Cash", "20"),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "40.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("60.00"),
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash')" },
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter')" },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalSplitDeviceTips", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        PaymentScreen.enterPaymentLineAmount("Square Counter", "40"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "40.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        { trigger: ".payment-screen button:disabled:contains('Tip')" },
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('60.00')" },
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "60.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        {
            trigger: ".payment-screen .electronic_status:contains('Check the original Square settlement')",
            run() {
                const order = window.posmodel.get_order();
                sessionStorage.setItem("squareTippedOrder", order.uuid);
                sessionStorage.setItem("squareTippedTenders", order.payment_ids.map((line) => line.uuid).sort().join());
            },
        },
        refresh(),
        { trigger: "body:not(:has(.pos-loader))" },
        Chrome.clickMenuOption("Orders"),
        {
            trigger: ".ticket-screen .order-row",
            async run(helpers) {
                await helpers.dblclick(this.anchor);
            },
        },
        {
            trigger: ".payment-screen .square-check-status:contains('Check status')",
            run() {
                const order = window.posmodel.get_order();
                if (order.uuid !== sessionStorage.getItem("squareTippedOrder") ||
                    order.payment_ids.map((line) => line.uuid).sort().join() !==
                        sessionStorage.getItem("squareTippedTenders")) {
                    throw new Error("Reload must preserve the original sale and both tender identities");
                }
                this.anchor.click();
            },
        },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("105.00"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
        { trigger: ".pos-receipt .orderline:contains('Terminal Gratuity'):contains('5.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('43.00')" },
        { trigger: ".pos-receipt .paymentlines:contains('62.00')" },
        {
            trigger: ".pos-receipt",
            async run() {
                const pos = window.posmodel;
                const order = pos.get_order();
                const [payment] = order.payment_ids;
                await pos.data.call("pos.order", "square_check_payment", [order.uuid, payment.uuid]);
                if (order.lines.filter((line) => line.product_id.display_name === "Terminal Gratuity").length !== 1) {
                    throw new Error("The native receipt must only contain one gratuity line");
                }
            },
        },
    ].flat(),
});

registry.category("web_tour.tours").add("SquareTerminalDeviceTipMissingProduct", {
    steps: () => [
        ...sendDeskPadToSquare(),
        { trigger: ".modal:contains('Configure an active, tax-free Odoo tip service product') .btn-primary", run: "click" },
        { trigger: ".payment-screen .send_payment_request" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalDeviceTipZero", {
    steps: () => [
        ...sendDeskPadToSquare(),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
        { trigger: ".pos-receipt:not(:has(.orderline:contains('Terminal Gratuity'))) .orderline:contains('Desk Pad')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalSingleDeviceTip", {
    steps: () => [
        ...sendDeskPadToSquare(),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("15.64"),
        { trigger: ".pos-receipt .orderline:contains('Terminal Gratuity'):contains('3.30')" },
        { trigger: ".pos-receipt .paymentlines:contains('15.64')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalAbandonSplit", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.clickNumpad("3"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "3.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Authorized by Square')" },
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.clickNumpad("4"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "4.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_payment .electronic_status:contains('Authorized by Square')" },
        { trigger: ".payment-screen .payment-status-remaining .amount:contains('5.34')" },
        { trigger: ".payment-screen .square-cancel-payment:contains('Abandon split')", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('Waiting for Square to confirm cancellation')" },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        { trigger: ".modal:contains('The balance is reserved by the original Square payment.') .btn-primary", run: "click" },
        { trigger: ".payment-screen .square-cancel-payment:contains('Continue cancellation')", run: "click" },
        { trigger: ".payment-screen .back-button:not(:disabled)" },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "12.34"),
        ...PaymentScreen.clickValidate(),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalCloseWithUncertainPayment", {
    steps: () => [
        ...sendDeskPadToSquare(),
        { trigger: ".payment-screen .electronic_status:contains('Square payment outcome needs verification')" },
        ...Chrome.clickMenuOption("Close Register"),
        { trigger: ".close-pos-popup .btn-primary:contains('Close Register')", run: "click" },
        {
            trigger: ".modal-body:contains('Square payment'):contains('12.34'):contains('PENDING')",
            run() {
                if (!this.anchor.textContent.includes(window.posmodel.get_order().pos_reference)) {
                    throw new Error("Register close must identify the original order");
                }
            },
        },
        { trigger: ".modal:not(.o_inactive_modal) .modal-footer .btn-primary", run: "click" },
        { trigger: ".pos-topheader .list-plus-btn", run: "click" },
        ...ProductScreen.addOrderline("Monitor Stand"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        ...PaymentScreen.clickValidate(),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("3.19"),
    ],
});

registry.category("web_tour.tours").add("SquareTerminalInvoiceCustomer", {
    steps: () => [
        ...openDeskPadPaymentScreen(),
        ...PaymentScreen.clickInvoiceButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        {
            trigger: ".payment-screen .send_payment_request",
            run() {
                const order = window.posmodel.get_order();
                sessionStorage.setItem("squareInvoiceOrder", order.uuid);
                sessionStorage.setItem("squareInvoiceTender", order.payment_ids[0].uuid);
                this.anchor.click();
            },
        },
        {
            trigger: ".modal:contains('Please select the Customer') .modal-footer .btn-secondary",
            run: "click",
        },
        { trigger: ".payment-screen .electronic_status:contains('Payment Successful')" },
        refresh(),
        { trigger: "body:not(:has(.pos-loader))" },
        {
            trigger: ".modal:contains('Please select the Customer') .modal-footer .btn-primary",
            run() {
                const order = window.posmodel.get_order();
                const [tender] = order.payment_ids;
                if (
                    order.uuid !== sessionStorage.getItem("squareInvoiceOrder") ||
                    tender.uuid !== sessionStorage.getItem("squareInvoiceTender") ||
                    tender.transaction_id !== "SQUAREPAYMENT" ||
                    order.get_partner()
                ) {
                    throw new Error("The native prompt must finish the original verified invoice payment");
                }
                this.anchor.click();
            },
        },
        PartnerList.clickPartner("Square Invoice Customer"),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
        { trigger: ".pos-receipt .paymentlines:contains('Square Counter')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalCounterHandover", {
    steps: () => [
        ...sendDeskPadToSquare(),
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("12.34"),
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
    ],
});

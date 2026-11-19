import { refresh } from "@point_of_sale/../tests/tours/utils/common";
import { registry } from "@web/core/registry";
import * as Chrome from "@point_of_sale/../tests/tours/utils/chrome_util";
import * as PaymentScreen from "@point_of_sale/../tests/tours/utils/payment_screen_util";
import * as ProductScreen from "@point_of_sale/../tests/tours/utils/product_screen_util";
import * as ReceiptScreen from "@point_of_sale/../tests/tours/utils/receipt_screen_util";

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
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
        { trigger: '.paymentmethod:not([aria-disabled="true"]):contains("Square Counter")' },
        ...PaymentScreen.clickPaymentMethod("Checkout Cash"),
        ...PaymentScreen.selectedPaymentlineHas("Checkout Cash", "12.34"),
        ...PaymentScreen.clickValidate(),
        ...ReceiptScreen.receiptIsThere(),
        { trigger: ".pos-receipt .paymentlines:contains('Checkout Cash')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalLostCheckoutResponse", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
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
        refresh(),
        { trigger: "body:not(:has(.pos-loader))" },
        Chrome.clickMenuOption("Orders"),
        {
            trigger: ".ticket-screen .order-row",
            async run(helpers) {
                const uuid = sessionStorage.getItem("squareRecoveryOrder");
                const order = window.posmodel.models["pos.order"].getBy("uuid", uuid);
                const rows = [...document.querySelectorAll(".ticket-screen .order-row")].filter((row) =>
                    [...row.querySelectorAll(".col")].some((column) => column.textContent.trim() === order?.pos_reference));
                if (rows.length !== 1) {
                    throw new Error("Original Square order is missing from the native Orders screen");
                }
                await helpers.dblclick(rows[0]);
            },
        },
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

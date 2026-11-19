import { registry } from "@web/core/registry";
import * as Chrome from "@point_of_sale/../tests/tours/utils/chrome_util";
import * as PaymentScreen from "@point_of_sale/../tests/tours/utils/payment_screen_util";
import * as ProductScreen from "@point_of_sale/../tests/tours/utils/product_screen_util";
import * as ReceiptScreen from "@point_of_sale/../tests/tours/utils/receipt_screen_util";
import { inLeftSide, refresh } from "@point_of_sale/../tests/tours/utils/common";

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

registry.category("web_tour.tours").add("SquareTerminalComboSummary", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Lunch Combo"),
        ...ProductScreen.addOrderline("Whiteboard Pen"),
        ...inLeftSide([
            ...ProductScreen.addDiscount(10),
            ...ProductScreen.selectedOrderlineHasDirect("Whiteboard Pen", 1, "18.00"),
        ]),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "60.00"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        ...ReceiptScreen.receiptIsThere(),
        ...ReceiptScreen.receiptAmountTotalIs("60.00"),
        { trigger: ".pos-receipt .orderline:contains('Lunch Combo')" },
        { trigger: ".pos-receipt .orderline:contains('Desk Pad')" },
        { trigger: ".pos-receipt .orderline:contains('Monitor Stand')" },
        { trigger: ".pos-receipt .orderline:contains('Whiteboard Pen')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalPreviewDiscrepancy", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        ...PaymentScreen.selectedPaymentlineHas("Square Counter", "12.34"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
        { trigger: ".payment-screen .electronic_status:contains('The Square order total differs from the Odoo sale.')" },
    ],
});

registry.category("web_tour.tours").add("SquareTerminalCancelRacesPayment", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
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
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
        ...PaymentScreen.clickPaymentMethod("Square Counter"),
        { trigger: ".payment-screen .send_payment_request", run: "click" },
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

registry.category("web_tour.tours").add("SquareTerminalSplitRecovery", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
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
        refresh(),
        { trigger: "body:not(:has(.pos-loader))" },
        Chrome.clickMenuOption("Orders"),
        {
            trigger: ".ticket-screen .order-row",
            async run(helpers) {
                const uuid = sessionStorage.getItem("squareSplitOrder");
                const order = window.posmodel.models["pos.order"].getBy("uuid", uuid);
                const rows = [...document.querySelectorAll(".ticket-screen .order-row")].filter((row) =>
                    [...row.querySelectorAll(".col")].some((column) => column.textContent.trim() === order?.pos_reference));
                if (rows.length !== 1) {
                    throw new Error("Original Square split is missing from the native Orders screen");
                }
                await helpers.dblclick(rows[0]);
            },
        },
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

registry.category("web_tour.tours").add("SquareTerminalAbandonSplit", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Desk Pad"),
        ...ProductScreen.clickPayButton(),
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

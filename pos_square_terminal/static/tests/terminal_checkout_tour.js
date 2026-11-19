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

import { registry } from "@web/core/registry";
import * as Chrome from "@point_of_sale/../tests/tours/utils/chrome_util";
import * as PaymentScreen from "@point_of_sale/../tests/tours/utils/payment_screen_util";
import * as ProductScreen from "@point_of_sale/../tests/tours/utils/product_screen_util";
import * as ReceiptScreen from "@point_of_sale/../tests/tours/utils/receipt_screen_util";

registry.category("web_tour.tours").add("SquareTerminalIncompatibleDevice", {
    steps: () => [
        ...Chrome.startPoS(),
        ...ProductScreen.addOrderline("Square setup product"),
        ...ProductScreen.clickPayButton(),
        {
            trigger: '.paymentmethod[aria-disabled="true"]:contains("Square Counter"):contains("6.62")',
            run: "click",
        },
        {
            trigger: '.modal:contains("Square Terminal unavailable"):contains("6.62") .btn-primary',
            run: "click",
        },
        ...PaymentScreen.emptyPaymentlines("10.00"),
        ...PaymentScreen.clickPaymentMethod("Setup Cash"),
        ...PaymentScreen.selectedPaymentlineHas("Setup Cash", "10.00"),
        ...PaymentScreen.clickValidate(),
        ...ReceiptScreen.receiptIsThere(),
    ],
});

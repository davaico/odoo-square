import { AlertDialog } from "@web/core/confirmation_dialog/confirmation_dialog";
import { _t } from "@web/core/l10n/translation";
import { patch } from "@web/core/utils/patch";
import { PosOrder } from "@point_of_sale/app/models/pos_order";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";

function squareUnavailableReason(method) {
    if (method?.use_payment_terminal === "square_terminal") {
        return method.square_terminal_unavailable_reason ||
            (!method.payment_terminal && _t("Square Terminal checkout is unavailable."));
    }
    return false;
}

patch(PosOrder.prototype, {
    addPaymentline(method) {
        const reason = squareUnavailableReason(method);
        if (reason) {
            return { status: false, data: reason };
        }
        return super.addPaymentline(...arguments);
    },
});

patch(PaymentScreen.prototype, {
    squareUnavailableReason,
    async addNewPaymentLine(method) {
        const reason = this.squareUnavailableReason(method);
        if (reason) {
            this.dialog.add(AlertDialog, {
                title: _t("Square Terminal unavailable"),
                body: reason,
            });
            return false;
        }
        return super.addNewPaymentLine(...arguments);
    },
});

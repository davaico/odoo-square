import { AlertDialog } from "@web/core/confirmation_dialog/confirmation_dialog";
import { _t } from "@web/core/l10n/translation";
import { RPCError } from "@web/core/network/rpc";
import { patch } from "@web/core/utils/patch";
import { PaymentInterface } from "@point_of_sale/app/payment/payment_interface";
import { PosOrder } from "@point_of_sale/app/models/pos_order";
import { PosPayment } from "@point_of_sale/app/models/pos_payment";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { PaymentScreenPaymentLines } from "@point_of_sale/app/screens/payment_screen/payment_lines/payment_lines";
import { register_payment_method } from "@point_of_sale/app/store/pos_store";

const PAID_STATES = ["paid", "done", "invoiced"];

function isSquarePayment(line) {
    return line?.payment_method_id.use_payment_terminal === "square_terminal" ||
        Boolean(line?.square_attempt_state);
}

function isReservedSquarePayment(line) {
    return isSquarePayment(line) && (
        Boolean(line.square_attempt_state) || ["waiting", "done"].includes(line.get_payment_status())
    );
}

function showSquareMessage(dialog, body) {
    dialog.add(AlertDialog, { title: _t("Square Terminal"), body });
}

class SquarePayment extends PaymentInterface {
    get fast_payments() {
        return false;
    }

    async send_payment_request(uuid) {
        let order = this.pos.get_order();
        const orderUuid = order.uuid;
        let line = order.payment_ids.find((line) => line.uuid === uuid);
        let preparing = true;
        try {
            const synced = await this.pos.syncAllOrders({ orders: [order], throw: true });
            order = synced?.find((record) => record.uuid === orderUuid && record.state === "draft");
            const savedLine = order?.payment_ids.find((line) => line.uuid === uuid);
            if (!savedLine || typeof savedLine.id !== "number") {
                showSquareMessage(this.env.services.dialog, _t("Save the sale before sending it to Square Terminal."));
                line.set_payment_status("pending");
                return false;
            }
            line = savedLine;
            const call = async (method) => {
                const result = await this.pos.data.call("pos.order", method, [orderUuid, uuid]);
                line.update({ square_attempt_state: result.state });
                return result;
            };
            let status = await call("square_prepare_payment");
            preparing = false;
            if (status.state === "prepared") {
                status = await call("square_prepare_linked_order");
            }
            if (status.state === "ready") {
                status = await call("square_dispatch_payment");
            }
            const deadline = Date.now() + 90_000;
            while (status.state === "pending" && Date.now() < deadline) {
                await new Promise((resolve) => setTimeout(resolve, 1200));
                status = await call("square_check_payment");
            }
            if (status.state === "verified") {
                line.update({ transaction_id: status.payment_id, payment_status: "done" });
                return true;
            }
        } catch (error) {
            if (preparing && error instanceof RPCError && !line.square_attempt_state) {
                line.set_payment_status("pending");
                showSquareMessage(this.env.services.dialog, error.data?.message || error.message);
            } else {
                line.update({ square_attempt_state: "uncertain" });
            }
        }
        return false;
    }
}

register_payment_method("square_terminal", SquarePayment);

patch(PosPayment.prototype, {
    set_amount() {
        if (!isReservedSquarePayment(this)) {
            return super.set_amount(...arguments);
        }
    },
    async pay() {
        if (!isSquarePayment(this)) {
            return super.pay(...arguments);
        }
        if (isReservedSquarePayment(this)) {
            return false;
        }
        this.set_payment_status("waiting");
        return this.payment_method_id.payment_terminal.send_payment_request(this.uuid);
    },
});

patch(PosOrder.prototype, {
    add_paymentline() {
        if (this.payment_ids.some(isReservedSquarePayment)) {
            return false;
        }
        return super.add_paymentline(...arguments);
    },
    remove_paymentline(line) {
        if (!isReservedSquarePayment(line)) {
            return super.remove_paymentline(...arguments);
        }
    },
    stop_electronic_payment() {
        if (!this.payment_ids.some(isReservedSquarePayment)) {
            return super.stop_electronic_payment(...arguments);
        }
    },
});

patch(PaymentScreen.prototype, {
    get squarePaymentReserved() {
        return this.paymentLines.some(isReservedSquarePayment);
    },
    async addNewPaymentLine() {
        if (this.squarePaymentReserved) {
            showSquareMessage(this.dialog, _t("The balance is reserved by the original Square payment."));
            return false;
        }
        return super.addNewPaymentLine(...arguments);
    },
    deletePaymentLine(uuid) {
        const line = this.paymentLines.find((line) => line.uuid === uuid);
        if (!isReservedSquarePayment(line)) {
            return super.deletePaymentLine(...arguments);
        }
    },
    async validateOrder() {
        if (this.paymentLines.some((line) => isSquarePayment(line) && line.square_attempt_state !== "verified")) {
            showSquareMessage(this.dialog, _t("Wait for Square to verify this payment before validating the sale."));
            return;
        }
        return super.validateOrder(...arguments);
    },
    async _finalizeValidation() {
        const order = this.currentOrder;
        const hasSquare = this.squarePaymentReserved;
        const result = await super._finalizeValidation(...arguments);
        if (hasSquare && order.state === "paid" && !PAID_STATES.includes(order.raw.state)) {
            order.state = "draft";
            showSquareMessage(this.dialog, _t("Square verified payment. Validate the original sale once the register is online."));
        }
        return result;
    },
    async afterOrderValidation(synced) {
        if (this.squarePaymentReserved && (!synced || !PAID_STATES.includes(this.currentOrder.raw.state))) {
            return;
        }
        return super.afterOrderValidation(...arguments);
    },
    async sendForceDone(line) {
        if (!isSquarePayment(line)) {
            return super.sendForceDone(...arguments);
        }
    },
});

patch(PaymentScreenPaymentLines.prototype, {
    isReservedSquarePayment,
    squarePendingMessage(line) {
        if (line.square_attempt_state === "blocked") {
            return _t("The Square order total differs from the Odoo sale.");
        }
        if (line.square_attempt_state === "uncertain") {
            return _t("Square payment outcome needs verification.");
        }
        if (line.square_attempt_state === "pending") {
            return _t("Waiting for Square Terminal");
        }
        return _t("Preparing Square payment");
    },
});

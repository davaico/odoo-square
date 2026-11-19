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
const squareRequestsInProgress = new Set();
const EXPLICIT_SEND_STATES = ["prepared", "ready", "failed"];

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

    async call(line, method, ...args) {
        const status = await this.pos.data.call(
            "pos.order", method, [line.pos_order_id.uuid, line.uuid, ...args]
        );
        if (status.state === "busy" || status.state === "unstarted") {
            line.update({ square_attempt_state: false, square_attempt_reference: false, payment_status: "pending" });
        } else {
            line.update({ square_attempt_state: status.state, square_attempt_reference: status.reference });
        }
        if (status.state === "busy") {
            showSquareMessage(this.env.services.dialog,
                _t("This Square Terminal is busy. No payment was sent. Remove the payment line or send it again when the terminal is free."));
        } else if (status.busy) {
            showSquareMessage(this.env.services.dialog,
                _t("This Square Terminal is busy. Retry when the terminal is free."));
        }
        if (status.state === "verified") {
            line.update({ transaction_id: status.payment_id, payment_status: "done" });
        }
        return status;
    }

    async checkPayment(line) {
        try {
            return (await this.call(line, "square_check_payment")).state === "verified";
        } catch {
            line.update({ square_attempt_state: "uncertain" });
            return false;
        }
    }

    async send_payment_request(uuid) {
        const order = this.pos.get_order();
        let line = order.payment_ids.find((line) => line.uuid === uuid);
        const originalState = line.square_attempt_state;
        let preparing = !originalState;
        try {
            let status;
            if (originalState === "failed") {
                status = await this.call(line, "square_retry_payment", line.square_attempt_reference);
            } else if (EXPLICIT_SEND_STATES.includes(originalState)) {
                status = await this.call(line, "square_check_payment");
            } else {
                const synced = await this.pos.syncAllOrders({ orders: [order], throw: true });
                const savedOrder = synced?.find((record) => record.uuid === order.uuid && record.state === "draft");
                const savedLine = savedOrder?.payment_ids.find((line) => line.uuid === uuid);
                if (!savedLine || typeof savedLine.id !== "number") {
                    line.set_payment_status("pending");
                    showSquareMessage(this.env.services.dialog, _t("Save the sale before sending it to Square Terminal."));
                    return false;
                }
                line = savedLine;
                status = await this.call(line, "square_prepare_payment");
                preparing = false;
            }
            if (status.state === "prepared") {
                status = await this.call(line, "square_prepare_linked_order");
            }
            if (status.state === "ready") {
                const reference = status.reference;
                status = await this.call(line, "square_mark_dispatching", reference);
                if (status.state === "dispatching" && status.reference === reference) {
                    status = await this.call(line, "square_dispatch_payment", reference);
                }
            }
            const deadline = Date.now() + 90_000;
            while (status.state === "pending" && Date.now() < deadline) {
                await new Promise((resolve) => setTimeout(resolve, 1200));
                status = await this.call(line, "square_check_payment");
            }
            return status.state === "verified";
        } catch (error) {
            if (error instanceof RPCError) {
                if (preparing && !line.square_attempt_state) {
                    line.set_payment_status("pending");
                }
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
        if (isReservedSquarePayment(this) && !EXPLICIT_SEND_STATES.includes(this.square_attempt_state)) {
            return this.payment_method_id.payment_terminal.checkPayment(this);
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
    onMounted() {
        super.onMounted(...arguments);
        if (!this.selectedPaymentLine) {
            const reserved = this.paymentLines.find(isReservedSquarePayment);
            if (reserved) {
                this.selectPaymentLine(reserved.uuid);
            }
        }
    },
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
    async sendPaymentRequest(line) {
        if (!isSquarePayment(line)) {
            return super.sendPaymentRequest(...arguments);
        }
        if (squareRequestsInProgress.has(line.uuid)) {
            return;
        }
        squareRequestsInProgress.add(line.uuid);
        try {
            await super.sendPaymentRequest(...arguments);
        } finally {
            squareRequestsInProgress.delete(line.uuid);
        }
    },
    async sendForceDone(line) {
        if (!isSquarePayment(line)) {
            return super.sendForceDone(...arguments);
        }
    },
});

patch(PaymentScreenPaymentLines.prototype, {
    isReservedSquarePayment,
    squareActionLabel(line) {
        if (line.square_attempt_state === "failed") {
            return _t("Retry");
        }
        if (EXPLICIT_SEND_STATES.includes(line.square_attempt_state)) {
            return _t("Send to Square Terminal");
        }
        return _t("Check status");
    },
    squarePendingMessage(line) {
        if (line.square_attempt_state === "blocked") {
            return _t("The Square order total differs from the Odoo sale.");
        }
        if (["dispatching", "uncertain"].includes(line.square_attempt_state)) {
            return _t("Square payment outcome needs verification.");
        }
        if (line.square_attempt_state === "declined") {
            return _t("Card declined. The original checkout is still active on Square Terminal.");
        }
        if (line.square_attempt_state === "failed") {
            return _t("Square confirmed that no payment was collected.");
        }
        if (line.square_attempt_state === "timed_out") {
            return _t("Square reported a timeout. Check the original payment before collecting again.");
        }
        if (line.square_attempt_state === "pending") {
            return _t("Waiting for Square Terminal");
        }
        if (line.square_attempt_state === "prepared" || line.square_attempt_state === "ready") {
            return _t("Ready to send the original Square payment.");
        }
        return _t("Preparing Square payment");
    },
});

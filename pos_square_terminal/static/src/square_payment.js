import { useEffect } from "@odoo/owl";
import { AlertDialog } from "@web/core/confirmation_dialog/confirmation_dialog";
import { _t } from "@web/core/l10n/translation";
import { RPCError } from "@web/core/network/rpc";
import { roundPrecision } from "@web/core/utils/numbers";
import { patch } from "@web/core/utils/patch";
import { PaymentInterface } from "@point_of_sale/app/payment/payment_interface";
import { PosOrder } from "@point_of_sale/app/models/pos_order";
import { PosPayment } from "@point_of_sale/app/models/pos_payment";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { PaymentScreenPaymentLines } from "@point_of_sale/app/screens/payment_screen/payment_lines/payment_lines";
import { PaymentScreenStatus } from "@point_of_sale/app/screens/payment_screen/payment_status/payment_status";
import { PosStore, register_payment_method } from "@point_of_sale/app/store/pos_store";

const PAID_STATES = ["paid", "done", "invoiced"];
const squareRequestsInProgress = new Set();
const squareCancellationsInProgress = new Set();
const resumeSquarePolling = new Map();
const EXPLICIT_SEND_STATES = ["prepared", "ready", "failed"];

function isSquarePayment(line) {
    return line?.payment_method_id.use_payment_terminal === "square_terminal" ||
        Boolean(line?.square_attempt_state);
}

function isReservedSquarePayment(line) {
    return isSquarePayment(line) && line.square_attempt_state !== "cancelled" && (
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
        const order = line.pos_order_id;
        const status = await this.pos.data.call(
            "pos.order", method, [order.uuid, line.uuid, ...args]
        );
        let cancelled = false;
        for (const tender of [...order.payment_ids]) {
            const result = tender.uuid === line.uuid ? status : status.tenders?.[tender.uuid];
            if (!result || (tender.square_attempt_state === "verified" && result.state !== "verified")) {
                continue;
            }
            if (result.state === "cancelled") {
                tender.update({ square_attempt_state: "cancelled" });
                order.remove_paymentline(tender);
                cancelled = true;
            } else if (result.state === "busy" || result.state === "unstarted") {
                tender.update({ square_attempt_state: false, square_attempt_reference: false, payment_status: "pending" });
            } else {
                tender.update({ square_attempt_state: result.state, square_attempt_reference: result.reference });
                if (result.state === "verified") {
                    tender.update({ transaction_id: result.payment_id, payment_status: "done" });
                }
            }
        }
        if (cancelled) {
            this.env.services.notification.add(_t("Square cancellation confirmed. You can edit the basket or take another payment."));
        }
        if (status.state === "busy") {
            showSquareMessage(this.env.services.dialog,
                _t("This Square Terminal is busy. No payment was sent. Remove the payment line or send it again when the terminal is free."));
        } else if (status.busy) {
            showSquareMessage(this.env.services.dialog,
                _t("This Square Terminal is busy. Retry when the terminal is free."));
        }
        return status;
    }

    async settleIfCovered(line, status) {
        if (status.state === "authorized") {
            status = await this.call(line, "square_prepare_settlement", status.reference);
        }
        if (["settling", "settlement_blocked"].includes(status.state)) {
            status = await this.call(line, "square_settle_payment", status.reference);
        }
        return status.state === "verified";
    }

    async checkPayment(line) {
        try {
            const status = await this.call(line, "square_check_payment");
            return await this.settleIfCovered(line, status);
        } catch {
            if (!["verified", "cancelled", "authorized", "settling"].includes(line.square_attempt_state)) {
                line.update({ square_attempt_state: "uncertain" });
            }
            return false;
        }
    }

    async cancelPayment(line) {
        try {
            return await this.call(line, "square_cancel_payment", line.square_attempt_reference);
        } catch {
            if (!["verified", "cancelled", "authorized", "settling"].includes(line.square_attempt_state)) {
                line.update({ square_attempt_state: "uncertain" });
            }
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
            while (
                status.state === "pending" && line.square_attempt_state === "pending" &&
                !squareCancellationsInProgress.has(order.uuid) && Date.now() < deadline
            ) {
                await new Promise((resolve) => {
                    const resume = () => {
                        clearTimeout(timer);
                        resumeSquarePolling.delete(line.uuid);
                        resolve();
                    };
                    const timer = setTimeout(resume, 1200);
                    resumeSquarePolling.set(line.uuid, resume);
                });
                if (squareCancellationsInProgress.has(order.uuid) || line.square_attempt_state !== "pending") {
                    break;
                }
                status = await this.call(line, "square_check_payment");
            }
            return await this.settleIfCovered(line, status);
        } catch (error) {
            if (error instanceof RPCError) {
                if (preparing && !line.square_attempt_state) {
                    line.set_payment_status("pending");
                }
                showSquareMessage(this.env.services.dialog, error.data?.message || error.message);
            } else if (!["verified", "cancelled", "authorized", "settling"].includes(line.square_attempt_state)) {
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
    squareCanAddShare(method) {
        return method?.use_payment_terminal === "square_terminal" &&
            this.payment_ids.every((line) => line.square_attempt_state === "authorized") &&
            this.get_due() > 0;
    },
    get squareAuthorizedAmount() {
        return this.payment_ids.reduce(
            (total, line) => total + (line.square_attempt_state === "authorized" ? line.amount : 0), 0
        );
    },
    get_due() {
        const due = super.get_due(...arguments);
        const authorized = this.squareAuthorizedAmount;
        return authorized ? roundPrecision(due - authorized, this.currency.rounding) : due;
    },
    recomputeOrderData() {
        super.recomputeOrderData(...arguments);
        if (this.payment_ids.some(isReservedSquarePayment)) {
            this.amount_return = Math.max(this.amount_return, 0);
        }
    },
    getDefaultAmountDueToPayIn() {
        return super.getDefaultAmountDueToPayIn(...arguments) - this.squareAuthorizedAmount;
    },
    electronic_payment_in_progress() {
        if (!this.squareAuthorizedAmount) {
            return super.electronic_payment_in_progress(...arguments);
        }
        return this.payment_ids.some((line) =>
            line.square_attempt_state !== "authorized" &&
            line.payment_status && !["done", "reversed"].includes(line.payment_status)
        );
    },
    add_paymentline(method) {
        if (this.payment_ids.some(isReservedSquarePayment) && !this.squareCanAddShare(method)) {
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

patch(PosStore.prototype, {
    showScreen(name, props) {
        const order = this.get_order();
        if (name === "ProductScreen" && order?.state === "draft" && order.payment_ids.some(isReservedSquarePayment)) {
            return super.showScreen("PaymentScreen", { orderUuid: order.uuid });
        }
        return super.showScreen(name, props);
    },
});

patch(PaymentScreen.prototype, {
    setup() {
        super.setup(...arguments);
        useEffect(
            () => {
                const order = this.currentOrder;
                if (
                    this.pos.config.auto_validate_terminal_payment &&
                    this.pos.get_order()?.uuid === order?.uuid &&
                    order?.state === "draft" && order.is_paid() &&
                    order.payment_ids.some((line) => line.square_attempt_state === "verified")
                ) {
                    this.validateOrder(false);
                }
            },
            () => [this.currentOrder?.uuid, this.currentOrder?.get_partner()?.id, this.currentOrder?.to_invoice]
        );
    },
    onMounted() {
        for (const line of this.paymentLines.filter((line) => line.square_attempt_state === "cancelled")) {
            this.currentOrder.remove_paymentline(line);
        }
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
    async addNewPaymentLine(method) {
        if (this.squarePaymentReserved && !this.currentOrder.squareCanAddShare(method)) {
            const message = this.currentOrder.squareAuthorizedAmount && method.use_payment_terminal !== "square_terminal"
                ? _t("Use another Square card or abandon the split to change tender.")
                : _t("The balance is reserved by the original Square payment.");
            showSquareMessage(this.dialog, message);
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
        if (this.squarePaymentReserved && (
            this.pos.get_order()?.uuid !== this.currentOrder.uuid ||
            this.pos.mainScreen.component !== this.constructor
        )) {
            return;
        }
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
        const orderUuid = line.pos_order_id.uuid;
        if (squareRequestsInProgress.has(orderUuid)) {
            return;
        }
        squareRequestsInProgress.add(orderUuid);
        try {
            await super.sendPaymentRequest(...arguments);
        } finally {
            squareRequestsInProgress.delete(orderUuid);
        }
    },
    async sendPaymentCancel(line) {
        if (!isSquarePayment(line)) {
            return super.sendPaymentCancel(...arguments);
        }
        const order = line.pos_order_id;
        if (squareCancellationsInProgress.has(order.uuid)) {
            return;
        }
        squareCancellationsInProgress.add(order.uuid);
        for (const tender of order.payment_ids) {
            resumeSquarePolling.get(tender.uuid)?.();
        }
        try {
            const status = await line.payment_method_id.payment_terminal.cancelPayment(line);
            if (
                status?.state === "verified" &&
                this.pos.get_order()?.uuid === line.pos_order_id.uuid &&
                this.currentOrder.is_paid() &&
                this.pos.config.auto_validate_terminal_payment
            ) {
                this.validateOrder(false);
            }
        } finally {
            squareCancellationsInProgress.delete(order.uuid);
        }
    },
    async sendForceDone(line) {
        if (!isSquarePayment(line)) {
            return super.sendForceDone(...arguments);
        }
    },
});

patch(PaymentScreenStatus.prototype, {
    get remainingText() {
        if (this.props.order.squareAuthorizedAmount) {
            return this.env.utils.formatCurrency(this.props.order.get_due());
        }
        return super.remainingText;
    },
});

patch(PaymentScreenPaymentLines.prototype, {
    isReservedSquarePayment,
    squareCancelLabel(line) {
        if (line.square_attempt_state === "canceling") {
            return _t("Continue cancellation");
        }
        return line.square_attempt_state === "settlement_blocked" || line.pos_order_id.squareAuthorizedAmount
            ? _t("Abandon split")
            : _t("Cancel payment");
    },
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
        if (line.square_attempt_state === "canceling") {
            return _t("Waiting for Square to confirm cancellation.");
        }
        if (line.square_attempt_state === "authorized") {
            return _t("Authorized by Square");
        }
        if (line.square_attempt_state === "settling") {
            return _t("Check the original Square settlement before taking another payment.");
        }
        if (line.square_attempt_state === "settlement_blocked") {
            return _t("Square released a card authorization. Abandon the split to take another payment.");
        }
        if (line.square_attempt_state === "unsupported_split") {
            return _t("This Square Terminal cannot authorize a split. Cancel the payment to choose another tender.");
        }
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

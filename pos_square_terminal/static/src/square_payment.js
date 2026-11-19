import { useEffect } from "@odoo/owl";
import { AlertDialog } from "@web/core/confirmation_dialog/confirmation_dialog";
import { _t } from "@web/core/l10n/translation";
import { ConnectionLostError, RPCError } from "@web/core/network/rpc";
import { patch } from "@web/core/utils/patch";
import { PaymentInterface } from "@point_of_sale/app/utils/payment/payment_interface";
import { PosOrder } from "@point_of_sale/app/models/pos_order";
import { PosPayment } from "@point_of_sale/app/models/pos_payment";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { PaymentScreenPaymentLines } from "@point_of_sale/app/screens/payment_screen/payment_lines/payment_lines";
import { PosStore, register_payment_method } from "@point_of_sale/app/services/pos_store";
import OrderPaymentValidation from "@point_of_sale/app/utils/order_payment_validation";

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
        Boolean(line.square_attempt_state) || ["waiting", "done"].includes(line.getPaymentStatus())
    );
}

function isNativeCashMethod(method) {
    return method?.type === "cash" && method.payment_method_type === "none" && !method.use_payment_terminal;
}

function isReservedSquareTender(line) {
    return isReservedSquarePayment(line) || Boolean(line?.square_cash_reserved);
}

function showSquareMessage(dialog, body) {
    dialog.add(AlertDialog, { title: _t("Square Terminal"), body });
}

class SquarePayment extends PaymentInterface {
    get fastPayments() {
        return false;
    }

    async call(line, method, ...args) {
        const order = line.pos_order_id;
        const status = await this.pos.data.call(
            "pos.order", method, [order.uuid, line.uuid, ...args]
        );
        if (status.device_tip_line) {
            const data = await this.pos.data.missingRecursive({ "pos.order.line": [status.device_tip_line] });
            this.pos.models.connectNewData(data);
            order.update({ is_tipped: true, tip_amount: status.device_tip_line.price_subtotal_incl });
            order.triggerRecomputeAllPrices();
        }
        let cancelled = false;
        for (const tender of [...order.payment_ids]) {
            const cash = status.cash_tenders?.[tender.uuid];
            if (cash) {
                if (!tender.transaction_id || cash.payment_id) {
                    tender.update({ square_cash_reserved: cash.reserved, transaction_id: cash.payment_id });
                }
                continue;
            }
            const result = tender.uuid === line.uuid ? status : status.tenders?.[tender.uuid];
            if (!result || (tender.square_attempt_state === "verified" && result.state !== "verified")) {
                continue;
            }
            if (result.state === "cancelled") {
                tender.update({ square_attempt_state: "cancelled" });
                order.removePaymentline(tender);
                cancelled = true;
            } else if (result.state === "busy" || result.state === "unstarted") {
                tender.update({ square_attempt_state: false, square_attempt_reference: false, payment_status: "pending" });
            } else {
                tender.update({ square_attempt_state: result.state, square_attempt_reference: result.reference });
                if (result.state === "verified") {
                    tender.update({ amount: result.amount, transaction_id: result.payment_id, payment_status: "done" });
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
        if (status.state === "recording_cash") {
            status = await this.call(line, "square_record_cash", status.reference);
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
            if (!["verified", "cancelled", "authorized", "recording_cash", "settling"].includes(line.square_attempt_state)) {
                line.update({ square_attempt_state: "uncertain" });
            }
            return false;
        }
    }

    async cancelPayment(line) {
        try {
            return await this.call(line, "square_cancel_payment", line.square_attempt_reference);
        } catch {
            if (!["verified", "cancelled", "authorized", "recording_cash", "settling"].includes(line.square_attempt_state)) {
                line.update({ square_attempt_state: "uncertain" });
            }
        }
    }

    async sendPaymentRequest(uuid) {
        const order = this.pos.getOrder();
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
                    line.setPaymentStatus("pending");
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
                status = await this.call(line, "square_dispatch_payment", status.reference);
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
                    line.setPaymentStatus("pending");
                }
                showSquareMessage(this.env.services.dialog, error.data?.message || error.message);
            } else if (!["verified", "cancelled", "authorized", "recording_cash", "settling"].includes(line.square_attempt_state)) {
                line.update({ square_attempt_state: "uncertain" });
            }
        }
        return false;
    }
}

register_payment_method("square_terminal", SquarePayment);

patch(PosPayment.prototype, {
    setAmount() {
        if (!isReservedSquareTender(this)) {
            return super.setAmount(...arguments);
        }
    },
    isForceDoneAllowed() {
        return !isSquarePayment(this) && super.isForceDoneAllowed(...arguments);
    },
    async pay() {
        if (!isSquarePayment(this)) {
            return super.pay(...arguments);
        }
        if (isReservedSquarePayment(this) && !EXPLICIT_SEND_STATES.includes(this.square_attempt_state)) {
            return this.payment_method_id.payment_terminal.checkPayment(this);
        }
        this.setPaymentStatus("waiting");
        return this.payment_method_id.payment_terminal.sendPaymentRequest(this.uuid);
    },
});

patch(PosOrder.prototype, {
    squareCanAddShare(method) {
        return (method?.use_payment_terminal === "square_terminal" || isNativeCashMethod(method)) &&
            this.payment_ids.every((line) =>
                line.square_attempt_state === "authorized" || isNativeCashMethod(line.payment_method_id)
            ) && this.remainingDue > 0;
    },
    squareCanSettleWithCash() {
        return Boolean(this.squareAuthorizedAmount) &&
            this.payment_ids.every((line) =>
                line.square_attempt_state === "authorized" || isNativeCashMethod(line.payment_method_id)
            ) && this.payment_ids.some((line) => isNativeCashMethod(line.payment_method_id)) && this.remainingDue === 0;
    },
    canBeValidated() {
        return super.canBeValidated(...arguments) || this.squareCanSettleWithCash();
    },
    get squareAuthorizedAmount() {
        return this.payment_ids.reduce(
            (total, line) => total + (line.square_attempt_state === "authorized" ? line.amount : 0), 0
        );
    },
    get remainingDue() {
        const due = super.remainingDue;
        return this.squareAuthorizedAmount
            ? Math.max(this.currency.round(due - this.squareAuthorizedAmount), 0)
            : due;
    },
    isPaid() {
        // An authorization reduces the remaining share without validating the sale.
        return this.squareAuthorizedAmount ? this.currency.isZero(super.remainingDue) : super.isPaid();
    },
    electronicPaymentInProgress() {
        if (!this.squareAuthorizedAmount) {
            return super.electronicPaymentInProgress(...arguments);
        }
        return this.payment_ids.some((line) =>
            line.square_attempt_state !== "authorized" &&
            line.payment_status && !["done", "reversed"].includes(line.payment_status)
        );
    },
    addPaymentline(method) {
        if (this.payment_ids.some(isReservedSquarePayment) && !this.squareCanAddShare(method)) {
            return { status: false, data: _t("The balance is reserved by the original Square payment.") };
        }
        return super.addPaymentline(...arguments);
    },
    removePaymentline(line) {
        if (!isReservedSquareTender(line)) {
            return super.removePaymentline(...arguments);
        }
    },
    setPartner(partner) {
        if (!this.payment_ids.some(isReservedSquarePayment)) {
            return super.setPartner(...arguments);
        }
        this.assertEditable();
        this.partner_id = partner;
        if (partner?.is_company) {
            this.setToInvoice(true);
        }
    },
});

patch(PosStore.prototype, {
    navigate(name, params = {}) {
        const order = params.orderUuid && this.models["pos.order"].getBy("uuid", params.orderUuid);
        if (name === "ProductScreen" && order?.state === "draft" && order.payment_ids.some(isReservedSquarePayment)) {
            return super.navigate("PaymentScreen", { orderUuid: order.uuid });
        }
        return super.navigate(...arguments);
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
                    this.pos.getOrder()?.uuid === order?.uuid &&
                    order?.state === "draft" && order.isPaid() &&
                    order.payment_ids.some((line) => line.square_attempt_state === "verified")
                ) {
                    this.validateOrder(false);
                }
            },
            () => [this.currentOrder?.uuid, this.currentOrder?.getPartner()?.id, this.currentOrder?.to_invoice]
        );
    },
    onMounted() {
        for (const line of this.paymentLines.filter((line) => line.square_attempt_state === "cancelled")) {
            this.currentOrder.removePaymentline(line);
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
    async addTip() {
        if (!this.squarePaymentReserved) {
            return super.addTip(...arguments);
        }
    },
    async addNewPaymentLine(method) {
        if (this.squarePaymentReserved && !this.currentOrder.squareCanAddShare(method)) {
            const message = this.currentOrder.squareAuthorizedAmount &&
                method.use_payment_terminal !== "square_terminal" && !isNativeCashMethod(method)
                ? _t("Use cash or another Square card, or abandon the split to change tender.")
                : _t("The balance is reserved by the original Square payment.");
            showSquareMessage(this.dialog, message);
            return false;
        }
        return super.addNewPaymentLine(...arguments);
    },
    deletePaymentLine(uuid) {
        const line = this.paymentLines.find((line) => line.uuid === uuid);
        if (!isReservedSquareTender(line)) {
            return super.deletePaymentLine(...arguments);
        }
    },
    async validateOrder() {
        if (this.squarePaymentReserved && (
            this.pos.getOrder()?.uuid !== this.currentOrder.uuid ||
            this.pos.router.state.current !== "PaymentScreen"
        )) {
            return;
        }
        if (this.currentOrder.squareCanSettleWithCash()) {
            this.numberBuffer.capture();
            if (!this.currentOrder.squareCanSettleWithCash()) {
                return;
            }
            const order = this.currentOrder;
            const card = order.payment_ids.find((line) => line.square_attempt_state === "authorized");
            try {
                const synced = await this.pos.syncAllOrders({ orders: [order], throw: true });
                const saved = synced?.find((record) => record.uuid === order.uuid && record.state === "draft");
                const savedCard = saved?.payment_ids.find((line) => line.uuid === card.uuid);
                if (!savedCard) {
                    showSquareMessage(this.dialog, _t("Save the original sale before settling with cash."));
                    return;
                }
                if (!await savedCard.payment_method_id.payment_terminal.checkPayment(savedCard)) {
                    this.selectPaymentLine(savedCard.uuid);
                    return;
                }
            } catch (error) {
                showSquareMessage(this.dialog, error.data?.message || _t("Save the original sale before settling with cash."));
                return;
            }
        }
        if (this.paymentLines.some((line) => isSquarePayment(line) && line.square_attempt_state !== "verified")) {
            showSquareMessage(this.dialog, _t("Wait for Square to verify this payment before validating the sale."));
            return;
        }
        return super.validateOrder(...arguments);
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
                this.pos.getOrder()?.uuid === line.pos_order_id.uuid &&
                this.currentOrder.isPaid() &&
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

patch(OrderPaymentValidation.prototype, {
    async shouldHideValidationBehindFeedbackScreen() {
        if (!this.order.payment_ids.some(isReservedSquarePayment)) {
            return super.shouldHideValidationBehindFeedbackScreen(...arguments);
        }
        const nextPage = this.nextPage;
        const ui = this.pos.env.services.ui;
        try {
            ui.block();
            await this.finalizeValidation();
            if (!PAID_STATES.includes(this.order.raw.state)) {
                this.order.state = "draft";
                this.pos.data.localUnsyncedPaidOrderUuids.delete(this.order.uuid);
                return false;
            }
        } finally {
            ui.unblock();
        }
        this.pos.navigate(nextPage.page, nextPage.params);
    },
    handleValidationError(error) {
        if (error instanceof ConnectionLostError && this.order.payment_ids.some(isReservedSquarePayment)) {
            this.order.state = "draft";
            this.pos.data.localUnsyncedPaidOrderUuids.delete(this.order.uuid);
            showSquareMessage(this.pos.dialog, _t("Square verified payment. Validate the original sale once the register is online."));
            return error;
        }
        return super.handleValidationError(...arguments);
    },
    async afterOrderValidation() {
        if (this.order.payment_ids.some(isReservedSquarePayment) && !PAID_STATES.includes(this.order.raw.state)) {
            return;
        }
        return super.afterOrderValidation(...arguments);
    },
});

patch(PaymentScreenPaymentLines.prototype, {
    isReservedSquarePayment,
    isReservedSquareTender,
    squareCancelLabel(line) {
        if (line.square_attempt_state === "canceling") {
            return _t("Continue cancellation");
        }
        return ["settling", "settlement_blocked"].includes(line.square_attempt_state) || line.pos_order_id.squareAuthorizedAmount
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
        if (line.square_attempt_state === "recording_cash") {
            return _t("Check the original cash recording before taking another payment.");
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

import { PaymentForm } from "@payment/interactions/payment_form";
import { loadJS } from "@web/core/assets";
import { _t } from "@web/core/l10n/translation";
import { rpc, RPCError } from "@web/core/network/rpc";
import { patch } from "@web/core/utils/patch";

const SDK_URLS = {
    sandbox: "https://sandbox.web.squarecdn.com/v1/square.js",
    production: "https://web.squarecdn.com/v1/square.js",
};

patch(PaymentForm.prototype, {
    setup() {
        super.setup(...arguments);
        this.squareOnlineCards = new Map();
    },

    async _prepareInlineForm(providerId, providerCode, paymentOptionId, paymentMethodCode, flow) {
        if (providerCode !== "square_online") {
            return super._prepareInlineForm(...arguments);
        }
        this._setPaymentFlow("direct");
        const radio = this.el.querySelector('input[name="o_payment_radio"]:checked');
        const inlineForm = radio && this._getInlineForm(radio);
        const container = inlineForm?.querySelector("[data-square-online-cart]");
        if (!container || this.squareOnlineCards.has(providerId)) {
            return;
        }
        inlineForm.classList.remove("d-none");
        try {
            await this.waitFor(loadJS(SDK_URLS[container.dataset.environment]));
            const payments = window.Square.payments(
                container.dataset.applicationId, container.dataset.locationId
            );
            const card = await this.waitFor(payments.card());
            await this.waitFor(card.attach(`#${container.id}`));
            this.squareOnlineCards.set(providerId, { card, container });
            this.registerCleanup(() => card.destroy().catch(() => {}));
        } catch {
            container.textContent = _t("The Square card form could not load. Choose another payment method or reload the page.");
        }
    },

    _prepareTransactionRouteParams() {
        const params = super._prepareTransactionRouteParams(...arguments);
        if (this.paymentContext.providerCode === "square_online") {
            params.square_online_cart = this.squareOnlineCards.get(params.provider_id)
                ?.container.dataset.squareOnlineCart;
        }
        return params;
    },

    async _initiatePaymentFlow(providerCode, paymentOptionId, paymentMethodCode, flow) {
        if (
            providerCode === "square_online"
            && !this.squareOnlineCards.has(this.paymentContext.providerId)
        ) {
            this._displayErrorDialog(
                _t("Square card form unavailable"),
                _t("Reload the page or choose another payment method.")
            );
            this._enableButton();
            return;
        }
        return super._initiatePaymentFlow(...arguments);
    },

    async _processDirectFlow(providerCode, paymentOptionId, paymentMethodCode, values) {
        if (providerCode !== "square_online") {
            return super._processDirectFlow(...arguments);
        }
        const { card, container } = this.squareOnlineCards.get(values.provider_id);
        let tokenResult;
        try {
            if (
                container.dataset.applicationId !== values.square_online_application_id
                || container.dataset.locationId !== values.square_online_location_id
            ) {
                this._displayErrorDialog(
                    _t("Checkout changed"), _t("Reload the page to pay with Square.")
                );
                this._enableButton();
                return;
            }
            tokenResult = await this.waitFor(card.tokenize(values.square_online_verification));
        } catch {
            tokenResult = null;
        }
        if (tokenResult?.status !== "OK" || !tokenResult.token) {
            this._displayErrorDialog(
                _t("Card verification was unsuccessful"),
                _t("Check your card details and complete any verification requested by your bank.")
            );
            this._enableButton();
            return;
        }

        try {
            const result = await this.waitFor(rpc("/payment/square_online/submit", {
                transaction_id: values.square_online_transaction_id,
                access_token: values.square_online_access_token,
                source_id: tokenResult.token,
            }));
            if (result.state === "error" || result.state === "cancel") {
                this._displayErrorDialog(_t("Square payment unsuccessful"), result.state_message);
                this._enableButton();
                return;
            }
        } catch (error) {
            if (error instanceof RPCError && error.data?.name === "odoo.exceptions.ValidationError") {
                this._displayErrorDialog(_t("Square payment unsuccessful"), error.data.message);
                this._enableButton();
                return;
            }
            // The payment may have been accepted even though this tab lost its response.
        }
        window.location.assign("/payment/status");
    },
});

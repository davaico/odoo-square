import { loadJS } from '@web/core/assets';
import { _t } from '@web/core/l10n/translation';
import { rpc, RPCError } from '@web/core/network/rpc';
import paymentForm from '@payment/js/payment_form';

const squareSDK = {
    sandbox: 'https://sandbox.web.squarecdn.com/v1/square.js',
    production: 'https://web.squarecdn.com/v1/square.js',
};

paymentForm.include({
    async _prepareInlineForm(providerId, providerCode, paymentOptionId, paymentMethodCode, flow) {
        if (providerCode !== 'square_online') {
            return this._super(...arguments);
        }
        this._setPaymentFlow('direct');
        const container = this.el.querySelector(`#o_square_card_${providerId}`);
        const environment = container.dataset.squareEnvironment;
        this.squareCards ??= new Map();
        if (!this.squareCards.has(providerId)) {
            this.squareCards.set(providerId, (async () => {
                if (!window.isSecureContext) {
                    throw new Error('Square requires a secure context');
                }
                await loadJS(squareSDK[environment]);
                const payments = window.Square.payments(
                    container.dataset.squareApplicationId, container.dataset.squareLocationId
                );
                const card = await payments.card();
                await card.attach(`#${container.id}`);
                return card;
            })());
        }
        try {
            await this.squareCards.get(providerId);
        } catch {
            this.squareCards.delete(providerId);
            this._displayErrorDialog(
                _t("Cannot display the Square card form"),
                _t("Refresh the page or select a different payment method.")
            );
        }
    },

    async _initiatePaymentFlow(providerCode, paymentOptionId, paymentMethodCode, flow) {
        if (providerCode !== 'square_online') {
            return this._super(...arguments);
        }
        const providerId = this.paymentContext.providerId;
        const container = this.el.querySelector(`#o_square_card_${providerId}`);
        const card = await this.squareCards?.get(providerId);
        if (!card) {
            this._displayErrorDialog(_t("Refresh checkout to load the Square card form."));
            this._enableButton();
            return;
        }

        let values;
        try {
            values = await rpc(this.paymentContext.transactionRoute, {
                ...this._prepareTransactionRouteParams(),
                square_cart_signature: container.dataset.squareCartSignature,
            });
        } catch (error) {
            this._displayErrorDialog(
                _t("Cannot start the payment"),
                error instanceof RPCError ? error.data.message : _t("Refresh checkout and try again.")
            );
            this._enableButton();
            return;
        }
        if (
            values.square_application_id !== container.dataset.squareApplicationId
            || values.square_location_id !== container.dataset.squareLocationId
        ) {
            this._displayErrorDialog(_t("The Square configuration has changed. Refresh checkout and try again."));
            this._enableButton();
            return;
        }

        // Keep the pay button disabled but let the customer interact with Square's authentication.
        this.call('ui', 'unblock');
        let tokenResult;
        try {
            tokenResult = await card.tokenize(values.square_verification);
        } catch {
            // Square did not produce a usable card token.
        }
        if (tokenResult?.status !== 'OK' || !tokenResult.token) {
            this._displayErrorDialog(
                _t("Square could not verify your card"),
                _t("Check your card details and complete any bank verification, then try again.")
            );
            this._enableButton(false);
            return;
        }

        this.call('ui', 'block');
        let result;
        try {
            result = await rpc('/payment/square/online/submit', {
                ...values.square_submission,
                source_id: tokenResult.token,
            });
        } catch (error) {
            if (error instanceof RPCError && error.data.name === 'odoo.exceptions.ValidationError') {
                this._displayErrorDialog(_t("Cannot submit this payment"), error.data.message);
                this._enableButton();
                return;
            }
            // When the request might already have reached Square, use Odoo's status page.
            window.location = '/payment/status';
            return;
        }
        if (result.state === 'error' || result.state === 'cancel') {
            this._displayErrorDialog(_t("Payment unsuccessful"), result.message);
            this._enableButton();
            return;
        }
        window.location = '/payment/status';
    },
});

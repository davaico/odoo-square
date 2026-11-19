import { AlertDialog } from "@web/core/confirmation_dialog/confirmation_dialog";
import { patch } from "@web/core/utils/patch";
import { ClosePosPopup } from "@point_of_sale/app/navbar/closing_popup/closing_popup";

patch(ClosePosPopup.prototype, {
    async handleClosingError(response) {
        if (response.square_payment_blocked) {
            this.props.close();
            this.dialog.add(AlertDialog, { title: response.title, body: response.message });
            return;
        }
        return super.handleClosingError(...arguments);
    },
});

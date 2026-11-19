from decimal import Decimal
from math import isfinite
from uuid import uuid4

from psycopg2.errors import UniqueViolation

from odoo import Command, _, api, models
from odoo.exceptions import AccessError, UserError
from odoo.tools.float_utils import float_compare, float_round

from ..square_api import SquareAPIError


PAID_STATES = ('paid', 'done', 'invoiced')


def minor_units(amount, digits):
    value = Decimal(str(amount)).scaleb(digits)
    if not value.is_finite() or value != value.to_integral_value():
        raise UserError(_("The Odoo amount cannot be represented in the Square location currency."))
    return int(value)


def decimal_string(amount):
    return format(Decimal(str(amount)).normalize(), 'f')


def same_value(record, field, value):
    current = record[field]
    field_type = record._fields[field].type
    if field_type == 'many2one':
        return current.id == value
    if field_type == 'many2many':
        ids = set(current.ids)
        return all(
            (command[0] == Command.LINK and command[1] in ids)
            or (command[0] == Command.SET and set(command[2]) == ids)
            or (command[0] == Command.CLEAR and not ids)
            for command in value
        )
    if field_type == 'one2many':
        return not value
    return (current or False) == (value or False)


class PosOrder(models.Model):
    _inherit = 'pos.order'

    _square_frozen_fields = {
        'amount_total', 'amount_tax', 'amount_return', 'session_id', 'company_id',
        'pricelist_id', 'fiscal_position_id', 'currency_id', 'config_id', 'uuid', 'pos_reference',
        'shipping_date', 'tip_amount', 'is_tipped',
    }

    @api.model
    def square_prepare_payment(self, order_uuid, payment_uuid):
        order, payment = self._square_checkout_records(order_uuid, payment_uuid)
        order.session_id._square_lock_admission(touch=True)
        order._square_lock_checkout(touch=True)
        attempt = order._square_attempts().filtered(lambda candidate: candidate.payment_uuid == payment.uuid)[-1:]
        if attempt:
            return attempt._cashier_status()
        attempts = order._square_current_attempts()
        order._square_check_session()
        if (
            order.state != 'draft' or payment.payment_status != 'waiting'
            or payment.payment_method_id.use_payment_terminal != 'square_terminal'
            or payment in attempts.payment_id
        ):
            raise UserError(_("Send one unpaid Square card share at a time."))

        method = payment.payment_method_id
        if not method.active or method not in order.config_id.payment_method_ids or method.company_id != order.company_id:
            raise UserError(_("This Square payment method is unavailable for this register."))
        reason = method._square_readiness_error(order.config_id)
        if reason:
            raise UserError(reason)
        terminal = method.sudo().square_terminal_id

        connection = terminal.connection_id
        link = attempts.square_order_link_id
        currency = terminal.location_id.currency_code
        digits = 0 if currency == 'JPY' else 2
        other_tenders = order.payment_ids - attempts.payment_id - link._cash_payment_lines() - payment
        cash = order._square_cash_tenders(other_tenders, digits)
        cash_minor = sum(minor_units(line.amount, digits) for line in cash)
        device_key = attempts._device_key(connection.environment, connection.merchant_id, terminal.device_id)
        if attempts.search_count([('active_device_key', '=', device_key)], limit=1):
            return {'state': 'busy'}

        amount_minor = minor_units(payment.amount, digits)
        if link:
            order._square_check_new_share(payment, attempts)
            if (
                connection.id != link.connection_id.id or terminal.location_id.id != link.location_id.id
                or currency != link.currency_code or terminal.location_id.square_id != link.square_location_id
            ):
                raise UserError(_("Use a Square Terminal at the original merchant and location for this split."))
            for attempt in attempts:
                attempt._check_provider_scope()
        else:
            total_minor, line_items, tip_minor = order._square_priced_basket(digits)
            if tip_minor:
                raise UserError(_("Square Terminal cannot send an Odoo-entered tip with this payment."))
            if total_minor > 2_147_483_647 or not 0 < amount_minor <= total_minor - cash_minor or payment.is_change:
                raise UserError(_("Enter a Square card share within the unpaid Odoo order total."))

        try:
            with self.env.cr.savepoint():
                if not link:
                    link = order._square_prepare_order(
                        terminal.location_id, digits, total_minor, line_items, tip_minor,
                        split=amount_minor < total_minor,
                    )
                link._reserve_cash_tenders(cash)
                attempt_reference = 'odoo-' + uuid4().hex
                checkout = {
                    'reference_id': attempt_reference,
                    'amount_money': {'amount': amount_minor, 'currency': currency},
                    'payment_options': {'autocomplete': not link.split},
                    'device_options': {
                        'device_id': terminal.device_id,
                        'show_itemized_cart': True,
                        'allow_auto_card_surcharge': False,
                        'tip_settings': {'allow_tipping': False},
                    },
                }
                if link.square_order_id:
                    checkout['order_id'] = link.square_order_id
                attempt = attempts.create({
                    'square_order_link_id': link.id,
                    'payment_id': payment.id,
                    'payment_uuid': payment.uuid,
                    'payment_method_id': method.id,
                    'terminal_id': terminal.id,
                    'square_device_id': terminal.device_id,
                    'application_id': connection.application_id,
                    'reference': attempt_reference,
                    'amount_minor': amount_minor,
                    'state': 'blocked' if link.pricing_issue else 'ready' if link.square_order_id else 'prepared',
                    'checkout_request': {'idempotency_key': str(uuid4()), 'checkout': checkout},
                })
                attempt.flush_recordset(['active_device_key'])
        except UniqueViolation as error:
            if error.diag.constraint_name != 'pos_square_payment_attempt_active_device_unique':
                raise
            return {'state': 'busy'}
        return attempt._cashier_status()

    def _square_prepare_order(self, location, digits, total_minor, line_items, tip_minor, *, split):
        self.ensure_one()
        connection = location.connection_id
        reference = 'odoo-' + uuid4().hex
        try:
            create_request, pricing_snapshot = self._square_order_payload(location, reference, line_items)
        except SquareAPIError:
            raise UserError(_("Square could not check the catalog. Try again before sending the payment.")) from None
        link = self.env['pos.square.order'].sudo().create({
            'order_id': self.id,
            'order_uuid': self.uuid,
            'company_id': self.company_id.id,
            'connection_id': connection.id,
            'environment': connection.environment,
            'merchant_id': connection.merchant_id,
            'location_id': location.id,
            'square_location_id': location.square_id,
            'reference': reference,
            'currency_code': location.currency_code,
            'currency_digits': digits,
            'total_minor': total_minor,
            'tip_minor': tip_minor,
            'create_request': create_request,
            'pricing_snapshot': pricing_snapshot,
            'split': split,
        })
        try:
            link._preview_square_order()
        except SquareAPIError:
            raise UserError(_("Square could not check the order pricing. Try again before sending the payment.")) from None
        return link

    @api.model
    def square_prepare_linked_order(self, order_uuid, payment_uuid):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if not attempt.square_order_link_id.cancel_requested:
            attempt._create_square_order()
        return attempt._cashier_status()

    @api.model
    def square_mark_dispatching(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if attempt.reference == attempt_reference and attempt._cashier_state() == 'ready':
            attempt.state = 'dispatching'
        return attempt._cashier_status()

    @api.model
    def square_dispatch_payment(self, order_uuid, payment_uuid, attempt_reference=None):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if attempt.reference == attempt_reference and attempt._cashier_state() == 'dispatching':
            attempt._dispatch()
        return attempt._cashier_status()

    @api.model
    def square_retry_payment(self, order_uuid, payment_uuid, attempt_reference):
        order, payment = self._square_checkout_records(order_uuid, payment_uuid)
        order._square_lock_checkout(touch=True)
        previous = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if previous.reference != attempt_reference or previous._cashier_state() != 'failed':
            return previous._cashier_status()

        order._square_check_session()
        attempts = order._square_current_attempts()
        order._square_check_reserved_amounts(attempts)
        if any(attempt.state != 'authorized' for attempt in attempts - previous):
            raise UserError(_("Check the other Square card share before retrying this payment."))
        method = payment.payment_method_id
        if (
            not method.active or method not in order.config_id.payment_method_ids
            or method.sudo().square_terminal_id.id != previous.terminal_id.id
            or previous.terminal_id.device_id != previous.square_device_id
            or previous.terminal_id.connection_id != previous.connection_id
            or previous.terminal_id.location_id != previous.square_order_link_id.location_id
        ):
            raise UserError(_("Restore the original Square Terminal configuration before retrying."))
        reason = method._square_readiness_error(order.config_id)
        if reason:
            raise UserError(reason)
        previous._check_provider_scope()
        device_key = previous._device_key(previous.environment, previous.merchant_id, previous.square_device_id)
        if previous.search_count([('active_device_key', '=', device_key)], limit=1):
            return {**previous._cashier_status(), 'busy': True}

        reference = 'odoo-' + uuid4().hex
        request = {
            **previous.checkout_request,
            'idempotency_key': str(uuid4()),
            'checkout': {**previous.checkout_request['checkout'], 'reference_id': reference},
        }
        try:
            with self.env.cr.savepoint():
                attempt = previous.create({
                    'square_order_link_id': previous.square_order_link_id.id,
                    'payment_id': payment.id,
                    'payment_uuid': payment.uuid,
                    'payment_method_id': method.id,
                    'terminal_id': previous.terminal_id.id,
                    'square_device_id': previous.square_device_id,
                    'application_id': previous.application_id,
                    'reference': reference,
                    'amount_minor': previous.amount_minor,
                    'checkout_request': request,
                    'state': 'ready',
                })
                attempt.flush_recordset(['active_device_key'])
        except UniqueViolation as error:
            if error.diag.constraint_name != 'pos_square_payment_attempt_active_device_unique':
                raise
            return {**previous._cashier_status(), 'busy': True}
        return attempt._cashier_status()

    @api.model
    def square_check_payment(self, order_uuid, payment_uuid):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=False, allow_missing=True)
        if not attempt:
            return {'state': 'unstarted'}
        link = attempt.square_order_link_id
        if attempt._cashier_state() == 'canceling':
            attempt.order_id._square_finish_cancellation(attempt)
        elif not link.settlement_request:
            attempt._refresh()
        return attempt._cashier_status()

    @api.model
    def square_prepare_settlement(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=False)
        link = attempt.square_order_link_id
        if (
            attempt.reference == attempt_reference and attempt.state == 'authorized'
            and link.split and not link.cancel_requested and not link.settlement_request
            and not link.cash_recording
        ):
            order = attempt.order_id.with_user(self.env.user)
            attempts = order._square_current_attempts()
            if all(share.state == 'authorized' for share in attempts):
                pending = order.payment_ids - attempts.payment_id - link._cash_payment_lines()
                if any(line.payment_method_id.use_payment_terminal == 'square_terminal' for line in pending):
                    return attempt._cashier_status()
                cash = order._square_cash_tenders(pending, link.currency_digits)
                covered = link._cash_total_minor() + sum(attempts.mapped('amount_minor'))
                added_cash = sum(minor_units(line.amount, link.currency_digits) for line in cash)
                if covered + added_cash == link.total_minor + link.tip_minor:
                    order._square_check_session()
                    link._reserve_cash_tenders(cash)
                    order._square_check_reserved_amounts(attempts)
                    if link.cash_tenders:
                        # Commit the recording intent before issuing any Cash CreatePayment.
                        link.cash_recording = True
                    else:
                        link._prepare_settlement(attempts)
        return attempt._cashier_status()

    @api.model
    def square_record_cash(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=False)
        if attempt.reference == attempt_reference and attempt._cashier_state() == 'recording_cash':
            order = attempt.order_id.with_user(self.env.user)
            attempts = order._square_current_attempts()
            order._square_check_session()
            order._square_check_reserved_amounts(attempts)
            link = attempt.square_order_link_id
            if all(share.state == 'authorized' for share in attempts) and link._record_cash_tenders(attempt):
                link._prepare_settlement(attempts)
        return attempt._cashier_status()

    @api.model
    def square_settle_payment(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=False)
        link = attempt.square_order_link_id
        state = attempt._cashier_state()
        if attempt.reference == attempt_reference and state in ('settling', 'settlement_blocked'):
            order = attempt.order_id.with_user(self.env.user)
            order._square_check_session()
            attempts = order._square_current_attempts()
            order._square_check_reserved_amounts(attempts)
            link._settle(attempts, read_only=state == 'settlement_blocked')
        return attempt._cashier_status()

    @api.model
    def square_cancel_payment(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if attempt.reference == attempt_reference and attempt._cashier_state() not in ('verified', 'cancelled'):
            order = attempt.order_id.with_user(self.env.user)
            order._square_check_session()
            link = attempt.square_order_link_id
            if link.settlement_request and not link.cancel_requested:
                if not link._settle(order._square_current_attempts(), read_only=True):
                    return attempt._cashier_status()
            link.cancel_requested = True
            order._square_finish_cancellation(attempt, request=True)
        return attempt._cashier_status()

    def _square_finish_cancellation(self, attempt, *, request=False):
        link = attempt.square_order_link_id
        related_attempts = self._square_reserved_attempts().filtered(
            lambda candidate: candidate.square_order_link_id == link,
        )
        for share in related_attempts:
            if request:
                share._cancel_checkout()
            else:
                share._refresh(recheck_failed=True)
        if any(candidate.state not in ('failed', 'blocked') for candidate in related_attempts):
            return
        try:
            attempt._check_provider_scope()
        except UserError:
            return
        if not link._release_cash_tenders(request=request):
            return
        released_payment_ids = related_attempts.filtered('square_payment_id').mapped('square_payment_id')
        released_payment_ids.extend(filter(None, link._cash_square_payment_ids()))
        link._cancel_unpaid_order(released_payment_ids, request=request)
        if link.cancelled:
            self._square_lock_checkout(touch=True)

    @api.model
    def _square_checkout_records(self, order_uuid, payment_uuid):
        if not self.env.user.has_group('point_of_sale.group_pos_user'):
            raise AccessError(_("Sign in as a Point of Sale user."))
        if not all(isinstance(value, str) and 1 <= len(value) <= 64 for value in (order_uuid, payment_uuid)):
            raise UserError(_("Invalid Point of Sale order or payment reference."))
        order = self.search([('uuid', '=', order_uuid)], limit=1)
        if not order or order.company_id not in self.env.companies:
            raise AccessError(_("This Point of Sale order is unavailable."))
        order.check_access('write')
        payment = order.payment_ids.filtered(lambda line: line.uuid == payment_uuid)
        if not payment:
            raise UserError(_("Save the Point of Sale payment before sending it to Square Terminal."))
        payment.check_access('write')
        return order, payment

    @api.model
    def _square_requested_attempt(self, order_uuid, payment_uuid, *, must_be_open, allow_missing=False):
        order, payment = self._square_checkout_records(order_uuid, payment_uuid)
        order._square_lock_checkout(touch=True)
        attempt = order._square_attempts().filtered(lambda candidate: (
            candidate.payment_id.id == payment.id and candidate.payment_uuid == payment.uuid
        ))[-1:]
        if not attempt and allow_missing and payment.payment_method_id.use_payment_terminal == 'square_terminal':
            return attempt
        if len(attempt) != 1:
            raise UserError(_("Square Terminal has no reserved attempt for this payment."))
        self.env.cr.execute(
            'SELECT id FROM pos_square_payment_attempt WHERE id = %s FOR NO KEY UPDATE', [attempt.id],
        )
        if must_be_open and attempt._cashier_state() in ('prepared', 'ready', 'dispatching'):
            order._square_check_session()
            order._square_check_reserved_amounts(order._square_current_attempts())
        return attempt

    def _square_check_session(self):
        self.ensure_one()
        session = self.session_id
        session.check_access('read')
        self.config_id.check_access('read')
        if (
            session.state != 'opened' or session.company_id != self.company_id
            or session.config_id != self.config_id or self.config_id.current_session_id != session
        ):
            raise UserError(_("Return to the original open register session before sending a Square payment."))

    def _square_priced_basket(self, digits):
        self.ensure_one()
        config = self.config_id
        allowed_positions = config.default_fiscal_position_id | config.fiscal_position_ids
        allowed_pricelists = config._get_available_pricelists()
        pricelist = self.pricelist_id if config.use_pricelist else self.env['product.pricelist']
        if (
            not self.lines or self.amount_return > 0 or self.shipping_date
            or (config.cash_rounding and not config.only_round_cash_method)
            or (config.use_pricelist and pricelist not in allowed_pricelists)
            or (not config.use_pricelist and self.pricelist_id and self.pricelist_id != config.pricelist_id)
            or (self.fiscal_position_id and self.fiscal_position_id not in allowed_positions)
        ):
            raise UserError(_("This sale configuration is unavailable for Square Terminal."))
        client_total = minor_units(self.amount_total, digits)
        client_tax = minor_units(self.amount_tax, digits)
        currency = self.currency_id
        account_tax = self.env['account.tax']
        price_digits = self.env['decimal.precision'].precision_get('Product Price')
        pricelist = pricelist.with_company(self.company_id)
        tip_lines = self.lines.filtered(lambda line: line.product_id == config.tip_product_id)
        tip_minor = minor_units(self.tip_amount, digits)
        if tip_minor or tip_lines:
            if (
                tip_minor < 0 or not self.is_tipped or len(tip_lines) != 1
                or tip_lines.qty != 1 or tip_lines.discount or tip_lines.tax_ids
                or tip_lines.combo_parent_id or tip_lines.combo_line_ids
                or minor_units(tip_lines.price_unit, digits) != tip_minor
            ):
                raise UserError(_("The Odoo tip differs from the recorded tip line."))
        combo_prices = self._square_combo_prices(pricelist, digits)
        unit_prices = {}
        for line in self.lines:
            product = line.product_id
            taxes = product.taxes_id._filter_taxes_by_company(self.company_id)
            if (
                not all(isfinite(value) for value in (line.qty, line.price_unit, line.discount))
                or line.qty <= 0 or line.price_unit < 0 or not 0 <= line.discount <= 100
                or (line.discount and not config.manual_discount)
                or (line.price_type == 'manual' and line not in tip_lines) or line.price_extra
                or line.refunded_orderline_id or (line.combo_item_id and line not in combo_prices)
                or (line.combo_parent_id and line not in combo_prices)
                or product.tracking != 'none'
                or not product.active or not product.sale_ok
                or (not product.available_in_pos and not line.combo_parent_id and line not in tip_lines)
                or (product.company_id and product.company_id != self.company_id)
                or set(taxes.ids) != set(line.tax_ids.ids)
            ):
                raise UserError(_("An Odoo basket item cannot be priced for Square Terminal."))
            if line in tip_lines:
                price = self.tip_amount
            elif line in combo_prices:
                price = combo_prices[line]
            else:
                price = pricelist._get_product_price(product, line.qty) if pricelist else product.lst_price
            if float_compare(price, line.price_unit, precision_digits=price_digits):
                raise UserError(_("The Odoo basket price changed. Reload the register and check the sale."))
            unit_prices[line] = float_round(price, precision_digits=price_digits)

        base_lines = self.lines._prepare_tax_base_line_values()
        for line, base in zip(self.lines, base_lines):
            base['price_unit'] = unit_prices[line]
        account_tax._add_tax_details_in_base_lines(base_lines, self.company_id)
        for line, base in zip(self.lines, base_lines):
            account_tax._round_base_lines_tax_details([base], self.company_id)
            details = base['tax_details']
            if (
                currency.compare_amounts(line.price_subtotal, details['total_excluded_currency'])
                or currency.compare_amounts(line.price_subtotal_incl, details['total_included_currency'])
            ):
                raise UserError(_("The Odoo basket price changed. Reload the register and check the sale."))

        account_tax._round_base_lines_tax_details(base_lines, self.company_id)
        line_items = []
        for line, base in zip(self.lines, base_lines):
            if line in tip_lines:
                continue
            details = base['tax_details']
            net_minor = minor_units(currency.round(
                details['total_excluded_currency'] + details['delta_total_excluded_currency'],
            ), digits)
            taxes = [
                (tax_data['tax'], minor_units(tax_data['tax_amount_currency'], digits))
                for tax_data in details['taxes_data']
            ]
            tax_minor = sum(amount for _tax, amount in taxes)
            unit_price = unit_prices[line]
            base_before_discount = minor_units(currency.round(unit_price * line.qty), digits)
            base_after_discount = minor_units(
                currency.round(unit_price * line.qty * (1 - line.discount / 100)), digits,
            )
            unit_minor = Decimal(str(unit_price)).scaleb(digits)
            square_taxes = [{
                'uid': f'tax-{tax.id}',
                'name': tax.name[:255],
                'scope': 'LINE_ITEM',
                'type': 'INCLUSIVE' if tax.price_include else 'ADDITIVE',
                'percentage': decimal_string(tax.amount),
            } for tax, amount in taxes if amount]
            line_items.append({
                'product': line.product_id,
                'name': line.product_id.with_context(display_default_code=False).display_name[:255],
                'combo': line in combo_prices,
                'quantity': decimal_string(line.qty),
                'unit_minor': int(unit_minor) if unit_minor == unit_minor.to_integral_value() else None,
                'total_minor': net_minor + tax_minor,
                'taxes': square_taxes,
                'inclusive_tax_minor': sum(amount for tax, amount in taxes if tax.price_include),
                'summary_tax': any(
                    tax.amount_type != 'percent' or tax.amount < 0 or tax.include_base_amount
                    or len(decimal_string(tax.amount)) > 10
                    for tax, amount in taxes if amount
                ),
                'tax_minor': tax_minor,
                'discount_minor': base_before_discount - base_after_discount,
            })
        expected_total = sum(line['total_minor'] for line in line_items)
        expected_tax = sum(line['tax_minor'] for line in line_items)
        self.with_context(backend_recomputation=True)._compute_prices()
        if (
            client_tax != expected_tax or client_total != expected_total + tip_minor
            or minor_units(self.amount_tax, digits) != expected_tax
            or minor_units(self.amount_total, digits) != expected_total + tip_minor
        ):
            raise UserError(_("The calculated Odoo amounts differ from the displayed basket."))
        return expected_total, line_items, tip_minor

    def _square_combo_prices(self, pricelist, digits):
        prices = {}
        for parent in self.lines.filtered(lambda line: line.product_id.type == 'combo'):
            groups = parent.product_id.combo_ids
            children = parent.combo_line_ids.sorted('id')
            if (
                parent.combo_parent_id or not groups or len(children) != len(groups)
                or set(children.combo_item_id.combo_id.ids) != set(groups.ids)
                or any(
                    child not in self.lines or child.product_id != child.combo_item_id.product_id
                    or child.qty != parent.qty or child.attribute_value_ids
                    for child in children
                )
            ):
                raise UserError(_("The Odoo combo no longer matches its configured choices."))
            prices[parent] = 0
            product = parent.product_id
            price = pricelist._get_product_price(product, 1) if pricelist else product.lst_price
            weight = sum(groups.mapped('base_price'))
            remaining = price
            for child in children:
                item = child.combo_item_id
                if child == children[-1]:
                    share = remaining
                else:
                    share = float_round(item.combo_id.base_price * price / weight, precision_digits=digits) if weight else 0
                remaining -= share
                prices[child] = share + item.extra_price
        return prices

    def _square_order_payload(self, location, reference, line_items):
        self.ensure_one()
        summary_reason = self._square_summary_reason(line_items)
        if summary_reason:
            line_items = self._square_receipt_summary(line_items)
        currency = self.currency_id.name
        order = {
            'location_id': location.square_id,
            'reference_id': reference,
            'state': 'OPEN',
            'pricing_options': {'auto_apply_discounts': False, 'auto_apply_taxes': False},
            'line_items': [],
            'taxes': [],
            'discounts': [],
        }
        expected = {}
        variations = {}
        units = {}
        for index, line in enumerate(line_items, 1):
            product = line['product']
            sku = product.default_code if product else None
            quantity = line['quantity']
            precision = len(quantity.partition('.')[2])
            if sku and sku not in variations:
                variations[sku] = location._square_catalog_variation(sku)
            variation = variations.get(sku)
            uid = f'line-{index}'
            payload = {
                'uid': uid,
                'quantity': quantity,
                'base_price_money': {'amount': line['unit_minor'], 'currency': currency},
            }
            catalog_unit = None
            if precision:
                if variation:
                    if variation['id'] not in units:
                        units[variation['id']] = location._square_catalog_quantity_unit(variation)
                    catalog_unit = units[variation['id']]
                    if not catalog_unit or precision > catalog_unit['precision']:
                        variation = catalog_unit = None
                if not variation:
                    name = product.uom_id.name
                    payload['quantity_unit'] = {
                        'measurement_unit': {'custom_unit': {'name': name[:50], 'abbreviation': name[:10]}},
                        'precision': precision,
                    }
            if variation:
                payload['catalog_object_id'] = variation['id']
                if variation.get('version'):
                    payload['catalog_version'] = variation['version']
            else:
                payload['name'] = line['name']
            for tax in line['taxes']:
                tax_uid = tax['uid']
                if tax not in order['taxes']:
                    order['taxes'].append(tax)
                payload.setdefault('applied_taxes', []).append({'tax_uid': tax_uid})
            if line['discount_minor']:
                discount_uid = f'discount-{index}'
                order['discounts'].append({
                    'uid': discount_uid, 'name': _("Odoo line discount"), 'scope': 'LINE_ITEM',
                    'type': 'FIXED_AMOUNT',
                    'amount_money': {'amount': line['discount_minor'], 'currency': currency},
                })
                payload['applied_discounts'] = [{'discount_uid': discount_uid}]
            order['line_items'].append(payload)
            expected[uid] = {
                'total_money': line['total_minor'],
                'total_tax_money': line['tax_minor'],
                'total_discount_money': line['discount_minor'],
            }
            if catalog_unit:
                expected[uid]['quantity_unit'] = catalog_unit
        return {'idempotency_key': str(uuid4()), 'order': order}, {
            'lines': expected,
            'summary_reason': summary_reason,
        }

    def _square_summary_reason(self, line_items):
        if any(line['combo'] for line in line_items):
            return 'combo'
        if any(
            line['unit_minor'] is None or len(line['quantity']) > 12
            or len(line['quantity'].partition('.')[2]) > 5
            for line in line_items
        ):
            return 'precision'
        if any(line['summary_tax'] for line in line_items):
            return 'tax'
        return False

    def _square_receipt_summary(self, line_items):
        combo_names = list(dict.fromkeys(line['name'] for line in line_items if line['product'].type == 'combo'))
        name = _("Odoo receipt %(receipt)s — %(description)s",
                 receipt=self.pos_reference or self.name, description=', '.join(combo_names) or _("Basket"))[:255]
        untaxed = {
            'product': None, 'name': name, 'quantity': '1', 'unit_minor': 0,
            'total_minor': 0, 'tax_minor': 0, 'discount_minor': 0, 'taxes': [],
        }
        summary = []
        for index, line in enumerate(line_items, 1):
            total, tax, discount = line['total_minor'], line['tax_minor'], line['discount_minor']
            if not tax:
                untaxed['unit_minor'] += total + discount
                untaxed['total_minor'] += total
                untaxed['discount_minor'] += discount
                continue
            taxes = line['taxes']
            tax_included = line['inclusive_tax_minor']
            if line['summary_tax']:
                # Square takes percentages, so report a single effective receipt tax for this amount.
                net = total - tax
                if net <= 0 or tax < 0:
                    raise UserError(_("The Odoo receipt tax cannot be represented on a Square order."))
                percentage = decimal_string(round(100 * Decimal(tax) / net, 7))
                if len(percentage) > 10:
                    percentage = percentage[:10].rstrip('.')
                tax_included = tax if tax_included == tax else 0
                taxes = [{
                    'uid': f'receipt-tax-{index}', 'name': _("Odoo receipt tax"),
                    'scope': 'LINE_ITEM', 'type': 'INCLUSIVE' if tax_included else 'ADDITIVE',
                    'percentage': percentage,
                }]
            summary.append({
                'product': None, 'name': name, 'quantity': '1',
                'unit_minor': total - tax + tax_included + discount,
                'total_minor': total, 'tax_minor': tax, 'discount_minor': discount, 'taxes': taxes,
            })
        if untaxed['unit_minor']:
            summary.append(untaxed)
        return summary

    def _square_attempts(self):
        return self.env['pos.square.payment.attempt'].sudo().search([('order_id', 'in', self.ids)], order='id')

    def _square_reserved_attempts(self):
        return self._square_attempts().filtered(lambda attempt: not attempt.square_order_link_id.cancelled)

    def _square_lock_checkout(self, *, touch=False):
        if not self.ids:
            return
        # A common row for admission and ORM writes to the order or any financial child.
        self.env.cr.execute(
            'SELECT id FROM pos_order WHERE id IN %s ORDER BY id FOR NO KEY UPDATE', [tuple(self.ids)],
        )
        if touch:
            # Force PostgreSQL to retry transactions that began before a competing child change.
            self.env.cr.execute('UPDATE pos_order SET write_date = write_date WHERE id IN %s', [tuple(self.ids)])

    def _square_current_attempts(self):
        self.ensure_one()
        by_payment = {attempt.payment_id.id: attempt.id for attempt in self._square_reserved_attempts()}
        return self.env['pos.square.payment.attempt'].sudo().browse(list(by_payment.values()))

    def _square_cash_tenders(self, payments, digits):
        self.ensure_one()
        for payment in payments:
            method = payment.payment_method_id
            if (
                not method.is_cash_count or method.payment_method_type != 'none' or method.use_payment_terminal
                or method.company_id != self.company_id or not method.active
                or method not in self.config_id.payment_method_ids
            ):
                raise UserError(_("Square Terminal only supports combining Odoo cash and Square cards. Remove %s before sending.",
                                  method.display_name))
            if (
                not payment.uuid or payment.is_change or payment.payment_status
                or any(payment[field] for field in payment._square_evidence_fields - {'payment_status'})
                or minor_units(payment.amount, digits) <= 0
            ):
                raise UserError(_("Record a positive cash share without change for this Square sale."))
        return payments

    def _square_check_new_share(self, payment, attempts):
        self.ensure_one()
        link = attempts.square_order_link_id
        pending = self.payment_ids - attempts.payment_id - link._cash_payment_lines()
        cards = pending.filtered(lambda line: line.payment_method_id.use_payment_terminal == 'square_terminal')
        cash = self._square_cash_tenders(pending - cards, link.currency_digits)
        if (
            self.state != 'draft' or not link.split or link.cancel_requested or link.settlement_request
            or link.cash_recording or any(attempt.state != 'authorized' for attempt in attempts)
            or payment not in pending or len(cards) > 1
            or any(card.is_change or not card.uuid for card in cards)
            or any(minor_units(card.amount, link.currency_digits) <= 0 for card in cards)
            or sum(minor_units(line.amount, link.currency_digits) for line in cash | cards)
            > link.total_minor + link.tip_minor - link._cash_total_minor() - sum(attempts.mapped('amount_minor'))
        ):
            raise UserError(_("Add cash or one Square card share within the remaining balance."))

    def _square_check_reserved_amounts(self, attempts):
        self.ensure_one()
        link = attempts.square_order_link_id
        total = sum(attempts.mapped('amount_minor')) + link._cash_total_minor()
        digits = link.currency_digits
        cash = link._cash_payment_lines()
        if (
            set(self.payment_ids.ids) != set((attempts.payment_id | cash).ids)
            or len(cash) != len(link.cash_tenders or [])
            or self.uuid != link.order_uuid or self.currency_id.name != link.currency_code
            or self.amount_return or minor_units(self.amount_total, digits) != link.total_minor + link.tip_minor
            or (link.split and not 0 < total <= link.total_minor + link.tip_minor)
            or (not link.split and (len(attempts) != 1 or total != link.total_minor + link.tip_minor))
            or any(
                attempt.payment_id.payment_method_id.id != attempt.payment_method_id.id
                or attempt.payment_id.uuid != attempt.payment_uuid or attempt.payment_id.is_change
                or minor_units(attempt.payment_id.amount, digits) != attempt.amount_minor
                for attempt in attempts
            )
            or not link._matches_native_cash(cash)
        ):
            raise UserError(_("The Odoo sale differs from the reserved Square payment."))

    def _square_require_verified(self):
        for order in self:
            attempts = order._square_current_attempts()
            if not attempts and not any(p.payment_method_id.use_payment_terminal == 'square_terminal' for p in order.payment_ids):
                continue
            link = attempts.square_order_link_id
            if (
                not attempts or any(attempt.state != 'verified' for attempt in attempts)
                or (link.split and (
                    sum(attempts.mapped('amount_minor')) + link._cash_total_minor() != link.total_minor + link.tip_minor
                    or link._settlement_payment_ids(attempts) != (link.settlement_request or {}).get('payment_ids')
                    or not link._matches_native_cash(link._cash_payment_lines(), verified=True)
                ))
            ):
                raise UserError(_("Square has not verified payment for this sale."))
            order._square_check_reserved_amounts(attempts)
            if (
                minor_units(order.currency_id.round(order.amount_paid), link.currency_digits) != link.total_minor + link.tip_minor
                or any(
                    attempt.payment_id.payment_status != 'done'
                    or attempt.payment_id.transaction_id != attempt.square_payment_id
                    for attempt in attempts
                )
            ):
                raise UserError(_("The original Odoo tender has not been verified by Square."))

    def _process_saved_order(self, draft):
        if not draft:
            self._square_lock_checkout()
            self._square_require_verified()
        result = super()._process_saved_order(draft)
        if not draft:
            attempts = self._square_reserved_attempts()
            if attempts and self.state not in PAID_STATES:
                raise UserError(_("Odoo could not complete the verified Square sale."))
            attempts.filtered_domain([('state', '=', 'verified')]).square_order_link_id._square_queue_handover()
        return result

    @api.model_create_multi
    def create(self, vals_list):
        orders = super().create(vals_list)
        orders.filtered(lambda order: order.state in PAID_STATES)._square_require_verified()
        return orders

    def write(self, vals):
        changes = self._square_frozen_fields.intersection(vals)
        payment_change = bool({'state', 'payment_ids', 'amount_paid'}.intersection(vals))
        reserved_orders = self.browse()
        if changes or payment_change or 'lines' in vals:
            self.check_access('write')
            self._square_lock_checkout()
            reserved_orders = self.browse(self._square_reserved_attempts().order_id.ids)
            for order in reserved_orders:
                if (
                    any(not same_value(order, field, vals[field]) for field in changes)
                    or not order._square_same_children(vals, 'lines')
                    or not order._square_payment_commands_allowed(vals)
                    or vals.get('state') == 'cancel'
                ):
                    raise UserError(_("The goods and tenders are reserved by the original Square payment."))
            if vals.get('state') in PAID_STATES:
                self._square_require_verified()
        if 'amount_paid' in vals and reserved_orders:
            # Native draft sync sends only completed client lines, then recomputes
            # the sum on the server. Keep reserved tender values canonical at both writes.
            for order in reserved_orders:
                super(PosOrder, order).write({
                    **vals, 'amount_paid': order.currency_id.round(sum(order.payment_ids.mapped('amount'))),
                })
            other_orders = self - reserved_orders
            result = super(PosOrder, other_orders).write(vals) if other_orders else True
        else:
            result = super().write(vals)
        if vals.get('payment_ids'):
            self.filtered(lambda order: order.state in PAID_STATES)._square_require_verified()
        return result

    def _square_payment_commands_allowed(self, vals):
        self.ensure_one()
        ids = set(self.payment_ids.ids)
        return all(
            command[0] == Command.CREATE
            or (command[0] in (Command.UPDATE, Command.LINK, Command.DELETE, Command.UNLINK) and command[1] in ids)
            for command in vals.get('payment_ids', [])
        )

    def _square_same_children(self, vals, field):
        ids = set(self[field].ids)
        return all(
            command[0] in (Command.UPDATE, Command.LINK) and command[1] in ids
            for command in vals.get(field, [])
        )

    def unlink(self):
        self.check_access('unlink')
        self._square_lock_checkout()
        if self._square_attempts():
            raise UserError(_("The original sale is reserved by a Square payment."))
        return super().unlink()

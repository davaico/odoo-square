from decimal import Decimal
from uuid import uuid4

from psycopg2.errors import UniqueViolation

from odoo import Command, _, api, models
from odoo.exceptions import AccessError, UserError


PAID_STATES = ('paid', 'done', 'invoiced')


def minor_units(amount, digits):
    value = Decimal(str(amount)).scaleb(digits)
    if not value.is_finite() or value != value.to_integral_value():
        raise UserError(_("The Odoo amount cannot be represented in the Square location currency."))
    return int(value)


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
        'amount_total', 'amount_tax', 'amount_paid', 'amount_return', 'session_id', 'company_id',
        'pricelist_id', 'fiscal_position_id', 'currency_id', 'config_id', 'uuid', 'pos_reference',
        'shipping_date', 'tip_amount', 'is_tipped',
    }

    @api.model
    def square_prepare_payment(self, order_uuid, payment_uuid):
        order, payment = self._square_checkout_records(order_uuid, payment_uuid)
        order._square_lock_checkout(touch=True)
        attempts = order._square_attempts()
        attempt = attempts.filtered(lambda candidate: candidate.payment_id.id == payment.id)[-1:]
        if attempt:
            return attempt._cashier_status()
        order._square_check_session()
        if (
            order.state != 'draft' or attempts or order.payment_ids != payment
            or payment.payment_method_id.use_payment_terminal != 'square_terminal'
            or payment.payment_status != 'waiting' or payment.is_change
        ):
            raise UserError(_("Square Terminal requires one card payment for the full unpaid sale."))

        method = payment.payment_method_id
        if not method.active or method not in order.config_id.payment_method_ids or method.company_id != order.company_id:
            raise UserError(_("This Square payment method is unavailable for this register."))
        reason = method._square_readiness_error(order.config_id)
        if reason:
            raise UserError(reason)
        terminal = method.sudo().square_terminal_id

        connection = terminal.connection_id
        device_key = attempts._device_key(connection.environment, connection.merchant_id, terminal.device_id)
        if attempts.search_count([('active_device_key', '=', device_key)], limit=1):
            return {'state': 'busy'}

        currency = terminal.location_id.currency_code
        digits = 0 if currency == 'JPY' else 2
        total_minor, line_items = order._square_priced_basket(digits)
        if (
            total_minor < 1 or total_minor > 2_147_483_647
            or minor_units(payment.amount, digits) != total_minor
        ):
            raise UserError(_("The Square payment must cover the exact Odoo order total."))

        order_reference = 'odoo-' + uuid4().hex
        try:
            with self.env.cr.savepoint():
                order_link = self.env['pos.square.order'].sudo().create({
                    'order_id': order.id,
                    'order_uuid': order.uuid,
                    'company_id': order.company_id.id,
                    'connection_id': connection.id,
                    'environment': connection.environment,
                    'merchant_id': connection.merchant_id,
                    'location_id': terminal.location_id.id,
                    'square_location_id': terminal.location_id.square_id,
                    'reference': order_reference,
                    'currency_code': currency,
                    'currency_digits': digits,
                    'total_minor': total_minor,
                    'create_request': order._square_order_payload(terminal.location_id, order_reference, line_items),
                })
                attempt_reference = 'odoo-' + uuid4().hex
                attempt = attempts.create({
                    'square_order_link_id': order_link.id,
                    'payment_id': payment.id,
                    'payment_uuid': payment.uuid,
                    'payment_method_id': method.id,
                    'terminal_id': terminal.id,
                    'square_device_id': terminal.device_id,
                    'application_id': connection.application_id,
                    'reference': attempt_reference,
                    'amount_minor': total_minor,
                    'checkout_request': {
                        'idempotency_key': str(uuid4()),
                        'checkout': {
                            'reference_id': attempt_reference,
                            'amount_money': {'amount': total_minor, 'currency': currency},
                            'payment_options': {'autocomplete': True},
                            'device_options': {
                                'device_id': terminal.device_id,
                                'show_itemized_cart': True,
                                'allow_auto_card_surcharge': False,
                                'tip_settings': {'allow_tipping': False},
                            },
                        },
                    },
                })
                attempt.flush_recordset(['active_device_key'])
        except UniqueViolation as error:
            if error.diag.constraint_name != 'pos_square_payment_attempt_active_device_unique':
                raise
            return {'state': 'busy'}
        return attempt._cashier_status()

    @api.model
    def square_prepare_linked_order(self, order_uuid, payment_uuid):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        attempt._create_square_order()
        return attempt._cashier_status()

    @api.model
    def square_mark_dispatching(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if attempt.reference == attempt_reference and attempt.state == 'ready':
            attempt.state = 'dispatching'
        return attempt._cashier_status()

    @api.model
    def square_dispatch_payment(self, order_uuid, payment_uuid, attempt_reference=None):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if attempt.reference == attempt_reference:
            attempt._dispatch()
        return attempt._cashier_status()

    @api.model
    def square_retry_payment(self, order_uuid, payment_uuid, attempt_reference):
        order, payment = self._square_checkout_records(order_uuid, payment_uuid)
        order._square_lock_checkout(touch=True)
        previous = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if previous.reference != attempt_reference or previous.state != 'failed':
            return previous._cashier_status()

        order._square_check_session()
        order._square_check_reserved_amounts(previous)
        method = payment.payment_method_id
        if (
            not method.active or method not in order.config_id.payment_method_ids
            or method.sudo().square_terminal_id.id != previous.terminal_id.id
            or previous.terminal_id.device_id != previous.square_device_id
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
        attempt._refresh()
        return attempt._cashier_status()

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
        order._square_lock_checkout()
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
        if must_be_open and attempt.state in ('prepared', 'ready', 'dispatching'):
            order._square_check_session()
            order._square_check_reserved_amounts(attempt)
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
            not self.lines or self.amount_return > 0 or self.is_tipped or self.tip_amount or self.shipping_date
            or (config.cash_rounding and not config.only_round_cash_method)
            or (config.use_pricelist and pricelist not in allowed_pricelists)
            or (not config.use_pricelist and self.pricelist_id and self.pricelist_id != config.pricelist_id)
            or (self.fiscal_position_id and self.fiscal_position_id not in allowed_positions)
        ):
            raise UserError(_("This sale configuration is unavailable for Square Terminal."))
        client_total = minor_units(self.amount_total, digits)
        client_tax = minor_units(self.amount_tax, digits)
        line_items = []
        expected_total = 0
        for line in self.lines:
            product = line.product_id
            taxes = product.taxes_id._filter_taxes_by_company(self.company_id)
            if (
                line.qty <= 0 or line.qty != int(line.qty) or line.price_unit <= 0 or line.discount
                or line.price_type == 'manual' or line.price_extra or line.refunded_orderline_id
                or line.combo_parent_id or line.combo_item_id or product.type == 'combo'
                or product.tracking != 'none'
                or not product.active or not product.sale_ok or not product.available_in_pos
                or (product.company_id and product.company_id != self.company_id)
                or set(taxes.ids) != set(line.tax_ids.ids)
                or line.tax_ids_after_fiscal_position
                or (config.tip_product_id and product == config.tip_product_id)
            ):
                raise UserError(_("Square Terminal currently supports ordinary untaxed items at their configured price."))
            price = (
                pricelist.with_company(self.company_id)._get_product_price(product, line.qty)
                if pricelist else product.lst_price
            )
            unit_minor = minor_units(line.price_unit, digits)
            subtotal = unit_minor * int(line.qty)
            if (
                minor_units(price, digits) != unit_minor or minor_units(line.price_subtotal, digits) != subtotal
                or minor_units(line.price_subtotal_incl, digits) != subtotal
            ):
                raise UserError(_("The Odoo basket price changed. Reload the register and check the sale."))
            expected_total += subtotal
            line_items.append({
                'name': product.display_name[:255],
                'quantity': str(int(line.qty)),
                'base_price_money': {'amount': unit_minor, 'currency': self.currency_id.name},
            })
        self.with_context(backend_recomputation=True)._compute_prices()
        if (
            client_tax or client_total != expected_total
            or minor_units(self.amount_tax, digits) or minor_units(self.amount_total, digits) != expected_total
        ):
            raise UserError(_("The calculated Odoo amounts differ from the displayed basket."))
        return expected_total, line_items

    def _square_order_payload(self, location, reference, line_items):
        self.ensure_one()
        return {
            'idempotency_key': str(uuid4()),
            'order': {
                'location_id': location.square_id, 'reference_id': reference, 'state': 'OPEN', 'line_items': line_items,
            },
        }

    def _square_attempts(self):
        return self.env['pos.square.payment.attempt'].sudo().search([('order_id', 'in', self.ids)], order='id')

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

    def _square_check_reserved_amounts(self, attempt):
        self.ensure_one()
        payment = attempt.payment_id
        order_link = attempt.square_order_link_id
        digits = order_link.currency_digits
        if (
            self.payment_ids.ids != payment.ids or payment.payment_method_id.id != attempt.payment_method_id.id
            or payment.uuid != attempt.payment_uuid or self.uuid != order_link.order_uuid
            or self.currency_id.name != attempt.currency_code or self.amount_return
            or minor_units(self.amount_total, digits) != order_link.total_minor
            or minor_units(payment.amount, digits) != attempt.amount_minor
            or minor_units(self.amount_paid, digits) != attempt.amount_minor
            or payment.is_change
        ):
            raise UserError(_("The Odoo sale differs from the reserved Square payment."))

    def _square_require_verified(self):
        attempts = self._square_attempts()
        for order in self:
            order_attempts = attempts.filtered(lambda attempt: attempt.order_id.id == order.id)
            if not order_attempts and not any(p.payment_method_id.use_payment_terminal == 'square_terminal' for p in order.payment_ids):
                continue
            order_attempts = order_attempts.filtered(lambda attempt: attempt.state != 'failed')
            if len(order_attempts) != 1 or order_attempts.state != 'verified':
                raise UserError(_("Square has not verified payment for this sale."))
            order._square_check_reserved_amounts(order_attempts)
            payment = order_attempts.payment_id
            if payment.payment_status != 'done' or payment.transaction_id != order_attempts.square_payment_id:
                raise UserError(_("The original Odoo tender has not been verified by Square."))

    def _process_saved_order(self, draft):
        if not draft:
            self._square_lock_checkout()
            self._square_require_verified()
        result = super()._process_saved_order(draft)
        if not draft and self._square_attempts() and self.state not in PAID_STATES:
            raise UserError(_("Odoo could not complete the verified Square sale."))
        return result

    @api.model_create_multi
    def create(self, vals_list):
        orders = super().create(vals_list)
        orders.filtered(lambda order: order.state in PAID_STATES)._square_require_verified()
        return orders

    def write(self, vals):
        changes = self._square_frozen_fields.intersection(vals)
        payment_change = bool({'state', 'payment_ids'}.intersection(vals))
        if changes or payment_change or 'lines' in vals:
            self.check_access('write')
            self._square_lock_checkout()
            for order in self._square_attempts().order_id:
                if (
                    any(not same_value(order, field, vals[field]) for field in changes)
                    or not order._square_same_children(vals, 'lines')
                    or not order._square_same_children(vals, 'payment_ids')
                    or vals.get('state') == 'cancel'
                ):
                    raise UserError(_("The goods and tenders are reserved by the original Square payment."))
            if vals.get('state') in PAID_STATES:
                self._square_require_verified()
        result = super().write(vals)
        if payment_change and vals.get('payment_ids'):
            self.filtered(lambda order: order.state in PAID_STATES)._square_require_verified()
        return result

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

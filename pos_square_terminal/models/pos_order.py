# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from math import isclose
from uuid import uuid4

from psycopg2.errors import UniqueViolation

from odoo import Command, _, api, models
from odoo.exceptions import AccessError, UserError
from odoo.service.model import retrying

from ..square_api import SquareAPIError
from .pos_order_pricing import minor_units
from .square_payment_attempt import UNPAID_ORDER_WITHOUT_CHECKOUT


PAID_STATES = ('paid', 'done', 'invoiced')


def same_value(record, field, value):
    current = record[field]
    field_type = record._fields[field].type
    if field_type == 'many2one':
        return current.id == value
    if field_type in ('many2many', 'one2many'):
        ids = set(current.ids)
        if value is False or value is None:
            return not ids
        if isinstance(value, tuple) or (value and not isinstance(value[0], (tuple, list))):
            return all(type(record_id) is int for record_id in value) and set(value) == ids
        return all(
            (command[0] == Command.LINK and command[1] in ids)
            or (command[0] == Command.SET and set(command[2]) == ids)
            or (command[0] == Command.CLEAR and not ids)
            for command in value
        )
    if field_type == 'float' and type(value) in (int, float):
        return isclose(current, value, rel_tol=1e-15)
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
            gross_minor = total_minor + tip_minor
            if (
                not 0 < total_minor <= gross_minor <= 2_147_483_647
                or not 0 < amount_minor <= gross_minor - cash_minor or payment.is_change
            ):
                raise UserError(_("Enter a Square card share within the unpaid Odoo order total."))

        try:
            with self.env.cr.savepoint():
                if not link:
                    link = order._square_prepare_order(
                        terminal.location_id, digits, total_minor, line_items, tip_minor,
                        split=amount_minor < gross_minor,
                    )
                link._reserve_cash_tenders(cash, attempts)
                money = link._tender_amounts(amount_minor, *link._covered_tender_amounts(attempts))
                if money['amount_money']['amount'] <= 0:
                    raise UserError(_("Enter a larger Square card share to include some of the goods total."))
                attempt_reference = 'odoo-' + uuid4().hex
                checkout = {
                    'reference_id': attempt_reference,
                    **money,
                    'payment_options': {'autocomplete': not link.split},
                    'device_options': {
                        'device_id': terminal.device_id,
                        'show_itemized_cart': True,
                        'allow_auto_card_surcharge': False,
                        'tip_settings': {'allow_tipping': bool(link.device_tip_product_id)},
                    },
                }
                if link.square_order_id:
                    checkout['order_id'] = link.square_order_id
                attempt = attempts.with_context({}, lang=self.env.lang).with_company(order.company_id).create({
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
        device_tip_product = self.env['product.product']
        if self.config_id.square_terminal_tipping and not self.is_tipped:
            device_tip_product = self._square_check_device_tip_product()
        connection = location.connection_id
        reference = 'odoo-' + uuid4().hex
        try:
            create_request, pricing_snapshot = self._square_order_payload(location, reference, line_items)
        except SquareAPIError:
            raise UserError(_("Square could not check the catalog. Try again before sending the payment.")) from None
        links = self.env['pos.square.order'].sudo().with_context({}, lang=self.env.lang).with_company(self.company_id)
        link = links.create({
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
            'device_tip_product_id': device_tip_product.id,
            'device_tip_line_id': False,
            'create_request': create_request,
            'pricing_snapshot': pricing_snapshot,
            'split': split,
        })
        try:
            link._preview_square_order()
        except SquareAPIError:
            raise UserError(_("Square could not check the order pricing. Try again before sending the payment.")) from None
        return link

    def _square_check_device_tip_product(self):
        self.ensure_one()
        product = self.config_id.tip_product_id
        if (
            not product or not product.active or not product.sale_ok or product.type != 'service'
            or (product.company_id and product.company_id != self.company_id)
            or product.taxes_id._filter_taxes_by_company(self.company_id)
        ):
            raise UserError(_("Configure an active, tax-free Odoo tip service product for this Square register."))
        return product

    def _square_project_device_tip(self, link):
        self.ensure_one()
        tip_minor = link._device_tip_minor(self._square_current_attempts())
        if not tip_minor or link.device_tip_line_id:
            return
        divisor = 10 ** link.currency_digits
        tip = tip_minor / divisor
        link.device_tip_line_id = self.lines._square_create_device_tip(self, link.device_tip_product_id, tip)
        super(PosOrder, self).write({
            'is_tipped': True,
            'tip_amount': tip,
            'amount_total': (link.total_minor + link.tip_minor + tip_minor) / divisor,
            'amount_paid': self.currency_id.round(sum(self.payment_ids.mapped('amount'))),
        })

    @api.model
    def square_prepare_linked_order(self, order_uuid, payment_uuid):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if not attempt.square_order_link_id.cancel_requested:
            attempt._create_square_order()
        return attempt._cashier_status()

    @api.model
    def square_mark_dispatching(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        status = attempt._cashier_status()
        if attempt.reference == attempt_reference and status['state'] == 'ready':
            # Existing browser tabs can proceed to Send without recording a provider submission.
            status['state'] = 'dispatching'
        return status

    @api.model
    def square_dispatch_payment(self, order_uuid, payment_uuid, attempt_reference=None):
        # Persist the marker inside Send so a disconnected browser can leave the reservation ready.
        # Own both transactions so a failure after the Square POST cannot roll back the marker.
        with self.env.registry.cursor() as cr:
            orders = self.with_env(self.env(cr=cr))
            attempt = orders._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
            if attempt.reference != attempt_reference or attempt._cashier_state() != 'ready':
                return attempt._cashier_status()
            attempt.state = 'dispatching'
            cr.commit()

            submission_started = False

            def mark_submitted():
                nonlocal submission_started
                submission_started = True

            def dispatch_reserved_checkout():
                attempt = orders._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
                if attempt.reference != attempt_reference or attempt.state != 'dispatching':
                    return attempt._cashier_status()
                if submission_started:
                    # A database retry after the external call can only reconcile the original request.
                    attempt._refresh()
                elif attempt.square_order_link_id.cancel_requested:
                    attempt.state = 'failed'
                    attempt.order_id._square_finish_cancellation(attempt)
                else:
                    attempt._dispatch(mark_submitted)
                return attempt._cashier_status()

            return retrying(dispatch_reserved_checkout, orders.env)

    @api.model
    def square_retry_payment(self, order_uuid, payment_uuid, attempt_reference):
        previous = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if previous.reference != attempt_reference or previous._cashier_state() != 'failed':
            return previous._cashier_status()
        if not previous._refresh(recheck_failed=True) or previous._cashier_state() != 'failed':
            return previous._cashier_status()

        payment = previous.payment_id.with_env(self.env)
        order = payment.pos_order_id
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
                attempt = previous.with_context({}, lang=self.env.lang).with_company(order.company_id).create({
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
        if attempt.reference == attempt_reference:
            attempt.order_id.with_user(self.env.user)._square_prepare_split_settlement(attempt)
        return attempt._cashier_status()

    def _square_prepare_split_settlement(self, attempt, *, reserve_cash=True):
        link = attempt.square_order_link_id
        if (
            self.state != 'draft' or attempt.state != 'authorized' or not link.split
            or link.cancel_requested or link.settlement_request or link.cash_recording
        ):
            return
        attempts = self._square_current_attempts()
        if any(share.state != 'authorized' for share in attempts):
            return
        pending = self.payment_ids - attempts.payment_id - link._cash_payment_lines()
        if pending and not reserve_cash:
            return
        if any(line.payment_method_id.use_payment_terminal == 'square_terminal' for line in pending):
            return
        cash = self._square_cash_tenders(pending, link.currency_digits)
        covered = link._covered_gross_minor(attempts)
        added_cash = sum(minor_units(line.amount, link.currency_digits) for line in cash)
        if covered + added_cash != link.total_minor + link.tip_minor:
            return
        self._square_check_session()
        link._reserve_cash_tenders(cash, attempts)
        self._square_check_reserved_amounts(attempts)
        if link.cash_tenders:
            # Commit the recording intent before issuing any Cash CreatePayment.
            link.cash_recording = True
        else:
            link._prepare_settlement(attempts)

    @api.model
    def square_record_cash(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=False)
        if attempt.reference == attempt_reference and attempt._cashier_state() == 'recording_cash':
            attempt.order_id.with_user(self.env.user)._square_record_split_cash(attempt)
        return attempt._cashier_status()

    def _square_record_split_cash(self, attempt):
        attempts = self._square_current_attempts()
        self._square_check_session()
        self._square_check_reserved_amounts(attempts)
        link = attempt.square_order_link_id
        if all(share.state == 'authorized' for share in attempts) and link._record_cash_tenders(attempt):
            link._prepare_settlement(attempts)

    @api.model
    def square_settle_payment(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=False)
        state = attempt._cashier_state()
        if attempt.reference == attempt_reference and state in ('settling', 'settlement_blocked'):
            attempt.order_id.with_user(self.env.user)._square_settle_split(attempt)
        return attempt._cashier_status()

    def _square_settle_split(self, attempt):
        self._square_check_session()
        attempts = self._square_current_attempts()
        self._square_check_reserved_amounts(attempts)
        link = attempt.square_order_link_id
        link._settle(attempts, allow_square_capture=not link.settlement_blocked)

    @api.model
    def square_cancel_payment(self, order_uuid, payment_uuid, attempt_reference):
        attempt = self._square_requested_attempt(order_uuid, payment_uuid, must_be_open=True)
        if attempt.reference == attempt_reference and attempt._cashier_state() not in ('verified', 'cancelled'):
            order = attempt.order_id.with_user(self.env.user)
            order._square_check_session()
            link = attempt.square_order_link_id
            if link.settlement_request and not link.cancel_requested:
                outcome = link._settle(order._square_current_attempts(), allow_square_capture=False, for_release=True)
                if outcome not in ('authorizations_only', 'released_tender_confirmed'):
                    return attempt._cashier_status()
            link.cancel_requested = True
            order._square_finish_cancellation(attempt)
        return attempt._cashier_status()

    def _square_finish_cancellation(self, attempt):
        link = attempt.square_order_link_id
        related_attempts = self._square_reserved_attempts().filtered(
            lambda candidate: candidate.square_order_link_id == link,
        )
        orphaned = related_attempts.browse()
        checks_complete = True
        for share in related_attempts:
            result = share._cancel_checkout()
            if result == UNPAID_ORDER_WITHOUT_CHECKOUT:
                orphaned |= share
            elif not result or share.state not in ('failed', 'blocked'):
                checks_complete = False
        if not checks_complete:
            return
        try:
            attempt._check_provider_scope()
        except UserError:
            return
        if not link._release_cash_tenders():
            return
        released_payment_ids = [
            payment_id for share in related_attempts for payment_id in share._known_payment_ids()
        ]
        released_payment_ids.extend(filter(None, link._cash_square_payment_ids()))
        link._cancel_unpaid_order(released_payment_ids)
        if link.cancelled:
            # An open order without a visible checkout is ambiguous until Square
            # confirms the versioned cancellation. CANCELED is terminal and unpaid.
            orphaned.write({'state': 'failed'})
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
            > link.total_minor + link.tip_minor - link._covered_gross_minor(attempts)
        ):
            raise UserError(_("Add cash or one Square card share within the remaining balance."))

    def _square_check_reserved_amounts(self, attempts):
        self.ensure_one()
        link = attempts.square_order_link_id
        total = link._covered_gross_minor(attempts)
        digits = link.currency_digits
        cash = link._cash_payment_lines()
        if (
            set(self.payment_ids.ids) != set((attempts.payment_id | cash).ids)
            or len(cash) != len(link.cash_tenders or [])
            or self.uuid != link.order_uuid or self.currency_id.name != link.currency_code
            or self.amount_return
            or minor_units(self.amount_total, digits) != link.total_minor + link.tip_minor + link._device_tip_minor(attempts)
            or (link.split and not 0 < total <= link.total_minor + link.tip_minor)
            or (not link.split and (len(attempts) != 1 or total != link.total_minor + link.tip_minor))
            or any(
                attempt.payment_id.payment_method_id.id != attempt.payment_method_id.id
                or attempt.payment_id.uuid != attempt.payment_uuid or attempt.payment_id.is_change
                or minor_units(attempt.payment_id.amount, digits) != attempt.amount_minor + attempt._device_tip_minor()
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
                    link._covered_gross_minor(attempts) != link.total_minor + link.tip_minor
                    or link._settlement_payment_ids(attempts) != (link.settlement_request or {}).get('payment_ids')
                    or not link._matches_native_cash(link._cash_payment_lines(), verified=True)
                ))
            ):
                raise UserError(_("Square has not verified payment for this sale."))
            order._square_check_reserved_amounts(attempts)
            if (
                minor_units(order.currency_id.round(order.amount_paid), link.currency_digits)
                != link.total_minor + link.tip_minor + link._device_tip_minor(attempts)
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

    @api.model
    def remove_from_ui(self, server_ids):
        orders = self.search([('id', 'in', server_ids), ('state', '=', 'draft')])
        orders._square_lock_checkout()
        audited_orders = orders & orders._square_attempts().order_id
        # Native discard deletes both the order and its payment lines. Preserve
        # the linked records and use the native canceled state to remove them from the till.
        audited_orders.write({'state': 'cancel'})
        return super().remove_from_ui((orders - audited_orders).ids) + audited_orders.ids

    def unlink(self):
        self.check_access('unlink')
        self._square_lock_checkout()
        if self._square_attempts():
            raise UserError(_("The original sale is reserved by a Square payment."))
        return super().unlink()

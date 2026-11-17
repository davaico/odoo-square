from uuid import uuid4

from odoo import SUPERUSER_ID, _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.http import request

from odoo.addons.payment import utils as payment_utils
from odoo.osv import expression


_SQUARE_TRANSACTION_FIELDS = {
    'amount', 'currency_id', 'invoice_ids', 'is_post_processed', 'landing_route',
    'last_state_change', 'operation', 'partner_id', 'payment_id', 'payment_method_id',
    'provider_id', 'provider_reference', 'reference', 'sale_order_ids',
    'source_transaction_id', 'state', 'state_message', 'token_id', 'tokenize',
}


class PaymentTransaction(models.Model):
    _inherit = 'payment.transaction'

    square_connection_id = fields.Many2one('payment.square.connection', readonly=True, copy=False, ondelete='restrict')
    square_merchant_id = fields.Char(readonly=True, copy=False)
    square_environment = fields.Selection(
        [('sandbox', 'Sandbox'), ('production', 'Production')], readonly=True, copy=False,
    )
    square_application_id = fields.Char(readonly=True, copy=False)
    square_location_ref = fields.Char(string='Square Location ID', readonly=True, copy=False)
    square_journal_id = fields.Many2one('account.journal', readonly=True, copy=False, ondelete='restrict')
    square_outstanding_account_id = fields.Many2one('account.account', readonly=True, copy=False)
    square_currency_code = fields.Char(readonly=True, copy=False)
    square_amount_minor = fields.Integer(readonly=True, copy=False)
    square_order_reference = fields.Char(readonly=True, copy=False)
    square_order_id = fields.Char(readonly=True, copy=False)
    square_cart_signature = fields.Char(readonly=True, copy=False)
    square_reserved_at = fields.Datetime(readonly=True, copy=False)
    square_dispatched_at = fields.Datetime(readonly=True, copy=False)

    _sql_constraints = [
        ('square_order_reference_unique', 'UNIQUE(square_order_reference)', 'Square checkout references must be unique.'),
        ('square_payment_reference_unique',
         'UNIQUE(square_environment, square_merchant_id, provider_reference)',
         'A Square operation can only be linked once per merchant and environment.'),
    ]

    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.su and (
            any(key.startswith('square_') for values in vals_list for key in values)
            or any(key.startswith('default_square_') for key in self.env.context)
        ):
            raise AccessError(_("Square payment records can only be created by the Square integration."))
        transactions = super().create(vals_list)
        if not self.env.su and transactions.filtered(lambda tx: tx.provider_code == 'square_online'):
            raise AccessError(_("Square payment records can only be created by the Square integration."))
        for tx in transactions.filtered(lambda t: t.operation and t.operation.startswith('online_')):
            for order in tx.sale_order_ids.filtered('website_id').sorted('id'):
                order._square_lock_checkout_for_change()
        return transactions

    def write(self, values):
        if not self.env.su and (
            _SQUARE_TRANSACTION_FIELDS.intersection(values) or any(key.startswith('square_') for key in values)
        ):
            self.check_access('write')
            target = self.env['payment.provider'].sudo().browse(values.get('provider_id') or [])
            if (
                any(key.startswith('square_') for key in values)
                or target.code == 'square_online'
                or self.sudo().filtered(lambda tx: tx.provider_code == 'square_online')
            ):
                raise AccessError(_("Square payment records can only be updated by the Square integration."))
        return super().write(values)

    def unlink(self):
        if not self.env.su:
            self.check_access('unlink')
            if self.sudo().filtered(lambda tx: tx.provider_code == 'square_online'):
                raise AccessError(_("Square payment records cannot be deleted manually."))
        return super().unlink()

    @api.model
    def _square_online_pos_scope_exists(self, environment):
        """Check for a checkout that could already have created an Order at Square."""
        domain = [
            ('provider_code', '=', 'square_online'),
            ('operation', '=', 'online_direct'),
            ('square_reserved_at', '!=', False),
        ]
        if environment:
            domain.append(('square_environment', '=', environment))
        return self._square_online_pos_has_transaction(domain)

    @api.model
    def _square_online_matches_pos_resource(self, environment, kind, resource, merchant_id=None):
        """Match an authenticated Square resource against durable website ownership."""
        location_id = resource.get('location_id')
        resource_id = resource.get('id')
        if not location_id or not resource_id:
            return False
        domain = [
            ('provider_code', '=', 'square_online'),
            ('square_environment', '=', environment),
            ('square_location_ref', '=', location_id),
        ]
        if merchant_id:
            domain.append(('square_merchant_id', '=', merchant_id))
        if kind == 'order':
            candidates = [[('square_order_id', '=', resource_id)]]
            if resource.get('reference_id'):
                candidates.append([
                    ('square_order_reference', '=', resource['reference_id']),
                    ('square_order_id', '=', False),
                    ('square_reserved_at', '!=', False),
                ])
        elif kind == 'payment':
            domain.append(('operation', '=', 'online_direct'))
            candidates = [[('provider_reference', '=', resource_id)]]
            if resource.get('reference_id') and resource.get('order_id'):
                candidates.append([
                    ('square_order_reference', '=', resource['reference_id']),
                    ('square_order_id', '=', resource['order_id']),
                    ('provider_reference', '=', False),
                    ('square_dispatched_at', '!=', False),
                ])
        elif kind == 'refund':
            domain.append(('operation', '=', 'refund'))
            candidates = [[('provider_reference', '=', resource_id)]]
        else:
            return False
        return self._square_online_pos_has_transaction(expression.AND([domain, expression.OR(candidates)]))

    def _square_online_pos_has_transaction(self, domain):
        # A resync may find a new Square order after its database snapshot began.
        # Checkout commits ownership before sending CreateOrder, so read from a fresh snapshot.
        with self.env.registry.cursor() as cr:
            transactions = self.with_env(api.Environment(cr, SUPERUSER_ID, {}))
            return bool(transactions.search_count(domain, limit=1))

    def _square_validate_checkout(self, order):
        self.ensure_one()
        provider = self.provider_id
        connection = provider._square_get_connection()
        if (
            provider.code != 'square_online' or self.operation != 'online_direct' or self.tokenize
            or self.payment_method_id != self.env.ref('payment.payment_method_card')
            or self.payment_method_id not in provider.payment_method_ids or not self.payment_method_id.active
            or self.sale_order_ids != order or self.partner_id != order.partner_invoice_id
            or order.state not in ('draft', 'sent') or self.company_id != order.company_id
            or self.currency_id != order.currency_id
            or self.amount <= 0 or order.currency_id.compare_amounts(self.amount, order.amount_total)
            or order.currency_id.compare_amounts(order.amount_paid, 0)
        ):
            raise ValidationError(_("The cart or payment method has changed. Please refresh checkout."))

        order._check_cart_is_ready_to_be_paid()
        # Use the same buyer and checkout context as the native website payment page.
        user = self.env.user
        partner = order.partner_id if user._is_public() else user.partner_id
        checkout = {'sale_order_id': order.id, 'website_id': order.website_id.id}
        available_providers = provider.with_context(square_online_checkout=True)._get_compatible_providers(
            order.company_id.id, partner.id, order.amount_total, currency_id=order.currency_id.id,
            **checkout,
        )
        available_methods = self.env['payment.method']._get_compatible_payment_methods(
            (available_providers & provider).ids, partner.id, currency_id=order.currency_id.id, **checkout,
        )
        if self.payment_method_id not in available_methods:
            raise ValidationError(_("Square is unavailable for this order. Please choose a different payment method."))
        return connection

    def _square_prepare_checkout(self, order, cart_signature):
        self.ensure_one()
        connection = self._square_validate_checkout(order)
        if not cart_signature or cart_signature != order._square_cart_signature():
            raise ValidationError(_("The cart has changed. Please refresh checkout."))

        provider = self.provider_id
        method_line = provider.journal_id.inbound_payment_method_line_ids.filtered(
            lambda line: line.payment_provider_id == provider,
        )
        self.write({
            'landing_route': '/shop/payment/validate',
            'amount': order.amount_total,
            'square_connection_id': connection.id,
            'square_merchant_id': connection.merchant_id,
            'square_environment': provider.square_environment,
            'square_application_id': provider.square_application_id,
            'square_location_ref': provider.square_location_id.sudo().square_id,
            'square_journal_id': provider.journal_id.id,
            'square_outstanding_account_id': method_line.payment_account_id.id,
            'square_currency_code': self.currency_id.name,
            'square_amount_minor': payment_utils.to_minor_currency_units(order.amount_total, self.currency_id),
            'square_order_reference': f'odoo-web-{uuid4().hex[:23]}',
            'square_cart_signature': cart_signature,
        })

    def _get_processing_values(self):
        result = super()._get_processing_values()
        if self.provider_code != 'square_online':
            return result
        if not self.square_order_reference or not request:
            raise ValidationError(_("Square is only available in website checkout."))
        return {
            **result,
            'square_application_id': self.square_application_id,
            'square_location_id': self.square_location_ref,
            'square_verification': self._square_verification_details(),
            'square_submission': {
                'transaction_id': self.id,
                'submission_token': payment_utils.generate_access_token(
                    self.id, self.square_order_reference, request.session.sid,
                ),
            },
        }

    def _square_verification_details(self):
        self.ensure_one()
        partner = self.sale_order_ids.partner_invoice_id
        given_name, _, family_name = (partner.name or '').partition(' ')
        digits = 0 if self.square_currency_code == 'JPY' else 2
        billing = {
            'givenName': given_name,
            'familyName': family_name,
            'email': partner.email,
            'phone': partner.phone,
            'addressLines': [line for line in (partner.street, partner.street2) if line],
            'city': partner.city,
            'state': partner.state_id.code,
            'postalCode': partner.zip,
            'countryCode': partner.country_id.code,
        }
        return {
            'amount': format(self.square_amount_minor / 10**digits, f'.{digits}f'),
            'currencyCode': self.square_currency_code,
            'intent': 'CHARGE',
            'customerInitiated': True,
            'sellerKeyedIn': False,
            'billingContact': {key: val for key, val in billing.items() if val},
        }

    def _square_claim_checkout(self, order):
        self.ensure_one()
        provider = self.provider_id
        connection = self._square_validate_checkout(order)
        if (
            order._square_cart_signature() != self.square_cart_signature
            or connection != self.square_connection_id or connection.merchant_id != self.square_merchant_id
            or provider.square_environment != self.square_environment
            or provider.square_application_id != self.square_application_id
            or provider.square_location_id.sudo().square_id != self.square_location_ref
            or provider.journal_id != self.square_journal_id
        ):
            raise ValidationError(_("Checkout has changed. Please refresh the page and try again."))
        if order._square_checkout_is_reserved() or (order.transaction_ids - self).filtered(
            lambda t: t.state in ('pending', 'authorized', 'done')
            or (t.state == 'draft' and t.provider_code != 'square_online')
        ):
            raise UserError(_("Another payment has already started for this order."))

        order.square_online_transaction_id = self
        self.square_reserved_at = fields.Datetime.now()
        self._set_pending(state_message=_("Square is processing your payment. Please do not pay again."))

    def _square_money(self):
        self.ensure_one()
        return {'amount': self.square_amount_minor, 'currency': self.square_currency_code}

    def _square_apply_payment(self, payment):
        """Apply a verified Square payment obtained with the original merchant's credentials.

        The caller must hold the transaction row lock. A mismatch leaves the reservation alone.
        """
        self.ensure_one()
        if not isinstance(payment, dict):
            return False
        payment_id = payment.get('id')
        application_details = payment.get('application_details') or {}
        application_id = (
            application_details.get('application_id') if isinstance(application_details, dict) else None
        )
        status = payment.get('status')
        if (
            not self.square_dispatched_at or not isinstance(payment_id, str) or not payment_id
            or (self.provider_reference and self.provider_reference != payment_id)
            or payment.get('reference_id') != self.square_order_reference
            or payment.get('location_id') != self.square_location_ref
            or not self.square_order_id or payment.get('order_id') != self.square_order_id
            or (application_id and application_id != self.square_application_id)
            or payment.get('source_type') != 'CARD'
            or payment.get('amount_money') != self._square_money()
            or ('total_money' in payment and payment['total_money'] != self._square_money())
            or status not in ('COMPLETED', 'PENDING', 'APPROVED', 'CANCELED', 'FAILED')
        ):
            return False
        if self.state != 'pending':
            return self.provider_reference == payment_id
        self.write({'provider_reference': payment_id, 'square_recovery_message': False})
        if status == 'COMPLETED':
            # The merchant refund scan may already have passed refunds for an unbound payment.
            refunded_money = payment.get('refunded_money')
            self.square_linked_refunds_pending = bool(payment.get('refund_ids')) or (
                isinstance(refunded_money, dict) and bool(refunded_money.get('amount'))
            )
            self._set_done()
        elif status == 'CANCELED':
            self._set_canceled(state_message=_("Square canceled the payment. You can try again."))
        elif status == 'FAILED':
            self._set_error(_("Square declined the payment. Check your card or use a different payment method."))
        return True

    def _create_payment(self, **extra_create_values):
        self.ensure_one()
        if self.provider_code == 'square_online' and self.operation == 'online_direct' and self.square_journal_id:
            journal = self.square_journal_id
            lines = journal.inbound_payment_method_line_ids
            method = lines.filtered(lambda line: line.payment_provider_id == self.provider_id)[:1]
            method = method or lines.filtered(lambda line: line.code == 'manual')[:1]
            extra_create_values = {
                'journal_id': journal.id,
                'payment_method_line_id': method.id,
                'outstanding_account_id': self.square_outstanding_account_id.id,
                **extra_create_values,
            }
        return super()._create_payment(**extra_create_values)

# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from collections import defaultdict
from decimal import Decimal
from math import isclose, isfinite
from uuid import uuid4

from odoo import _, models
from odoo.exceptions import UserError
from odoo.tools.float_utils import float_compare, float_round


def minor_units(amount, digits):
    value = Decimal(str(amount)).scaleb(digits)
    if not value.is_finite() or value != value.to_integral_value():
        raise UserError(_("The Odoo amount cannot be represented in the Square location currency."))
    return int(value)


def decimal_string(amount):
    return format(Decimal(str(amount)).normalize(), 'f')


class PosOrderPricing(models.Model):
    _inherit = 'pos.order'

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
        can_edit_price = not config.restrict_price_control or config.group_pos_manager_id in self.env.user.all_group_ids
        discount_lines = self.env['pos.order.line']
        if 'discount_product_id' in config._fields and config.module_pos_discount and config.discount_product_id:
            discount_lines = self.lines.filtered(lambda line: line.product_id == config.discount_product_id)
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
        configured_options = {line: self._square_selected_options(line) for line in self.lines}
        combo_prices = self._square_combo_prices(pricelist, configured_options)
        self._square_check_tracking()
        lot_quantities = defaultdict(float)
        for line in self.lines.filtered(lambda line: line.product_id.tracking == 'lot'):
            lot_quantities[line.product_id] += line.qty
        unit_prices = {}
        manual_tax_fields = {'computation_key', 'manual_tax_amounts', 'manual_total_excluded', 'manual_total_excluded_currency'}
        for line in self.lines:
            product = line.product_id
            taxes = product.taxes_id._filter_taxes_by_company(self.company_id)
            global_discount = line in discount_lines
            options = configured_options[line]
            price_context = product._get_product_price_context(options)
            option_price_extra = price_context.get('no_variant_attributes_price_extra', 0)
            expected_extra = 0 if line.combo_parent_id else option_price_extra
            if line.extra_tax_data and (
                not isinstance(line.extra_tax_data, dict)
                or (not global_discount and manual_tax_fields.intersection(line.extra_tax_data))
            ):
                raise UserError(_("Square Terminal cannot verify a basket with manual tax overrides."))
            if (
                not all(isfinite(value) for value in (line.qty, line.price_unit, line.price_extra, line.discount))
                or line.qty <= 0 or (line.price_unit < 0 and not global_discount) or not 0 <= line.discount <= 100
                or (line.discount and not config.manual_discount)
                or (line.price_type == 'manual' and line not in tip_lines and not global_discount and not can_edit_price)
                or (global_discount and (
                    line.price_type != 'manual' or line.qty != 1 or line.price_unit >= 0 or line.discount
                    or line.combo_parent_id or line.combo_line_ids or line in tip_lines
                ))
                or float_compare(line.price_extra, expected_extra, precision_digits=price_digits)
                or line.refunded_orderline_id or (line.combo_item_id and line not in combo_prices)
                or (line.combo_parent_id and line not in combo_prices)
                or not product.active or not product.sale_ok
                or (not product.available_in_pos and not line.combo_parent_id and line not in tip_lines and not global_discount)
                or (product.company_id and product.company_id != self.company_id)
                or (not global_discount and set(taxes.ids) != set(line.tax_ids.ids))
            ):
                raise UserError(_("An Odoo basket item cannot be priced for Square Terminal."))
            if line in tip_lines:
                price = self.tip_amount
            elif line in combo_prices:
                price = combo_prices[line]
            elif line.price_type == 'manual':
                price = line.price_unit
            else:
                priced_product = product.with_context(**price_context)
                price = (
                    pricelist._get_product_price(priced_product, lot_quantities.get(product, line.qty)) if pricelist
                    else product.lst_price + option_price_extra
                )
            if float_compare(price, line.price_unit, precision_digits=price_digits):
                raise UserError(_("The Odoo basket price changed. Reload the register and check the sale."))
            unit_prices[line] = price if global_discount else float_round(price, precision_digits=price_digits)

        if discount_lines:
            discountable_lines = self.lines - (discount_lines | tip_lines)
            discountable_bases = discountable_lines._prepare_tax_base_line_values()
            for line, base in zip(discountable_lines, discountable_bases):
                base.update(price_unit=unit_prices[line], rate=1.0)
            self._square_check_global_discount_tax_data(discount_lines, discountable_bases, price_digits)

        base_lines = self.lines._prepare_tax_base_line_values()
        for line, base in zip(self.lines, base_lines):
            base['price_unit'] = unit_prices[line]
        account_tax._add_tax_details_in_base_lines(base_lines, self.company_id)
        discount_groups = {}
        groups_by_line = {}
        for line, base in zip(self.lines, base_lines):
            account_tax._round_base_lines_tax_details([base], self.company_id)
            details = base['tax_details']
            if (
                currency.compare_amounts(line.price_subtotal, details['total_excluded_currency'])
                or currency.compare_amounts(line.price_subtotal_incl, details['total_included_currency'])
            ):
                raise UserError(_("The Odoo basket price changed. Reload the register and check the sale."))
            if discount_lines and line not in tip_lines:
                taxes = base['tax_ids']
                discount_taxes = taxes.filtered(lambda tax: tax._can_be_discounted())
                group = discount_groups.setdefault(frozenset(discount_taxes.ids), {
                    'base': Decimal(0), 'tax_groups': set(), 'discounts': [], 'items': [],
                })
                groups_by_line[line] = group
                if line in discount_lines:
                    if discount_taxes != taxes:
                        raise UserError(_("The Odoo global discount has unexpected taxes."))
                    group['discounts'].append(-Decimal(str(line.price_unit)))
                else:
                    group['tax_groups'].add(frozenset(taxes.ids))
                    # Odoo's Global Discount starts from each line's total less additive taxes.
                    group['base'] += Decimal(str(details['total_included_currency'])) - sum(
                        Decimal(str(tax['tax_amount_currency'])) for tax in details['taxes_data']
                        if not tax['tax'].price_include
                    )

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
            item = {
                'product': line.product_id,
                'name': line.product_id.with_context(display_default_code=False).display_name[:255],
                'combo': line in combo_prices,
                'needs_summary': bool(
                    configured_options[line] or line.custom_attribute_value_ids or line.product_id.tracking != 'none'
                ),
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
            }
            line_items.append(item)
            if discount_lines:
                groups_by_line[line]['items'].append(item)
        expected_total = sum(line['total_minor'] for line in line_items)
        expected_tax = sum(line['tax_minor'] for line in line_items)
        self.with_context(backend_recomputation=True)._compute_prices()
        if (
            client_tax != expected_tax or client_total != expected_total + tip_minor
            or minor_units(self.amount_tax, digits) != expected_tax
            or minor_units(self.amount_total, digits) != expected_total + tip_minor
        ):
            raise UserError(_("The calculated Odoo amounts differ from the displayed basket."))
        if discount_lines:
            line_items = self._square_global_discount_basket(discount_groups.values(), digits, price_digits)
        return expected_total, line_items, tip_minor

    def _square_check_global_discount_tax_data(self, discount_lines, discounted_bases, price_digits):
        account_tax = self.env['account.tax']
        error = _("The Odoo global discount does not match the original basket.")
        percent = (discount_lines[0].extra_tax_data or {}).get('discount_percentage')
        if type(percent) not in (int, float) or not 0 < percent <= 100:
            raise UserError(error)

        # The POS tax engine builds the discount in the register currency with a rate of 1.
        account_tax._add_tax_details_in_base_lines(discounted_bases, self.company_id)
        account_tax._round_base_lines_tax_details(discounted_bases, self.company_id)
        expected = {
            frozenset(base['tax_ids'].ids): base
            for base in account_tax._prepare_global_discount_lines(
                discounted_bases, self.company_id, 'percent', percent,
            )
        }
        for line in discount_lines:
            base = expected.pop(frozenset(line.tax_ids.ids), None)
            if not base or float_compare(line.price_unit, base['price_unit'], precision_digits=price_digits):
                raise UserError(error)
            canonical = account_tax._export_base_line_extra_tax_data(base)
            canonical['discount_percentage'] = percent
            if line.extra_tax_data != canonical:
                raise UserError(error)
        if expected:
            raise UserError(error)

    def _square_global_discount_basket(self, groups, digits, price_digits):
        receipt_items = []
        min_rate, max_rate = Decimal(0), Decimal(1)
        price_tolerance = Decimal(5).scaleb(-price_digits - 1)
        for group in groups:
            base, discounts, items = group['base'], group['discounts'], group['items']
            discount = sum(discounts)
            if base > 0:
                margin = price_tolerance * max(1, len(discounts))
                min_rate = max(min_rate, (discount - margin) / base)
                max_rate = min(max_rate, (discount + margin) / base)
            if len(discounts) > len(group['tax_groups']) or (discount and base <= 0) or min_rate > max_rate:
                raise UserError(_("The Odoo global discount does not match the original basket."))
            if not discounts:
                receipt_items.extend(items)
                continue

            # Preserve the group's tax amounts when folding its negative discount lines into one Square line.
            representative = next((item for item in items if item['product'].type == 'combo'), items[0])
            taxed_items = [item for item in items if item['tax_minor']]
            taxes = taxed_items[0]['taxes'] if taxed_items else []
            receipt_items.append({
                **representative,
                'global_discount': True,
                'total_minor': sum(item['total_minor'] for item in items),
                'tax_minor': sum(item['tax_minor'] for item in items),
                'inclusive_tax_minor': sum(item['inclusive_tax_minor'] for item in items),
                'discount_minor': (
                    minor_units(self.currency_id.round(float(discount)), digits)
                    + sum(item['discount_minor'] for item in items)
                ),
                'taxes': taxes,
                'summary_tax': any(item['summary_tax'] or item['taxes'] != taxes for item in taxed_items),
            })
        return receipt_items

    def _square_selected_options(self, line):
        product = line.product_id
        template = product.product_tmpl_id
        variant_values = product.product_template_attribute_value_ids
        combination = variant_values | line.attribute_value_ids
        custom = line.custom_attribute_value_ids
        if not combination and not template.valid_product_template_attribute_line_ids and not custom:
            return combination
        selected_custom = combination.filtered('is_custom')
        if (
            not template._is_combination_possible(combination)
            or template._get_variant_for_combination(combination) != product
            or custom.custom_product_template_attribute_value_id != selected_custom
            or len(custom) != len(selected_custom)
        ):
            raise UserError(_("Square Terminal cannot verify these product options."))
        return combination - variant_values

    def _square_check_tracking(self):
        tracked = self.lines.filtered(lambda line: line.product_id.tracking != 'none')
        if not tracked:
            return
        picking_type = self.config_id.picking_type_id
        error = _("Set valid lot or serial numbers before sending this basket to Square Terminal.")
        if not (picking_type.use_existing_lots or picking_type.use_create_lots):
            raise UserError(error)
        serials = set()
        identifiers = set()
        for line in tracked:
            names = line.pack_lot_ids.mapped('lot_name')
            expected = line.qty if line.product_id.tracking == 'serial' else 1
            if len(names) != expected or any(not name or not name.strip() for name in names):
                raise UserError(error)
            line_identifiers = {(line.product_id.id, name) for name in names}
            if line.product_id.tracking == 'serial':
                if len(line_identifiers) != len(names) or serials.intersection(line_identifiers):
                    raise UserError(error)
                serials.update(line_identifiers)
            identifiers.update(line_identifiers)
        if picking_type.use_existing_lots and not picking_type.use_create_lots:
            existing = self.env['stock.lot'].sudo().with_context(skip_preprocess_gs1=True).search([
                ('product_id', 'in', tracked.product_id.ids),
                ('name', 'in', list({name for _, name in identifiers})),
                ('company_id', 'in', [False, self.company_id.id]),
            ])
            if not identifiers.issubset({(lot.product_id.id, lot.name) for lot in existing}):
                raise UserError(error)

    def _square_combo_prices(self, pricelist, configured_options):
        prices = {}
        error = _("The Odoo combo no longer matches its configured choices.")
        for parent in self.lines.filtered(lambda line: line.product_id.type == 'combo'):
            groups = parent.product_id.combo_ids
            children = parent.combo_line_ids.sorted('id')
            if (
                parent.combo_parent_id or parent.combo_item_id or not groups or not children
                or not isfinite(parent.qty) or parent.qty <= 0 or configured_options[parent]
                or any(
                    child not in self.lines or child.combo_item_id.combo_id not in groups
                    or child.product_id != child.combo_item_id.product_id
                    or not isfinite(child.qty) or child.qty <= 0
                    or any(
                        value.attribute_id.create_variant == 'dynamic' and value.price_extra
                        for value in child.product_id.product_template_attribute_value_ids
                    )
                    for child in children
                )
            ):
                raise UserError(error)
            included = []
            extras = []
            quantities = defaultdict(int)
            for child in children:
                group = child.combo_item_id.combo_id
                ratio = child.qty / parent.qty
                if not isfinite(ratio):
                    raise UserError(error)
                quantity = round(ratio)
                if quantity <= 0 or not isclose(ratio, quantity, rel_tol=1e-9):
                    raise UserError(error)
                before = quantities[group]
                quantities[group] += quantity
                if before < group.qty_free:
                    # POS creates separate lines when a choice spans the included and extra quantities.
                    if quantities[group] > group.qty_free:
                        raise UserError(error)
                    included.append((child, quantity))
                else:
                    extras.append((child, quantity))
            if any(not group.qty_free <= quantities[group] <= group.qty_max for group in groups):
                raise UserError(error)

            prices[parent] = 0
            product = parent.product_id
            price = pricelist._get_product_price(product, 1) if pricelist else product.lst_price
            # Follow the native POS allocation: included items share the combo base price.
            # Paid extras use the group base price and receive the allocation only when every group is optional.
            weight = sum(child.combo_item_id.combo_id.base_price * qty for child, qty in (included or extras))
            if not weight:
                raise UserError(error)
            remaining = price
            for child, quantity in included:
                item = child.combo_item_id
                share = self.currency_id.round(item.combo_id.base_price * price / weight)
                remaining -= share * quantity
                if child == included[-1][0]:
                    if quantity != 1:
                        raise UserError(error)
                    share += remaining
                prices[child] = share + item.extra_price + sum(configured_options[child].mapped('price_extra'))
            for child, quantity in extras:
                item = child.combo_item_id
                share = self.currency_id.round(item.combo_id.base_price)
                if not included and remaining:
                    allocation = self.currency_id.round(item.combo_id.base_price * price / weight)
                    share += allocation
                    remaining -= allocation * quantity
                    if child == extras[-1][0]:
                        share += remaining / quantity
                prices[child] = share + item.extra_price + sum(configured_options[child].mapped('price_extra'))
        return prices

    def _square_order_payload(self, location, reference, line_items):
        self.ensure_one()
        if self._square_needs_receipt_summary(line_items):
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
        count_unit = self.env.ref('uom.product_uom_unit')
        for index, line in enumerate(line_items, 1):
            product = line['product']
            uom = product.uom_id if product else count_unit
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
            if variation:
                unit_id = variation['item_variation_data'].get('measurement_unit_id')
                if isinstance(unit_id, str) and unit_id:
                    if unit_id not in units:
                        units[unit_id] = location._square_catalog_quantity_unit(unit_id)
                    catalog_uom, catalog_unit = units[unit_id] or (None, None)
                    if catalog_uom != uom or precision > catalog_unit['precision']:
                        variation = catalog_unit = None
                elif unit_id is not None or precision or uom != count_unit:
                    variation = None
            if variation:
                payload['catalog_object_id'] = variation['id']
                if variation.get('version'):
                    payload['catalog_version'] = variation['version']
            else:
                payload['name'] = line['name']
                if precision or uom != count_unit:
                    name = uom.name
                    payload['quantity_unit'] = {
                        'measurement_unit': {'custom_unit': {'name': name[:50], 'abbreviation': name[:10]}},
                        'precision': precision,
                    }
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
            if variation:
                expected[uid]['quantity_unit'] = catalog_unit or False
        return {'idempotency_key': str(uuid4()), 'order': order}, {'lines': expected}

    def _square_needs_receipt_summary(self, line_items):
        return any(
            line.get('global_discount') or line['combo'] or line['needs_summary']
            or line['unit_minor'] is None or len(line['quantity']) > 12
            or len(line['quantity'].partition('.')[2]) > 5 or line['summary_tax']
            for line in line_items
        )

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

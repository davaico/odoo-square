from urllib.parse import quote

from odoo import _, api, fields, models

from ..const import SQUARE_LOCATION_CURRENCIES


SQUARE_STANDARD_UOMS = {
    ('generic_unit', 'UNIT'): 'uom.product_uom_unit',
    ('weight_unit', 'METRIC_GRAM'): 'uom.product_uom_gram',
    ('weight_unit', 'METRIC_KILOGRAM'): 'uom.product_uom_kgm',
    ('weight_unit', 'IMPERIAL_WEIGHT_OUNCE'): 'uom.product_uom_oz',
    ('weight_unit', 'IMPERIAL_POUND'): 'uom.product_uom_lb',
    ('length_unit', 'METRIC_MILLIMETER'): 'uom.product_uom_millimeter',
    ('length_unit', 'METRIC_CENTIMETER'): 'uom.product_uom_cm',
    ('length_unit', 'METRIC_METER'): 'uom.product_uom_meter',
    ('length_unit', 'METRIC_KILOMETER'): 'uom.product_uom_km',
    ('length_unit', 'IMPERIAL_INCH'): 'uom.product_uom_inch',
    ('length_unit', 'IMPERIAL_FOOT'): 'uom.product_uom_foot',
    ('length_unit', 'IMPERIAL_YARD'): 'uom.product_uom_yard',
    ('length_unit', 'IMPERIAL_MILE'): 'uom.product_uom_mile',
    ('area_unit', 'METRIC_SQUARE_METER'): 'uom.uom_square_meter',
    ('area_unit', 'IMPERIAL_SQUARE_FOOT'): 'uom.uom_square_foot',
    ('volume_unit', 'METRIC_LITER'): 'uom.product_uom_litre',
    ('volume_unit', 'IMPERIAL_CUBIC_INCH'): 'uom.product_uom_cubic_inch',
    ('volume_unit', 'IMPERIAL_CUBIC_FOOT'): 'uom.product_uom_cubic_foot',
}


class SquareLocation(models.Model):
    _name = 'pos.square.location'
    _description = 'Square Terminal Location'
    _order = 'name, id'

    connection_id = fields.Many2one('pos.square.connection', required=True, ondelete='cascade')
    company_id = fields.Many2one(related='connection_id.company_id', store=True, index=True)
    square_id = fields.Char(required=True)
    name = fields.Char(required=True)
    country_code = fields.Char()
    currency_code = fields.Char()
    is_available = fields.Boolean()
    unavailable_reason = fields.Char()

    _sql_constraints = [
        ('connection_square_id', 'unique(connection_id, square_id)', 'This Square location is already on this connection.'),
    ]

    def _square_catalog_variation(self, sku):
        self.ensure_one()
        result = self.connection_id._request('POST', '/v2/catalog/search', payload={
            'object_types': ['ITEM_VARIATION'],
            'include_related_objects': True,
            'query': {'exact_query': {'attribute_name': 'sku', 'attribute_value': sku}},
        })
        objects = result.get('objects', [])
        if not isinstance(objects, list) or len(objects) != 1 or result.get('cursor'):
            return None
        variation = objects[0]
        if not (
            isinstance(variation, dict) and variation.get('type') == 'ITEM_VARIATION'
            and isinstance(variation.get('id'), str) and variation['id']
            and self._square_catalog_object_available(variation)
        ):
            return None
        data = variation.get('item_variation_data')
        if not isinstance(data, dict) or data.get('sku') != sku or not data.get('sellable', True):
            return None
        related = result.get('related_objects') or []
        parent = next((
            item for item in related
            if isinstance(item, dict) and item.get('type') == 'ITEM' and item.get('id') == data.get('item_id')
        ), None)
        if (
            not parent or not self._square_catalog_object_available(parent)
            or (parent.get('item_data') or {}).get('is_archived')
        ):
            return None
        return variation

    def _square_catalog_object_available(self, obj):
        self.ensure_one()
        if obj.get('is_deleted'):
            return False
        if obj.get('present_at_all_locations', True):
            return self.square_id not in (obj.get('absent_at_location_ids') or [])
        return self.square_id in (obj.get('present_at_location_ids') or [])

    def _square_catalog_quantity_unit(self, unit_id):
        self.ensure_one()
        path = '/v2/catalog/object/' + quote(unit_id, safe='')
        unit = self.connection_id._request('GET', path).get('object')
        if (
            not isinstance(unit, dict) or unit.get('id') != unit_id or unit.get('type') != 'MEASUREMENT_UNIT'
            or not self._square_catalog_object_available(unit)
        ):
            return None
        data = unit.get('measurement_unit_data')
        if not isinstance(data, dict) or not isinstance(data.get('measurement_unit'), dict):
            return None
        digits = data.get('precision', 3)
        measurements = [(field, value) for field, value in data['measurement_unit'].items() if field.endswith('_unit')]
        if len(measurements) != 1 or type(digits) is not int or not 0 <= digits <= 5:
            return None
        field, value = measurements[0]
        unit_type = data['measurement_unit'].get('type')
        if unit_type is not None and unit_type != f"TYPE_{field.removesuffix('_unit').upper()}":
            return None
        xml_id = SQUARE_STANDARD_UOMS.get((field, value)) if isinstance(value, str) else None
        if not xml_id:
            return None
        return self.env.ref(xml_id), {
            'catalog_object_id': unit_id, 'precision': digits, 'measurement_unit': {field: value},
        }

    @api.model
    def _square_location_values(self, response, merchant_id):
        locations = response.get('locations', [])
        if not isinstance(locations, list):
            raise ValueError('Invalid Square locations response')
        result = {}
        for location in locations:
            if not isinstance(location, dict) or location.get('merchant_id') != merchant_id:
                raise ValueError('Square location belongs to a different merchant')
            square_id, country, currency = (location.get(key) for key in ('id', 'country', 'currency'))
            if not isinstance(square_id, str) or not square_id or square_id in result:
                raise ValueError('Invalid Square location ID')
            if not all(value is None or isinstance(value, str) for value in (country, currency)):
                raise ValueError('Invalid Square location currency or country')
            capabilities = location.get('capabilities')
            reason = False
            if location.get('status') != 'ACTIVE':
                reason = _("This Square location is inactive.")
            elif not country or not currency or SQUARE_LOCATION_CURRENCIES.get(country) != currency:
                reason = _("Square Terminal with linked orders is unavailable for this location's country or currency.")
            elif not isinstance(capabilities, list) or 'CREDIT_CARD_PROCESSING' not in capabilities:
                reason = _("This Square location cannot process credit cards.")
            result[square_id] = {
                'name': location.get('name') or square_id, 'country_code': country, 'currency_code': currency,
                'is_available': not reason, 'unavailable_reason': reason,
            }
        return result

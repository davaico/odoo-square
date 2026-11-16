# Square–Odoo Integration

Square sales are reflected in Odoo sales records, accounting, and inventory.

## Language

**Square checkout**:
The completion of a customer's sale in Square POS. It is the checkout covered by this integration's acceptance criteria.
_Avoid_: Odoo POS checkout, unqualified POS checkout

**Square payment**:
Money collected from a customer through Square for a sale.

**Odoo payment record**:
The accounting record in Odoo representing money already collected or refunded through Square.
_Avoid_: Square charge

**Square refund**:
A refund initiated in Square whose outcome is reflected in Odoo accounting and, where applicable, returned stock.

**Version port**:
An adaptation of the existing integration to another Odoo major version while preserving its supported business workflows. It does not include migrating an existing Odoo database.

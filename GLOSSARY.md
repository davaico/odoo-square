# Square–Odoo Integration

**Square checkout**:
Completion of a sale started in Square POS. Square POS Integration imports these sales into Odoo.

**Square Terminal checkout**:
A request from Odoo POS to collect one card share on a paired Square Terminal. Multiple card shares can belong to the same Odoo sale and linked Square order.

**Square payment**:
Money collected from a customer through Square for a sale.

**Odoo payment record**:
The accounting record in Odoo representing money already collected or refunded through Square.
_Avoid_: Square charge

**Square refund**:
Money returned through Square against an original payment. For a Square Terminal payment, the manager reviews the refund in Odoo and decides which accounting correction or stock return to record.

**Version port**:
An adaptation of the existing integration to another Odoo major version while preserving its supported business workflows. It does not include migrating an existing Odoo database.

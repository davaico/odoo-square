# Square–Odoo Integration

**Square checkout** means completion of a sale started in Square POS. Square POS Integration imports these sales into Odoo.

**Square Terminal checkout** is a request from Odoo POS to collect one card share on a paired terminal. Multiple card shares can belong to the same Odoo sale and linked Square order.

**Square payment** is money collected from a customer through Square for a sale.

**Odoo payment record** is the accounting record in Odoo for money already collected or refunded through Square.

**Square refund** is money returned through Square against an original payment. For a Square Terminal payment, a manager reviews the refund in Odoo and decides which accounting correction or stock return to record.

**Version port** is an adaptation of the integration to another Odoo major version that preserves the supported workflows on that version. Installed databases follow Odoo’s separate database upgrade process.

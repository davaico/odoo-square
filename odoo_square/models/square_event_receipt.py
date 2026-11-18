from odoo import api, fields, models


class SquareEventReceipt(models.Model):
    _name = "square.event.receipt"
    _description = "Applied Square POS Operation"

    company_id = fields.Many2one("res.company", required=True)
    operation = fields.Char(required=True)
    resource_id = fields.Char(required=True)
    event_key = fields.Char(required=True)

    _unique_operation = models.Constraint(
        "UNIQUE(company_id, operation, resource_id, event_key)",
        "This Square operation has already been applied.",
    )

    @api.model
    def _was_applied(self, operation, resource_id, event_key):
        return bool(resource_id and event_key and self.sudo().search_count([
            ("company_id", "=", self.env.company.id),
            ("operation", "=", operation),
            ("resource_id", "=", resource_id),
            ("event_key", "=", event_key),
        ], limit=1))

    @api.model
    def _record_applied(self, operation, resource_id, event_key):
        if resource_id and event_key:
            self.sudo().create({
                "company_id": self.env.company.id,
                "operation": operation,
                "resource_id": resource_id,
                "event_key": event_key,
            })

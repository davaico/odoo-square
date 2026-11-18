from odoo import fields, models


class SquareWebhookEvent(models.Model):
    _name = "square.webhook.event"
    _description = "Applied Square Webhook Event"

    config_id = fields.Many2one("square.config", required=True, ondelete="cascade")
    event_key = fields.Char(required=True)

    _sql_constraints = [
        ("unique_event", "UNIQUE(config_id, event_key)", "Square event was already applied."),
    ]

    def _apply_once(self, config, event_key, process, *, duplicate_status="already_processed"):
        """Claim before side effects; keep the receipt only after successful processing."""
        if not config or not event_key:
            return process()
        # ON CONFLICT serializes identical deliveries. Odoo retries a transaction
        # if the winning receipt is newer than its repeatable-read snapshot.
        with self.env.cr.savepoint():
            self.env.cr.execute(
                """
                INSERT INTO square_webhook_event (config_id, event_key)
                VALUES (%s, %s) ON CONFLICT (config_id, event_key) DO NOTHING
                RETURNING id
                """,
                (config.id, event_key),
            )
            row = self.env.cr.fetchone()
            if not row:
                return {"status": duplicate_status}
            result = process()
            if result.get("status") not in ("success", "updated", "already_processed", "skipped"):
                self.sudo().browse(row[0]).unlink()
            return result

    def _apply_provider_event(self, config, event_id, process):
        return self._apply_once(config, "event:" + event_id if event_id else None, process)

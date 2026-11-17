# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo import _, api, Command, fields, models
from odoo.exceptions import AccessError, ValidationError
from odoo.http import request

from odoo.addons.payment import utils as payment_utils


_CONNECTION_BY_STATE = {
    "test": "square_online_sandbox_connection_id",
    "enabled": "square_online_production_connection_id",
}


class PaymentProvider(models.Model):
    _inherit = "payment.provider"

    code = fields.Selection(
        selection_add=[("square_online", "Square Online")],
        ondelete={"square_online": "set default"},
    )
    square_online_sandbox_connection_id = fields.Many2one(
        "square.online.connection", string="Sandbox Connection", check_company=True,
        groups="base.group_system", copy=False, ondelete="restrict",
    )
    square_online_production_connection_id = fields.Many2one(
        "square.online.connection", string="Production Connection", check_company=True,
        groups="base.group_system", copy=False, ondelete="restrict",
    )

    def copy_data(self, default=None):
        values_list = super().copy_data(default=default)
        if not default or "payment_method_ids" not in default:
            for provider, values in zip(self.with_context(active_test=False), values_list):
                if values is not None and provider.code == "square_online":
                    # Card may still be inactive when an administrator duplicates the provider.
                    methods = provider.payment_method_ids._filtered_access("read")
                    values["payment_method_ids"] = [Command.set(methods.ids)]
        return values_list

    def write(self, vals):
        # Odoo resets provider codes during module removal, before the registry is ready.
        if self.env.registry.ready and {"company_id", "code"} & vals.keys():
            changing = self.filtered(lambda provider: provider.code == "square_online" and (
                vals.get("company_id", provider.company_id.id) != provider.company_id.id
                or vals.get("code", provider.code) != provider.code
            ))
            if changing:
                changing.check_access("write")
                if self.env["payment.transaction"].sudo().search_count([
                    ("provider_id", "in", changing.ids),
                    ("square_online_connection_id", "!=", False),
                ], limit=1):
                    raise ValidationError(_(
                        "A Square provider with payment transactions must keep its company and provider type. "
                        "Duplicate the provider to configure a different company."
                    ))
        return super().write(vals)

    def _compute_feature_support_fields(self):
        super()._compute_feature_support_fields()
        self.filtered(lambda provider: provider.code == "square_online").support_refund = "full_only"

    def _get_default_payment_method_codes(self):
        self.ensure_one()
        if self.code == "square_online":
            return {"card"}
        return super()._get_default_payment_method_codes()

    def _square_online_get_connection(self):
        self.ensure_one()
        field_name = _CONNECTION_BY_STATE.get(self.state) if self.code == "square_online" else None
        if field_name:
            return self.sudo()[field_name]
        return self.env["square.online.connection"].sudo()

    def _square_online_has_compatible_journal(self, connection):
        self.ensure_one()
        journal = self.journal_id
        return (
            journal
            and journal.type == "bank"
            and journal.active
            and journal.company_id == self.company_id
            and (not journal.currency_id or journal.currency_id.name == connection.location_id.currency_code)
        )

    @api.constrains(
        "code", "company_id", "website_id", "square_online_sandbox_connection_id",
        "square_online_production_connection_id",
    )
    def _check_square_online_connection_ownership(self):
        for provider in self.filtered(lambda item: item.code == "square_online"):
            if provider.website_id and provider.website_id.company_id != provider.company_id:
                raise ValidationError(_("Choose a website owned by the Square provider's company."))
            for environment, connection in (
                ("sandbox", provider.sudo().square_online_sandbox_connection_id),
                ("production", provider.sudo().square_online_production_connection_id),
            ):
                if connection and (
                    connection.company_id != provider.company_id or connection.environment != environment
                ):
                    raise ValidationError(_(
                        "Choose a Square connection for this company and environment."
                    ))

    def _check_required_if_provider(self):
        super()._check_required_if_provider()
        for provider in self.filtered(lambda item: item.code == "square_online" and item.state != "disabled"):
            connection = provider._square_online_get_connection()
            if not connection or not connection._square_online_is_ready():
                raise ValidationError(_(
                    "Authorize Square and select an eligible location for this environment."
                ))
            if not provider._square_online_has_compatible_journal(connection):
                raise ValidationError(_(
                    "Choose an active bank journal in this company. A fixed journal currency must "
                    "match the Square location's processing currency."
                ))

    @api.model
    def _get_compatible_providers(
        self, company_id, partner_id, amount, currency_id=None, website_id=None, report=None, **kwargs
    ):
        providers = super()._get_compatible_providers(
            company_id, partner_id, amount, currency_id=currency_id,
            website_id=website_id, report=report, **kwargs,
        )
        currency_code = self.env["res.currency"].browse(currency_id).name
        for provider in providers.filtered(lambda item: item.code == "square_online"):
            connection = provider._square_online_get_connection()
            if (
                provider.company_id.id != company_id
                or (request and not self.env.context.get("square_online_website_checkout"))
                or (provider.website_id and provider.website_id.id != website_id)
                or not connection
                or not connection._square_online_is_ready()
                or not provider._square_online_has_compatible_journal(connection)
                or (currency_code and connection.location_id.currency_code != currency_code)
            ):
                providers -= provider
                payment_utils.add_to_report(
                    report, provider, available=False,
                    reason=_("No ready Square connection for this company, website, and currency."),
                )
        return providers

    def action_square_online_connections(self):
        self.ensure_one()
        self.check_access("write")
        if self.code != "square_online" or not self.env.user.has_group("base.group_system"):
            raise AccessError(_("Only administrators can manage Square connections."))
        action = self.env["ir.actions.actions"]._for_xml_id(
            "payment_square_online.square_online_connection_action"
        )
        return {
            **action,
            "domain": [("company_id", "=", self.company_id.id)],
            "context": {"default_company_id": self.company_id.id},
        }

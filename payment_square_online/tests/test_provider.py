# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestSquareOnlineProvider(TransactionCase):

    def test_installs_disabled_with_native_card_payment_method(self):
        provider = self.env.ref("payment_square_online.payment_provider_square_online")
        self.assertEqual(provider.state, "disabled")
        self.assertEqual(provider.code, "square_online")
        self.assertEqual(provider.with_context(active_test=False).payment_method_ids.code, "card")
        dependencies = self.env.ref("base.module_payment_square_online").dependencies_id
        self.assertNotIn("odoo_square", dependencies.mapped("name"))

    def test_cannot_enable_provider_with_incomplete_connection(self):
        provider = self.env.ref("payment_square_online.payment_provider_square_online")
        connection = self.env["square.online.connection"].create({
            "name": "Unconnected Sandbox",
            "company_id": provider.company_id.id,
            "application_id": "sandbox-sq0idp-unconnected",
        })
        provider.square_online_sandbox_connection_id = connection
        with self.assertRaises(ValidationError):
            with self.env.cr.savepoint():
                provider.state = "test"
        self.assertEqual(provider.state, "disabled")

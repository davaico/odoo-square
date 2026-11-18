# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
from odoo.addons.payment import reset_payment_provider, setup_provider

from . import controllers, models


def post_init_hook(env):
    setup_provider(env, "square_online")


def uninstall_hook(env):
    reset_payment_provider(env, "square_online")

import re
from datetime import datetime, timezone
from urllib.parse import quote
from uuid import uuid4

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError

from ..const import SQUARE_SANDBOX_DEVICE_ID
from ..square_api import SquareAPIError


class SquareTerminal(models.Model):
    _name = 'pos.square.terminal'
    _description = 'Square Terminal Device'
    _check_company_auto = True

    name = fields.Char(required=True, size=128)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(related='connection_id.company_id', store=True, index=True)
    connection_id = fields.Many2one('pos.square.connection', required=True, ondelete='restrict')
    location_id = fields.Many2one('pos.square.location', required=True, ondelete='restrict', check_company=True)
    environment = fields.Selection(related='connection_id.environment')
    pairing_code_id = fields.Char(string='Square Pairing Reference', readonly=True, copy=False, groups='point_of_sale.group_pos_manager')
    pairing_code = fields.Char(readonly=True, copy=False, groups='point_of_sale.group_pos_manager')
    pair_by = fields.Datetime(readonly=True, copy=False)
    device_id = fields.Char(readonly=True, copy=False, index=True)
    application_version = fields.Char(readonly=True, copy=False)
    readiness = fields.Selection([
        ('unpaired', 'Unpaired'), ('awaiting_pairing', 'Awaiting pairing'), ('expired', 'Code expired'),
        ('ready', 'Ready'), ('sandbox', 'Sandbox simulator ready'),
        ('incompatible', 'Incompatible'), ('offline', 'Unavailable'), ('failed', 'Check failed'),
    ], required=True, default='unpaired', readonly=True, copy=False)
    check_message = fields.Char(readonly=True, copy=False)
    last_checked_at = fields.Datetime(readonly=True, copy=False)
    unavailable_reason = fields.Char(compute='_compute_unavailable_reason')

    _provider_fields = {
        'pairing_code_id', 'pairing_code', 'pair_by', 'device_id', 'application_version',
        'readiness', 'check_message', 'last_checked_at', 'company_id', 'environment',
    }

    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.su:
            if any(
                self._provider_fields & vals.keys() for vals in vals_list
            ) or any(f'default_{field}' in self.env.context for field in self._provider_fields):
                raise AccessError(_("Verify the Square Terminal through the pairing actions."))
            vals_list = [{
                'pairing_code_id': False, 'pairing_code': False, 'pair_by': False, 'device_id': False,
                'application_version': False, 'readiness': 'unpaired', 'check_message': False,
                'last_checked_at': False, **vals,
            } for vals in vals_list]
        return super().create(vals_list)

    def write(self, vals):
        if not self.env.su and self._provider_fields & vals.keys():
            raise AccessError(_("Verify the Square Terminal through the pairing actions."))
        if {'connection_id', 'location_id'} & vals.keys():
            self.check_access('write')
            for terminal in self:
                terminal._lock()
                if (terminal.pairing_code_id or terminal.device_id) and any(
                    vals[name] != terminal[name].id for name in {'connection_id', 'location_id'} & vals.keys()
                ):
                    raise ValidationError(_("Create a new terminal to change its Square connection or location."))
        return super().write(vals)

    @api.constrains('connection_id', 'location_id')
    def _check_location_connection(self):
        for terminal in self:
            if terminal.location_id.connection_id != terminal.connection_id:
                raise ValidationError(_("Select a location from this Square connection."))

    @api.depends(
        'active', 'connection_id.active', 'connection_id.authorization_result', 'location_id.is_available',
        'location_id.unavailable_reason', 'device_id', 'readiness', 'check_message', 'pair_by',
    )
    def _compute_unavailable_reason(self):
        for terminal in self:
            terminal.unavailable_reason = terminal._readiness_error()

    def _scope_error(self):
        self.ensure_one()
        if not self.active or not self.connection_id.active:
            return _("This Square Terminal or its connection is archived.")
        if self.location_id.connection_id != self.connection_id:
            return _("Select a location from this Square connection.")
        if not self.location_id.is_available:
            return self.location_id.unavailable_reason or _("This Square location is no longer available.")
        return self.connection_id._authorization_error()

    def _readiness_error(self):
        self.ensure_one()
        error = self._scope_error()
        if error:
            return error
        if self.device_id and self.readiness in ('ready', 'sandbox'):
            return False
        if self.readiness == 'awaiting_pairing' and self.pair_by and self.pair_by <= fields.Datetime.now():
            return _("The pairing code expired. Generate a new code.")
        return self.check_message or _("Pair the Square Terminal and check its readiness.")

    def _lock(self):
        self.ensure_one()
        self.env.cr.execute('SELECT id FROM pos_square_terminal WHERE id = %s FOR NO KEY UPDATE', [self.id])

    def _prepare_pairing_action(self):
        self.ensure_one()
        self.check_access('write')
        self._lock()
        error = self._scope_error()
        if error:
            raise UserError(error)

    def action_create_pairing_code(self):
        self._prepare_pairing_action()
        if self.environment != 'production':
            raise UserError(_("Physical Square Terminals require a Production connection. Use the Sandbox simulator for testing."))
        if self.device_id:
            raise UserError(_("This terminal already has a verified device. Create a new terminal for a replacement."))
        try:
            response = self.connection_id._request('POST', '/v2/devices/codes', payload={
                'idempotency_key': str(uuid4()),
                'device_code': {
                    'name': self.name, 'product_type': 'TERMINAL_API', 'location_id': self.location_id.square_id,
                },
            })
            pairing = self._pairing(response)
            if pairing.get('status') != 'UNPAIRED' or not isinstance(pairing.get('code'), str) or not pairing['code']:
                raise ValueError('Invalid Square pairing code')
            pair_by = datetime.fromisoformat(pairing['pair_by'])
            if not pair_by.tzinfo:
                raise ValueError('Invalid Square pairing deadline')
            pair_by = pair_by.astimezone(timezone.utc).replace(tzinfo=None)
            if pair_by <= fields.Datetime.now():
                self._record_check('expired', _("The pairing code expired. Generate a new code."))
                return True
        except SquareAPIError as error:
            self._record_check('failed', self._square_error_message(error))
        except (ValueError, TypeError, KeyError):
            self._record_check('incompatible', _("Square did not return a Terminal API pairing code for the selected location."))
        else:
            self._record_check(
                'awaiting_pairing', _("Enter the code on the Square Terminal, then check readiness."),
                pairing_code_id=pairing['id'], pairing_code=pairing['code'], pair_by=pair_by,
            )
        return True

    def action_use_sandbox_device(self):
        self._prepare_pairing_action()
        if self.environment != 'sandbox':
            raise UserError(_("The Square simulator requires a Sandbox connection."))
        self._record_check('sandbox', device_id=SQUARE_SANDBOX_DEVICE_ID)
        return True

    def action_check_readiness(self):
        self._prepare_pairing_action()
        if self.environment == 'sandbox':
            raise UserError(_("Use the Sandbox simulator to verify test configuration."))
        if not self.pairing_code_id:
            raise UserError(_("Generate a pairing code first."))
        try:
            if self.device_id:
                # Once paired, Devices is the authority on the active Terminal API session.
                self._check_device(self.device_id)
            else:
                self._check_pairing()
        except SquareAPIError as error:
            if error.status == 404 and not self.device_id and self.pair_by and self.pair_by <= fields.Datetime.now():
                self._record_check('expired', _("The pairing code expired. Generate a new code."), pairing_code=False)
            elif error.status == 404 and self.device_id:
                self._record_check('incompatible', _("Square cannot find this device in the connected merchant account."))
            else:
                self._record_check('failed', self._square_error_message(error))
        except ValueError:
            self._record_check('incompatible', _("Square reported a device or pairing from a different connection or location."))
        return True

    def _pairing(self, response, expected_id=None):
        pairing = response.get('device_code')
        if not (
            isinstance(pairing, dict) and isinstance(pairing.get('id'), str) and pairing['id']
            and (expected_id is None or pairing['id'] == expected_id)
            and pairing.get('product_type') == 'TERMINAL_API'
            and pairing.get('location_id') == self.location_id.square_id
        ):
            raise ValueError('Invalid Square device-code scope')
        return pairing

    def _check_pairing(self):
        pairing_code_id = quote(self.pairing_code_id, safe='')
        response = self.connection_id._request('GET', f'/v2/devices/codes/{pairing_code_id}')
        pairing = self._pairing(response, self.pairing_code_id)
        status = pairing.get('status')
        if status == 'PAIRED':
            device_id = pairing.get('device_id')
            if not isinstance(device_id, str) or not device_id:
                raise ValueError('Missing paired device ID')
            self._check_device(device_id)
        elif status == 'EXPIRED' or (
            status == 'UNPAIRED' and self.pair_by and self.pair_by <= fields.Datetime.now()
        ):
            self._record_check('expired', _("The pairing code expired. Generate a new code."), pairing_code=False)
        elif status == 'UNPAIRED':
            self._record_check('awaiting_pairing', _("Enter the code on the Square Terminal, then check readiness."))
        else:
            self._record_check('failed', _("Square could not confirm the pairing status. Check again."))

    def _check_device(self, device_id):
        prefixed_id = 'device:' + device_id.removeprefix('device:')
        path = '/v2/devices/' + quote(prefixed_id, safe='')
        device = self.connection_id._request('GET', path).get('device')
        if not isinstance(device, dict) or device.get('id') != prefixed_id:
            raise ValueError('Invalid Square device ID')
        attributes = device.get('attributes')
        if not isinstance(attributes, dict) or attributes.get('merchant_token', self.connection_id.merchant_id) != self.connection_id.merchant_id:
            raise ValueError('Invalid Square device merchant')
        if attributes.get('type') != 'TERMINAL':
            self._record_check('incompatible', _("Select a Square Terminal that supports the Terminal API."), pairing_code=False)
            return
        components = device.get('components')
        if not isinstance(components, list):
            components = []
        applications = [
            component['application_details'] for component in components
            if isinstance(component, dict) and component.get('type') == 'APPLICATION'
            and isinstance(component.get('application_details'), dict)
            and component['application_details'].get('application_type') == 'TERMINAL_API'
        ]
        if len(applications) != 1:
            self._record_check('incompatible', _("Sign in to the device using this Terminal API pairing code."), pairing_code=False)
            return
        application = applications[0]
        if (
            application.get('session_location') != self.location_id.square_id
            or application.get('device_code_id', self.pairing_code_id) != self.pairing_code_id
        ):
            raise ValueError('Invalid Square device location or pairing')
        version = application.get('version')
        match = re.match(r'^(\d+)\.(\d+)(?:\.|$)', version) if isinstance(version, str) else None
        if not match or tuple(map(int, match.groups())) < (6, 62):
            self._record_check(
                'incompatible', _("Update the Square Terminal software to version 6.62 or later for linked orders."),
                pairing_code=False,
            )
            return
        status = device.get('status')
        if not isinstance(status, dict) or status.get('category') != 'AVAILABLE':
            self._record_check(
                'offline', _("Square reports the terminal is unavailable. Check the device's connection and try again."),
                pairing_code=False,
            )
            return
        self._record_check(
            'ready', device_id=device_id, application_version=version, pairing_code=False, pair_by=False,
        )

    def _record_check(self, readiness, message=False, **values):
        self.sudo().write({
            **values, 'readiness': readiness, 'check_message': message, 'last_checked_at': fields.Datetime.now(),
        })

    @staticmethod
    def _square_error_message(error):
        if error.is_authorization_error:
            return _("Reconnect Square and grant the requested device and payment permissions.")
        return _("Square did not confirm terminal readiness. Try again.")

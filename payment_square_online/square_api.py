# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).
import re
from decimal import Decimal, DecimalException, Inexact, localcontext

import requests


SQUARE_API_VERSION = "2026-09-16"
SQUARE_HOSTS = {
    "sandbox": "https://connect.squareupsandbox.com",
    "production": "https://connect.squareup.com",
}
OAUTH_CALLBACK_PATH = "/payment/square_online/oauth/callback"
SQUARE_ONLINE_WEBHOOK_PATH = "/payment/square_online/webhook"
OAUTH_SCOPES = ("MERCHANT_PROFILE_READ", "PAYMENTS_READ", "PAYMENTS_WRITE")
CURRENCY_BY_COUNTRY = {
    "AU": "AUD",
    "CA": "CAD",
    "ES": "EUR",
    "FR": "EUR",
    "GB": "GBP",
    "IE": "EUR",
    "JP": "JPY",
    "US": "USD",
}
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
_AUTHORIZATION_ERROR_CODES = {"ACCESS_TOKEN_EXPIRED", "ACCESS_TOKEN_REVOKED", "INSUFFICIENT_SCOPES"}


class SquareAPIError(Exception):
    """A Square failure with no request or response body in the exception."""

    def __init__(self, status_code=None, codes=()):
        super().__init__("The Square request did not succeed.")
        self.status_code = status_code
        self.codes = tuple(codes)

    @property
    def authorization_failed(self):
        return self.status_code == 401 or bool(_AUTHORIZATION_ERROR_CODES.intersection(self.codes))


def square_request(environment, method, path, *, token=None, payload=None, params=None):
    headers = {"Square-Version": SQUARE_API_VERSION, "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        response = requests.request(
            method,
            SQUARE_HOSTS[environment] + path,
            headers=headers,
            json=payload,
            params=params,
            timeout=(5, 15),
            allow_redirects=False,
        )
        content = response.json()
    except requests.RequestException:
        raise SquareAPIError() from None
    except ValueError:
        raise SquareAPIError(response.status_code) from None

    if not isinstance(content, dict):
        raise SquareAPIError(response.status_code)
    errors = content.get("errors")
    if not 200 <= response.status_code < 300 or errors:
        codes = []
        for error in errors if isinstance(errors, list) else []:
            code = error.get("code") if isinstance(error, dict) else None
            if isinstance(code, str) and _ERROR_CODE.fullmatch(code):
                codes.append(code)
        raise SquareAPIError(response.status_code, codes)
    return content


MINOR_UNIT_DIGITS = {currency: 0 if currency == "JPY" else 2 for currency in CURRENCY_BY_COUNTRY.values()}


def square_amount_in_minor_units(amount, currency_code):
    try:
        digits = MINOR_UNIT_DIGITS[currency_code]
        value = Decimal(str(amount))
        if not value.is_finite():
            raise ValueError("Square amounts must be finite.")
        with localcontext() as context:
            context.traps[Inexact] = True
            return int(value.quantize(Decimal(1).scaleb(-digits)).scaleb(digits))
    except (DecimalException, KeyError) as exc:
        raise ValueError("Invalid Square checkout amount or currency.") from exc

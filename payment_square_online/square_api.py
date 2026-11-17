from requests import RequestException, request

from .const import SQUARE_API_VERSION, SQUARE_BASE_URLS


class SquareAPIError(Exception):
    def __init__(self, status=None, codes=()):
        super().__init__('Square API request failed')
        self.status = status
        self.codes = codes

    @property
    def is_authorization_error(self):
        return self.status in (401, 403)


def square_request(environment, method, path, *, access_token=None, payload=None, params=None):
    headers = {'Square-Version': SQUARE_API_VERSION, 'Accept': 'application/json'}
    if access_token:
        headers['Authorization'] = f'Bearer {access_token}'
    try:
        response = request(
            method, SQUARE_BASE_URLS[environment] + path, headers=headers, json=payload,
            params=params, timeout=(5, 15), allow_redirects=False,
        )
        data = response.json()
    except (RequestException, ValueError):
        raise SquareAPIError() from None
    if not isinstance(data, dict):
        raise SquareAPIError(response.status_code)
    if not 200 <= response.status_code < 300 or data.get('errors'):
        errors = data.get('errors')
        codes = tuple(
            error['code'] for error in errors
            if isinstance(error, dict) and isinstance(error.get('code'), str)
        ) if isinstance(errors, list) else ()
        raise SquareAPIError(response.status_code, codes)
    return data

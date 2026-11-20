# Security Policy

## Supported Versions

Security fixes are accepted for these Odoo addon series.

| Odoo series | Release branch | Addons |
| --- | --- | --- |
| 17.0 | `17.0` | Square POS Integration |
| 18.0 | `18.0` | Square POS Integration, Square Online Payments |
| 19.0 | `19.0` | Square POS Integration, Square Online Payments, Square Terminal for Point of Sale |

## Reporting a Vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Send a private report to the repository maintainers with:

- a clear description of the vulnerability
- affected version or commit
- reproduction steps
- impact assessment
- relevant logs, payloads, or screenshots with secrets removed

If you are not sure whether an issue is security-sensitive, report it privately first.

## Sensitive Data

Redact access tokens, signing and encryption keys, pairing codes, card details, and customer data from reports and pull requests. Keep production database dumps and raw webhook payloads private.

## Operational Recommendations

- Configure the Square webhook signature key before exposing `/square/webhook` publicly.
- Serve Odoo over HTTPS in production.
- Rotate Square credentials when staff access changes or a secret may have been exposed.
- Restrict Odoo access to Square configuration records to trusted administrators.
- Review integration logs after credential rotation, webhook changes, and failed webhook bursts.

# Security Policy

## Supported Versions

Security fixes are accepted for the active Odoo 17.0 addon series.

| Odoo series | Addon series | Supported |
| --- | --- | --- |
| 17.0 | 17.0.x.y.z | Yes |

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

This addon handles Square credentials, webhook signatures, customer contact data, sales orders, invoices, refunds, and inventory records. Reports and pull requests must not include live access tokens, webhook signature keys, customer personal data, production database dumps, or unredacted webhook payloads.

## Operational Recommendations

- Configure the Square webhook signature key before exposing `/square/webhook` publicly.
- Serve Odoo over HTTPS in production.
- Rotate Square credentials when staff access changes or a secret may have been exposed.
- Restrict Odoo access to Square configuration records to trusted administrators.
- Review integration logs after credential rotation, webhook changes, and failed webhook bursts.

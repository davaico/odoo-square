# Contributing

Thank you for helping improve the Odoo Square Integration.

This project is an Odoo addon, so changes should follow Odoo module conventions, preserve upgrade safety, and include tests for behavior that affects accounting, inventory, webhooks, or data synchronization.

## Development Principles

- Keep business logic inside Odoo models and services, not controllers.
- Keep controllers thin: validate input, delegate work, and return explicit HTTP responses.
- Prefer Odoo ORM APIs over raw SQL unless there is a clear performance or locking reason.
- Use `sudo()` only at integration boundaries where the module intentionally performs system-level work.
- Preserve auditability. Do not delete order lines, logs, refunds, or queue records when a state transition is a better representation.
- Treat accounting, stock moves, and external API calls as high-risk areas. Add regression tests for every fix.
- Keep commits focused. Avoid mixing formatting, refactoring, and behavior changes in the same pull request.

## Local Setup

```bash
git clone https://github.com/davaico/odoo-square.git
cd odoo-square
cp .env.example .env
```

Set `DB_PASSWORD` in `.env`, then start the local stack:

```bash
docker compose up -d
```

Install or upgrade the addon from Odoo Apps, or run:

```bash
docker compose exec odoo bash -lc 'odoo \
  --config=/etc/odoo/odoo.conf \
  -d odoo \
  --db_host="${HOST:-db}" \
  --db_port=5432 \
  --db_user="${USER:-odoo}" \
  --db_password="$PASSWORD" \
  -u odoo_square \
  --stop-after-init'
```

## Tests

Run the full addon test suite before opening a pull request:

```bash
DB_PASSWORD=ci_odoo_test docker compose -f docker-compose.test.yml run --rm odoo \
  odoo \
    --config=/etc/odoo/odoo.conf \
    -d odoo \
    --db_host=db \
    --db_port=5432 \
    --db_user=odoo \
    --db_password=ci_odoo_test \
    -i odoo_square \
    --test-enable \
    --workers=0 \
    --stop-after-init \
    --test-tags=odoo_square \
    --log-level=test
```

Tests should be added or updated when a change affects:

- webhook validation or event routing
- Square order, payment, refund, or exchange processing
- stock reservations, deliveries, returns, or quantity sync
- accounting documents, journals, invoices, or credit notes
- manual resync behavior
- security-sensitive configuration or access rights

## Pull Request Checklist

- The change is scoped to one problem.
- The addon installs or upgrades cleanly on Odoo 17.0.
- Tests pass locally or the pull request explains why they could not be run.
- New or changed behavior is covered by tests.
- README, configuration notes, or migration notes are updated when relevant.
- No secrets, tokens, customer data, or generated database dumps are committed.
- The AGPL-3.0-or-later license headers remain intact on addon Python files.

## Issue Reports

For bug reports, include:

- Odoo edition and exact 17.0 build if known
- addon version from the manifest or Square configuration screen
- Square event type involved
- expected behavior
- actual behavior
- relevant Odoo logs or integration log entries with secrets removed
- reproduction steps on a clean database when possible

For feature requests, describe the operational workflow and the Odoo/Square records involved.

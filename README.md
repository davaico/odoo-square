# Square integrations for Odoo 19

[![CI](https://github.com/davaico/odoo-square/actions/workflows/ci.yml/badge.svg)](https://github.com/davaico/odoo-square/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![Odoo](https://img.shields.io/badge/Odoo-19.0-875A7B.svg)](https://www.odoo.com)

Install any of these addons independently or use all three in one Odoo 19 Community database.

| Where the sale starts | Addon | What it does |
| --- | --- | --- |
| Square POS | [Square POS Integration](#square-configuration) | Imports sales, EUR refunds, and supported exchanges. Publishes Odoo warehouse counts to mapped Square locations. |
| Odoo Point of Sale | [Square Terminal for Point of Sale](pos_square_terminal/README.md) | Sends a card share to a paired terminal. Odoo POS keeps the sale and itemized receipt. |
| Odoo website | [Square Online Payments](payment_square_online/README.md) | Takes website payments through Square and lets Accounting refund the original payment. |

Each addon has its own Odoo Apps presentation. Choose by where the sale starts: **Square register** for sales synchronization, **Odoo register** for Terminal payments, or **Odoo website** for online checkout.

## Importer requirements

- Odoo 19 Community and PostgreSQL.
- Python package `requests` in the Odoo environment. The Compose image includes it.
- A Square Developer account with an application, access token, location IDs, and webhook signature key.
- Docker and Docker Compose for the included local setup.

## Quick Start

Clone the Odoo 19 release branch and create a local environment file.

```bash
git clone --branch 19.0 https://github.com/davaico/odoo-square.git
cd odoo-square
cp .env.example .env
```

Set `DB_PASSWORD` in `.env`. Install the addon into the fresh database, then start Odoo.

```bash
docker compose build --pull
docker compose run --rm odoo odoo --database=odoo_square_v19 --init=odoo_square --stop-after-init
docker compose up -d
```

Open [Odoo on localhost port 8069](http://localhost:8069) and sign in to the fresh database with Odoo's initial `admin` login and password. Change the initial password after signing in. If you changed `DB_NAME` or `ODOO_PORT`, use the corresponding database name and port. The `odoo-square-v19` Compose project has its own database and filestore volumes.

For a standalone Odoo 19 Community setup, put the `odoo_square` folder on the Odoo addons path, restart Odoo, update the Apps list in the fresh database, and install **Square POS Integration**.

## Square Configuration

Set up the company's chart of accounts and configure EUR for Square transactions. In Odoo, open **Square > Configuration** and create the Square configuration.

- Enter the **Square Application ID** and **Square Access Token**, and select **Sandbox** or **Production** to match the credentials.
- Select the company's **Payment Journal** for incoming and outgoing Square payment records. A bank journal can record both sales and refunds.
- Add a **Square Location Mapping** with the Square location ID, location name, and corresponding Odoo warehouse for each location.
- Optionally select a sales team for each mapping and a **Default Sales Team**.
- Save the configuration and use **Test Connection** to verify the Square credentials.

For products that should affect stock, enable **Track Inventory** in Odoo and match the Odoo **Internal Reference** to the Square SKU.

In the Square Developer Dashboard, create a webhook subscription that points to:

```text
https://your-odoo-domain.example/square/webhook
```

Copy the subscription's signature key into **Webhook Signature Key** in the Odoo configuration. Subscribe to these event types.

- `order.created` and `order.updated` for order import and supported status and line changes.
- `payment.updated` for completed payments tied to supported exchanges.
- `refund.created` and `refund.updated` for refund creation and status changes.

Use a publicly reachable HTTPS URL for the webhook. When a reverse proxy terminates TLS, keep `proxy_mode = True` in `config/odoo.conf` and forward the public hostname so Odoo can verify the signature against the subscription URL.

## Development

To refresh the base images and apply current Ubuntu package updates, rebuild the images and recreate the services with the existing volumes.

```bash
docker compose build --pull --no-cache
docker compose up -d
```

When updating to PostgreSQL 17.12, review its migration notes for affected indexes and incremental backups. [PostgreSQL 17.12 release notes](https://www.postgresql.org/docs/17/release-17-12.html)

Follow Odoo logs.

```bash
docker compose logs -f odoo
```

Restart Odoo after Python changes.

```bash
docker compose restart odoo
```

Update the addon after manifest, XML, or model changes. Use the database name from `.env` if it differs from the default.

```bash
docker compose stop odoo
docker compose run --rm odoo odoo --database=odoo_square_v19 --update=odoo_square --stop-after-init
docker compose up -d odoo
```

## Testing

From the repository root, run the importer suite in a fresh Compose project.

```bash
(
  set -eu
  compose=(docker compose -p "odoo-square-v19-test-$BASHPID" -f docker-compose.test.yml)
  trap '"${compose[@]}" down -v' EXIT
  "${compose[@]}" build
  "${compose[@]}" run --rm -T odoo
)
```

Use Bash for the command above. For a focused importer run, add the desired `--test-tags` to the [Compose service command](docker-compose.test.yml). For browser, coexistence, or concurrency tests, follow the matching [CI matrix entry and commands](.github/workflows/ci.yml) in a fresh project. The browser image includes Chrome and the Python WebSocket dependency. The concurrency suites commit test records and need disposable databases.

Check that the intended tests ran, with a nonzero test count and zero failures, errors, or skipped addon tests. The suites simulate Square responses.

## Operational Notes

When adding Square Terminal to a database with the importer, upgrade `odoo_square` to at least `19.0.1.4.3` before resuming stock sync. The importer needs the new inventory queue metadata to account for stock deductions already made by Square.

When upgrading an existing Square Terminal installation from a prerelease build, update `pos_square_terminal` to apply the payment and notification schema changes. Replay protection covers the notification IDs present at upgrade and new notifications received afterward.

When upgrading from an importer earlier than `19.0.1.4.2`, update the installed module to apply the refund schema change. Unverified legacy refunds keep their Square ID and document links for review. If the `19.0.1.4.1` migration removed a historical refund, consult a backup taken before the migration for its original links.

- Configure the webhook signature key in Odoo before exposing the endpoint publicly.
- Keep Square access tokens out of Git. Use Odoo configuration records, environment-specific secrets management, or encrypted backups.
- Map every active Square location to the intended Odoo warehouse before enabling production inventory sync.
- **Square POS: Send inventory counts** publishes the latest queued warehouse quantity for each mapped product and location. Odoo takes a fresh measurement when a queued count is more than 24 hours old. If duplicate Square catalog SKUs paused a count, make the SKU unique and choose **Synchronize All Products**.
- Review **Square Integration Logs** after installation and after every Square credential or webhook change.
- To recover missing Square orders, open **Manual Resync Orders** from **Square > Configuration**, scan the relevant dates, and select the orders to import. If references are waiting for ownership verification, recover the listed sales and scan again after Square access is restored.

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request.

## Security

Please do not open public issues for vulnerabilities. See [SECURITY.md](SECURITY.md) for the supported version and private reporting process.

## Maintained by Davai

This module is built and maintained by [Davai](https://davai.co), a Paris software studio. We build custom ERPs and adapt Odoo to our clients' operations: [ERP sur mesure et intégration Odoo](https://davai.co/fr/services/erp-sur-mesure). More of our open source work: [davai.co/fr/open-source](https://davai.co/fr/open-source).

## License

This project is licensed under the GNU Affero General Public License v3.0 or later. See [LICENSE](LICENSE).

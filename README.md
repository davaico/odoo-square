# Square POS Integration

[![CI](https://github.com/davaico/odoo-square/actions/workflows/ci.yml/badge.svg)](https://github.com/davaico/odoo-square/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![Odoo](https://img.shields.io/badge/Odoo-18.0-875A7B.svg)](https://www.odoo.com)

Odoo 18 Community addon for integrating Square POS with Odoo Sales, Accounting, and Inventory.

The module processes Square webhooks, creates and updates Odoo sales orders, records payments and refunds initiated in Square, maps Square locations to Odoo warehouses, and synchronizes inventory with an auditable activity log.

## Features

- Secure Square webhook endpoint at `/square/webhook` with HMAC-SHA256 validation when a webhook signature key is configured.
- Order ingestion for `order.created` and `order.updated` events, including customer matching and sale order creation.
- Refund and exchange handling with invoices, credit notes, stock returns, and preserved order history.
- Square location to Odoo warehouse mapping for multi-location inventory workflows.
- Inventory synchronization hooks for Odoo stock movements and manual quantity changes.
- Manual resync wizard for finding and replaying missing Square orders.
- Integration log models and views for monitoring webhook, order, refund, exchange, and stock sync activity.
- Docker Compose development and CI test setup.

## Requirements

- Odoo 18.0 Community and PostgreSQL. The included Compose setup uses Odoo 18.0-20261015 and PostgreSQL 17.4.
- Python package: `requests`, included in the official Odoo container
- Square Developer account with an application, access token, location IDs, and webhook signature key
- Docker and Docker Compose for the included local development environment

## Compatibility

This repository follows Odoo addon versioning. The current manifest version is `18.0.1.4.0`, where `18.0` is the Odoo series and `1.4.0` is the addon release version.

The addon manifest declares the following Odoo dependencies:

- `base`
- `web`
- `sale`
- `sales_team`
- `account`
- `stock`
- `sale_stock`
- `payment`

## Quick Start

Clone the Odoo 18 release branch:

```bash
git clone --branch 18.0 https://github.com/davaico/odoo-square.git
cd odoo-square
```

Create a local environment file:

```bash
cp .env.example .env
```

Edit `.env` and set a non-default `DB_PASSWORD`.

Install **Square POS Integration** into the fresh local database, then start Odoo:

```bash
docker compose build odoo
docker compose run --rm odoo odoo -d odoo -i odoo_square --stop-after-init
docker compose up -d
```

Use the database name from `.env` in place of `odoo` if you changed `DB_NAME`. Open [http://localhost:8069](http://localhost:8069) and sign in with the fresh local administrator credentials (`admin` / `admin`).

## Square Configuration

In the Square Developer Dashboard, create a webhook subscription that points to:

```text
https://your-odoo-domain.example/square/webhook
```

Configure at least these event types:

- `order.created`
- `order.updated`
- `payment.updated`
- `refund.created`
- `refund.updated`

Set up the company's chart of accounts and create or select a payment journal for Square payments. In Odoo, open **Square > Configuration** and create a configuration with:

- The matching Square environment (`Sandbox` or `Production`), application ID, and access token
- The webhook signature key from the Square Developer Dashboard
- The payment journal used to record Square transactions
- A mapping from each Square location ID to its Odoo warehouse
- An optional default Sales Team and optional per-location Sales Teams

Save the configuration and use **Test Connection** to verify the Square credentials.

For production deployments, run Odoo behind HTTPS and keep `proxy_mode = True` in `config/odoo.conf` when a reverse proxy terminates TLS.

## Development

Start the local stack:

```bash
docker compose up -d
```

Follow Odoo logs:

```bash
docker compose logs -f odoo
```

Restart Odoo after Python changes:

```bash
docker compose restart odoo
```

Update the addon after manifest, XML, or model changes:

```bash
docker compose stop odoo
docker compose run --rm odoo odoo -d odoo -u odoo_square --stop-after-init
docker compose up -d
```

## Testing

Run the CI test service with its own Compose project, database, and volumes:

```bash
test_project=odoo-square-v18-test-$$
export DB_PASSWORD=ci_odoo_test DB_NAME=odoo_square_v18_test
docker compose -p "$test_project" -f docker-compose.test.yml build odoo
docker compose -p "$test_project" -f docker-compose.test.yml run --rm odoo
docker compose -p "$test_project" -f docker-compose.test.yml down -v
```

Set `ODOO_TEST_TAGS` for a focused run, for example `/odoo_square:TestSquareModuleContracts`.

The suite includes HTTP webhook tests, Square order/refund integration scenarios, stock return behavior, manual resync behavior, and addon contract tests.

## Project Structure

```text
odoo-square/
├── odoo_square/
│   ├── controllers/            # Public webhook endpoint
│   ├── data/                   # Default users and Square products
│   ├── models/                 # Odoo business models and services
│   ├── security/               # ir.model.access.csv
│   ├── static/description/     # Odoo app listing assets
│   ├── tests/                  # Odoo TransactionCase and HttpCase tests
│   └── views/                  # Backend UI views and menus
├── config/                     # Odoo container configuration
├── .github/workflows/          # CI
├── docker-compose.yml          # Local development stack
├── docker-compose.test.yml     # Test stack used by CI
└── README.md
```

## Architecture

```mermaid
flowchart TB
    Square["Square POS and API"]
    Webhook["/square/webhook"]
    Queue["square.webhook.queue"]
    Processor["square.order.processor"]
    Sale["sale.order"]
    Accounting["Invoices and credit notes"]
    Stock["Stock pickings, moves, and quants"]
    Sync["square.stock.sync"]
    Log["square.integration.log"]

    Square -->|"webhook events"| Webhook
    Webhook -->|"out-of-order updates"| Queue
    Queue --> Processor
    Webhook --> Processor
    Processor --> Sale
    Processor --> Accounting
    Processor --> Stock
    Stock --> Sync
    Sync -->|"inventory adjustments"| Square
    Webhook --> Log
    Processor --> Log
    Sync --> Log
```

## Operational Notes

- Configure the webhook signature key in Odoo before exposing the endpoint publicly.
- Keep Square access tokens out of Git. Use Odoo configuration records, environment-specific secrets management, or encrypted backups.
- Map every active Square location to the intended Odoo warehouse before enabling production inventory sync.
- Review **Square Integration Logs** after installation and after every Square credential or webhook change.
- Use the manual resync wizard for missed or delayed Square events instead of replaying raw webhook requests manually.

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request.

## Security

Please do not open public issues for vulnerabilities. See [SECURITY.md](SECURITY.md) for the supported version and private reporting process.

## License

This project is licensed under the GNU Affero General Public License v3.0 or later. See [LICENSE](LICENSE).

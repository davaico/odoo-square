# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**Odoo Square Integration** is an Odoo 17 module that provides comprehensive integration between Odoo and Square POS. It handles webhook processing for orders, refunds, and exchanges, plus bidirectional inventory synchronization.

## Development Commands

### Build and Run Locally

```bash
# Copy environment file and edit with your database password
cp .env.example .env
# Edit .env to set DB_PASSWORD

# Start the Docker stack
docker compose up -d

# View logs
docker compose logs -f odoo

# Stop services
docker compose down
```

### Running Tests

```bash
# Run the full test suite locally
docker compose exec odoo odoo \
  -d odoo \
  --db_host=db \
  --test-enable \
  --stop-after-init \
  --test-tags="odoo_square" \
  --log-level=test

# Run a specific test file
docker compose exec odoo odoo \
  -d odoo \
  --db_host=db \
  --test-enable \
  --stop-after-init \
  --test-tags="odoo_square.test_square_webhook" \
  --log-level=test

# Run tests in the CI environment (docker-compose.test.yml)
docker compose -f docker-compose.test.yml up -d db
docker compose -f docker-compose.test.yml build odoo
docker compose -f docker-compose.test.yml run --rm odoo \
  odoo -d odoo --db_host=db --db_port=5432 \
  --db_user=odoo --db_password=ci_odoo_test \
  -i odoo_square --test-enable --workers=0 \
  --stop-after-init --test-tags="odoo_square" --log-level=test
```

### Installing the Module in Odoo

1. Navigate to Apps menu
2. Click "Update Apps List"
3. Search for "Odoo Square Integration"
4. Click "Install"
5. Configure at Settings → Square Configuration

## Code Architecture

### Core Components

**1. Webhook Processing (`controllers/square_webhook.py`)**
- Endpoint: `/square/webhook`
- Validates HMAC-SHA256 signatures
- Routes events to appropriate processors
- Events: `order.created`, `order.updated`, `refund.created`, `payment.updated`

**2. Order Processing (`models/square_order_processor.py`)**
- Main orchestrator for Square order lifecycle
- `process_square_order()` - Creates draft orders from Square orders
- `process_square_order_update()` - Handles order state changes (completion, cancellation)
- `process_product_exchange()` - Manages exchanges with return pickings and credit notes
- Race condition prevention with database locks (`SELECT FOR UPDATE NOWAIT`)
- Idempotency checks via `square_order_id` unique constraint

**3. Sale Order Extension (`models/sale_order.py`)**
- `create_from_square()` - Creates sale order from Square order data
- Fields: `square_order_id`, `square_order_data` (JSON), `square_location_id`
- Smart customer matching: email → phone → name → auto-create
- Line items stored with `square_line_id` and `square_catalog_id` for tracking

**4. Stock Synchronization (`models/square_stock_sync.py`)**
- Bidirectional sync: Odoo ↔ Square inventory
- Triggers on stock moves and manual adjustments
- Loop prevention: excludes Square-originated moves
- Uses Square Catalog API for product matching
- Configured warehouse focus: only syncs from specific warehouse

**5. Refund Processing (`models/square_refund.py`)**
- Handles full and partial refunds
- Credit note creation with `reversed_entry_id` link to original invoice
- Payment registration for refunds

**6. Integration Logging (`models/square_integration_log.py`)**
- Centralized audit trail for all Square operations
- Methods: `log_square_event()`, `log_error()`
- Chatter messages on related sales orders

### Data Flow

```
Square Webhook
    ↓
Signature Validation
    ↓
Order Creation (if new) / Update (if existing)
    ↓
Confirmation (if COMPLETED)
    ↓
Invoice Creation + Stock Moves
    ↓
Payment Registration
    ↓
Stock Sync to Square
```

### Exchange/Return Flow

```
order.updated (with returns/new items)
    ↓
Detect exchange (return + new products)
    ↓
Create return pickings (from original delivery)
    ↓
Create credit note (for returned items)
    ↓
Create new SO (for new items)
    ↓
Confirm + Invoice new SO
    ↓
Reconcile credit note ↔ new invoice
    ↓
Register delta payment if needed
```

## Key Implementation Details

### Race Condition Prevention

- **Database Locking**: `_check_existing_order()` uses `SELECT FOR UPDATE NOWAIT` to prevent concurrent duplicate creation
- **Idempotency**: All operations check for existing records before creating
- **Duplicate Constraint**: `square_order_id` is unique in database schema

### Idempotency

- Orders: checked via `square_order_id`
- Exchanges: checked via return pickings + SO existence
- Payments: checked before creating duplicates
- All operations are safe to retry without side effects

### Customer Matching

Priority order in `sale_order.py`:
1. Email match
2. Phone match (parsed from Square order)
3. Name match
4. Create new customer if no match

### Warehouse Configuration

- Primary method: `square_config.get_configured_warehouse()`
- Fallback: first warehouse in system
- Location-specific mapping: `square.location.mapping` for multi-location setups

### Tax Handling

- VAT tax lookup: 20% tax with type "sale" and amount_type "percent"
- TTC → HT conversion: `Decimal(str(total_ttc)) / Decimal("1.2") / Decimal(str(quantity))`
- Applied to all order lines created from Square

### Currency

- All operations assume EUR currency (Square default for EU)
- Orders from Square are treated as EUR
- Warnings logged if invoices created in different currency

## File Structure

**Note:** On branch `17.0`, the module is at the repository root for Odoo Apps Store compatibility. On `master`, it's in `addons/odoo_square/` for local development.

```
odoo_square/                        # Module at root (17.0 branch)
├── controllers/
│   ├── __init__.py
│   └── square_webhook.py          # Webhook endpoint and routing
├── models/
│   ├── __init__.py
│   ├── sale_order.py              # Sale order creation and customer matching
│   ├── sale_order_line.py         # Order line extensions
│   ├── square_api_client.py       # Square API wrapper
│   ├── square_config.py           # Configuration and credentials
│   ├── square_order_processor.py  # Main order orchestration
│   ├── square_integration_log.py  # Audit trail and logging
│   ├── square_refund.py           # Refund processing
│   ├── square_stock_sync.py       # Inventory synchronization
│   ├── square_webhook_queue.py    # Webhook queue (retry mechanism)
│   ├── square_location_mapping.py # Multi-location support
│   ├── stock_move.py              # Stock move hooks
│   ├── stock_picking.py           # Picking extensions
│   └── stock_quant.py             # Stock adjustment hooks
├── views/
│   ├── views.xml                  # Main UI views
│   ├── square_config_views.xml    # Configuration interface
│   ├── square_integration_log_views.xml # Log views
│   └── square_webhook_queue_views.xml   # Queue management
├── data/
│   ├── square_bot_user.xml        # System bot user for automation
│   └── square_products.xml        # Default products
├── security/
│   └── ir.model.access.csv        # Access control rules
├── tests/
│   ├── common.py                  # Test fixtures and helpers
│   ├── test_square_webhook.py     # Webhook processing tests
│   ├── test_square_integration_scenario.py # End-to-end scenarios
│   └── test_stock_return.py       # Return/exchange tests
├── static/description/icon.png
├── __init__.py
└── __manifest__.py                 # Module metadata
```

## Testing Best Practices

- **Test Framework**: Odoo's built-in `TransactionCase`
- **Test Data**: See `common.py` for test fixtures
- **Idempotency**: All tests verify that operations can be safely retried
- **Database Isolation**: Each test runs in a transaction and rolls back
- **API Mocking**: Use `unittest.mock` to mock Square API calls in tests

## Common Debugging Patterns

### Webhook Not Processing

1. Check signature validation in logs
2. Verify webhook signature key matches Square settings
3. Check integration log for failed events
4. Ensure Square bot user exists: `self.env.ref("odoo_square.user_square_bot")`

### Stock Not Syncing to Square

1. Verify warehouse configured in Square settings
2. Check `stock.move` has proper `square_origin` marker (to prevent loops)
3. Review Square API credentials and permissions
4. Check integration log for sync errors

### Invoice/Payment Issues

1. Verify 20% VAT tax exists in chart of accounts
2. Check payment journal configuration
3. Ensure bot user has access to accounting modules
4. Review `account.payment` records created during processing

### Race Conditions

- Check `--workers=0` in test commands (required for transactional tests)
- Verify `SELECT FOR UPDATE NOWAIT` is being used in duplicate detection
- Check database logs for lock timeout errors

## Important Configuration

**Environment Variables** (in `.env` or `docker-compose.yml`):
- `DB_NAME` - PostgreSQL database name (default: `odoo`)
- `DB_HOST` - PostgreSQL host (default: `db`)
- `DB_USER` - PostgreSQL user (default: `odoo`)
- `DB_PASSWORD` - PostgreSQL password (required, no default)

**Odoo Configuration** (Settings → Square Configuration):
- Application ID - Square app credentials
- Access Token - Square API token
- Location ID - Default Square location
- Webhook Signature Key - For HMAC validation
- Warehouse - For inventory sync
- Payment Journal - For Square payments

## Continuous Integration

GitHub Actions workflow in `.github/workflows/ci.yml`:
- Runs on push to `main`/`master` and all pull requests
- Spins up PostgreSQL in Docker
- Builds Odoo image
- Runs full test suite with `--test-tags="odoo_square"`
- Cleans up containers after test completion

To run locally: `docker compose -f docker-compose.test.yml up -d db && docker compose -f docker-compose.test.yml build odoo && docker compose -f docker-compose.test.yml run --rm odoo [test command]`

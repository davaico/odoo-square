# -*- coding: utf-8 -*-
from datetime import datetime, timezone
from decimal import Decimal
import logging

from dateutil import parser as dt_parser
from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class SaleOrder(models.Model):
    _inherit = "sale.order"

    square_order_id = fields.Char(
        string="Square Order ID",
        help="The unique identifier from Square for this order",
        index=True,
        copy=False,
    )

    square_payment_id = fields.Char(
        string="Square Payment ID",
        help="The unique identifier from Square for the payment associated with this order",
        index=True,
        copy=False,
    )

    _sql_constraints = [
        (
            "square_order_id_unique",
            "UNIQUE(square_order_id)",
            "Square Order ID must be unique. This order may already exist in the system.",
        )
    ]

    square_order_data = fields.Text(
        string="Square Order Data",
        help="Raw JSON data from Square webhook for debugging",
        copy=False,
    )

    is_square_order = fields.Boolean(
        string="Is Square Order",
        compute="_compute_is_square_order",
        store=True,
        help="Indicates if this order originated from Square",
    )

    square_refund_ids = fields.One2many(
        "square.refund",
        "sale_order_id",
        string="Square Refunds",
        help="Refunds associated with this Square order",
    )

    # Refund tracking fields
    total_refunded_amount = fields.Monetary(
        string="Total Refunded Amount",
        compute="_compute_total_refunded_amount",
        currency_field="currency_id",
        help="Total amount refunded for this order",
    )

    refund_status = fields.Selection(
        [
            ("not_refunded", "Not Refunded"),
            ("partially_refunded", "Partially Refunded"),
            ("fully_refunded", "Fully Refunded"),
        ],
        string="Refund Status",
        compute="_compute_refund_status",
        store=True,
        help="Current refund status of the order",
    )

    @api.depends("square_order_id")
    def _compute_is_square_order(self):
        for order in self:
            order.is_square_order = bool(order.square_order_id)

    @api.depends("square_refund_ids.refund_amount", "square_refund_ids.status")
    def _compute_total_refunded_amount(self):
        """Compute total refunded amount"""
        for order in self:
            # Get all completed refunds
            completed_refunds = order.square_refund_ids.filtered(
                lambda r: r.status == "completed"
            )

            # Calculate total refunded amount
            total_refunded = sum(completed_refunds.mapped("refund_amount"))
            order.total_refunded_amount = total_refunded

    @api.depends(
        "square_refund_ids.refund_amount", "square_refund_ids.status", "amount_total"
    )
    def _compute_refund_status(self):
        """Compute refund status"""
        for order in self:
            # Get all completed refunds
            completed_refunds = order.square_refund_ids.filtered(
                lambda r: r.status == "completed"
            )

            # Calculate total refunded amount for status determination
            total_refunded = sum(completed_refunds.mapped("refund_amount"))

            # Determine refund status
            if total_refunded == 0:
                order.refund_status = "not_refunded"
            elif total_refunded >= order.amount_total:
                order.refund_status = "fully_refunded"
            else:
                order.refund_status = "partially_refunded"

    def _get_square_bot_user(self):
        """Get the Square integration bot user"""
        try:
            return self.env.ref("odoo_square.user_square_bot")
        except ValueError:
            # Fallback to admin user if bot user doesn't exist
            _logger.warning("Square bot user not found, falling back to admin user")
            return self.env.ref("base.user_admin")

    def _get_configured_warehouse(self):
        """Get the configured warehouse from Square configuration (legacy method)"""
        square_config = self.env["square.config"].search([], limit=1)
        if square_config:
            warehouse = square_config.get_configured_warehouse()
            if warehouse:
                _logger.info(f"Using configured Square warehouse: {warehouse.name}")
                return warehouse
            else:
                _logger.warning("No warehouse configured in Square settings")
                return None
        else:
            _logger.warning("No Square configuration found")
            return None

    def _get_warehouse_for_square_location(self, square_location_id):
        """Get the warehouse mapped to a specific Square location"""
        if not square_location_id:
            _logger.warning("No Square location ID provided, using default warehouse")
            return self._get_configured_warehouse()

        square_config = self.env["square.config"].search([], limit=1)
        if square_config:
            warehouse = square_config.get_warehouse_for_location(square_location_id)
            if warehouse:
                _logger.info(
                    f"Using warehouse '{warehouse.name}' for Square location '{square_location_id}'"
                )
                return warehouse
            else:
                _logger.warning(
                    f"No warehouse mapping found for Square location '{square_location_id}'"
                )
        else:
            _logger.warning("No Square configuration found")

        # Fallback to default warehouse
        return self._get_configured_warehouse()

    def _sales_team_matches_company(self, team, company):
        if not team or not team.exists():
            return False
        if not company:
            return True
        if not team.company_id:
            return True
        return team.company_id.id == company.id

    @api.model
    def _normalize_square_metadata(self, raw):
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list):
            out = {}
            for item in raw:
                if isinstance(item, dict) and item.get("key") is not None:
                    out[item["key"]] = item.get("value")
            return out
        return {}

    @api.model
    def _resolve_sales_team_from_square_metadata(self, square_data, company):
        """Use metadata key odoo_sales_team_id (numeric crm.team id) if present."""
        Team = self.env["crm.team"]
        if not isinstance(square_data, dict):
            return Team.browse()
        meta = self._normalize_square_metadata(square_data.get("metadata"))
        raw = meta.get("odoo_sales_team_id")
        if raw is None or raw == "":
            return Team.browse()
        try:
            tid = int(str(raw).strip())
        except (ValueError, TypeError):
            _logger.warning("Invalid odoo_sales_team_id in Square metadata: %s", raw)
            return Team.browse()
        team = Team.browse(tid)
        if team.exists() and self._sales_team_matches_company(team, company):
            _logger.info("Using Sales Team %s from Square metadata", team.name)
            return team
        return Team.browse()

    @api.model
    def _get_sales_team_for_square_location(self, square_location_id, company):
        config = self.env["square.config"].search([], limit=1)
        if config:
            return config.get_sales_team_for_location(
                square_location_id, company=company
            )
        return self.env["crm.team"].browse()

    def _apply_square_sales_team_override_from_full_order(self, sale_order, full_order_data):
        team = sale_order._resolve_sales_team_from_square_metadata(
            full_order_data, sale_order.company_id
        )
        if team:
            sale_order.write({"team_id": team.id})

    def _prepare_invoice(self):
        vals = super()._prepare_invoice()
        if self.team_id:
            vals["team_id"] = self.team_id.id
        return vals

    @api.model
    def _parse_square_order_created_at(self, square_data):
        """Square ``created_at`` (RFC3339) -> naive UTC datetime for ``date_order``."""
        if not square_data:
            return False
        raw = square_data.get("created_at")
        if not raw:
            return False
        try:
            dt = raw if isinstance(raw, datetime) else dt_parser.parse(raw)
            if getattr(dt, "tzinfo", None):
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            _logger.warning("Could not parse Square created_at: %r", raw)
            return False

    @api.model
    def create_from_square(self, square_data):
        """
        Create a sale order from Square webhook data.

        Note: This method assumes duplicate checking has already been done
        at the processor level. It focuses on order creation logic only.
        """
        _logger.info(
            f"Creating sale order from Square data: {square_data.get('order_id')}"
        )

        try:
            # Extract basic order information
            square_order_id = square_data.get("order_id")
            if not square_order_id:
                raise ValueError("Missing Order ID in Square data")

            # Note: Duplicate check is handled at the processor level for better idempotency
            # Get or create customer
            customer = self._get_or_create_customer_from_square(square_data)

            company = (
                self.env.company
                if self.env.company.id
                else self.env["res.company"].search([], limit=1)
            )
            if not company:
                raise ValueError(
                    "No company found in the system. Please ensure at least one company exists."
                )

            # Get warehouse based on Square location from order data
            square_location_id = square_data.get("location_id")
            warehouse = self._get_warehouse_for_square_location(square_location_id)
            if not warehouse:
                raise ValueError(
                    f"No warehouse configured for Square location '{square_location_id}'. "
                    "Please configure a mapping d'entrepôt dans les paramètres Square."
                )

            meta_team = self._resolve_sales_team_from_square_metadata(
                square_data, company
            )
            map_team = (
                meta_team
                if meta_team
                else self._get_sales_team_for_square_location(
                    square_location_id, company
                )
            )

            # Create sale order
            order_vals = {
                "partner_id": customer.id,
                "square_order_id": square_order_id,
                "square_order_data": str(square_data),
                "state": "draft",
                "origin": f"Square Order {square_order_id}",
                "company_id": company.id,
                "currency_id": company.currency_id.id,
                "warehouse_id": warehouse.id,
            }
            sq_created = self._parse_square_order_created_at(square_data)
            if sq_created:
                order_vals["date_order"] = sq_created
            if map_team:
                order_vals["team_id"] = map_team.id

            # Create the sale order - any duplicate constraint errors will be handled at processor level
            sale_order = self.create(order_vals)
            _logger.info(
                f"Created new sale order {sale_order.name} for Square order {square_order_id}"
            )

            # Lines: webhook payload first (tests / offline), else Square API
            if square_data.get("line_items"):
                payment_id = self._extract_payment_id_from_square_order(square_data)
                upd = {"square_order_data": str(square_data)}
                if payment_id:
                    upd["square_payment_id"] = payment_id
                sale_order.write(upd)
                self._create_order_lines_from_square(sale_order, square_data)
            else:
                self._fetch_and_create_order_lines(sale_order, square_order_id)

            # Add chatter message for order creation
            try:
                # Get Square bot user for message posting
                bot_user = self._get_square_bot_user()
                sale_order.with_user(bot_user).with_context(
                    mail_auto_subscribe_no_notify=True, mail_create_nosubscribe=True
                ).message_post(
                    body=f"Square order created - ID: {square_order_id}, Customer: {customer.name}, Amount: {sale_order.amount_total} {sale_order.currency_id.name}",
                    subject="Square Integration: Order Created",
                    message_type="comment",
                )
            except Exception as e:
                _logger.warning(
                    f"Could not post chatter message for order {sale_order.name}: {str(e)}"
                )

            # Log to integration dashboard
            self.env["square.integration.log"].log_order_creation(
                sale_order, square_data
            )

            _logger.info(
                f"Successfully created draft sale order {sale_order.name} from Square order {square_order_id}"
            )
            return sale_order

        except Exception as e:
            _logger.error(
                f"Error creating sale order from Square: {str(e)}", exc_info=True
            )

            # Log error to integration dashboard
            try:
                self.env["square.integration.log"].log_error(
                    title="Order Creation Error",
                    error_message=str(e),
                    square_order_id=(
                        square_data.get("order_id") if square_data else None
                    ),
                    technical_details=f"Square data: {square_data}\nError: {str(e)}",
                )
            except:
                pass  # Don't let logging errors break the main process

            raise

    def _get_or_create_customer_from_square(self, square_data):
        """
        Get or create customer from Square order data
        Matching strategy: email -> phone -> name -> create new
        """
        # Get full order data from Square API to access customer information
        square_order_id = square_data.get("order_id")
        if square_order_id:
            _logger.info(
                f"Fetching full order details from Square API for customer info: {square_order_id}"
            )
            try:
                square_api = self.env["square.api.client"]
                full_order_data = square_api.get_order(square_order_id)
                if full_order_data:
                    square_data = (
                        full_order_data  # Use full data instead of webhook data
                    )
                else:
                    _logger.warning(
                        f"Could not fetch full order data, using webhook data"
                    )
            except Exception as e:
                _logger.error(
                    f"Error fetching full order data for customer info: {str(e)}"
                )
                # Continue with webhook data if API call fails

        # Extract customer info from Square data (now full data if available)
        customer_info = self._extract_customer_info_from_square(square_data)

        if not customer_info:
            # Create anonymous customer
            return self._create_anonymous_customer()

        Partner = self.env["res.partner"]

        # Try to match by email first
        if customer_info.get("email"):
            partner = Partner.search([("email", "=", customer_info["email"])], limit=1)
            if partner:
                _logger.info(f"Found customer by email: {partner.name}")
                return partner

        # Try to match by phone
        if customer_info.get("phone"):
            partner = Partner.search([("phone", "=", customer_info["phone"])], limit=1)
            if partner:
                _logger.info(f"Found customer by phone: {partner.name}")
                return partner

        # Try to match by name and address
        if customer_info.get("name"):
            domain = [("name", "ilike", customer_info["name"])]
            if customer_info.get("street"):
                domain.append(("street", "ilike", customer_info["street"]))
            if customer_info.get("city"):
                domain.append(("city", "ilike", customer_info["city"]))

            partner = Partner.search(domain, limit=1)
            if partner:
                _logger.info(f"Found customer by name/address: {partner.name}")
                return partner

        # Create new customer
        return self._create_customer_from_square_info(customer_info)

    def _extract_customer_info_from_square(self, square_data):
        """Extract customer information from Square order data"""
        customer_info = {}

        # Method 1: Check for direct customer_id and fetch details from Square Customers API
        customer_id = square_data.get("customer_id")
        if customer_id:
            _logger.info(f"Found customer_id in order: {customer_id}")
            try:
                square_api = self.env["square.api.client"]
                customer_data = square_api.get_customer(customer_id)
                if customer_data:
                    # Extract customer details from Square Customers API
                    if customer_data.get("given_name") or customer_data.get(
                        "family_name"
                    ):
                        name_parts = [
                            customer_data.get("given_name", ""),
                            customer_data.get("family_name", ""),
                        ]
                        customer_info["name"] = " ".join(
                            part for part in name_parts if part
                        ).strip()

                    if customer_data.get("email_address"):
                        customer_info["email"] = (
                            customer_data["email_address"].strip().lower()
                        )

                    if customer_data.get("phone_number"):
                        # Normalize phone number format
                        phone = customer_data["phone_number"].strip()
                        customer_info["phone"] = self._normalize_phone_number(phone)

                    # Extract address information if available
                    if customer_data.get("address"):
                        address = customer_data["address"]
                        if address.get("address_line_1"):
                            customer_info["street"] = address["address_line_1"]
                        if address.get("locality"):
                            customer_info["city"] = address["locality"]
                        if address.get("postal_code"):
                            customer_info["zip"] = address["postal_code"]

                    customer_info["square_customer_id"] = customer_id
                    _logger.info(
                        f"Retrieved customer details from Square API: {customer_info}"
                    )

                    # If we got customer info from API, return it directly
                    if (
                        customer_info.get("name")
                        or customer_info.get("email")
                        or customer_info.get("phone")
                    ):
                        return customer_info
            except Exception as e:
                _logger.error(
                    f"Error fetching customer details from Square API: {str(e)}"
                )
                # Continue with fallback methods

        # Fallback methods if no customer_id or API call failed
        # Method 2: Get fulfillments (contains recipient info)
        fulfillments = square_data.get("fulfillments", [])
        if fulfillments:
            for fulfillment in fulfillments:
                # Try pickup details first
                pickup_details = fulfillment.get("pickup_details", {})
                recipient = pickup_details.get("recipient", {})

                if not recipient:
                    # Try shipment details if pickup not available
                    shipment_details = fulfillment.get("shipment_details", {})
                    recipient = shipment_details.get("recipient", {})

                if recipient:
                    if recipient.get("display_name"):
                        customer_info["name"] = recipient["display_name"].strip()
                    if recipient.get("email_address"):
                        customer_info["email"] = (
                            recipient["email_address"].strip().lower()
                        )
                    if recipient.get("phone_number"):
                        phone = recipient["phone_number"].strip()
                        customer_info["phone"] = self._normalize_phone_number(phone)

                    # Extract address from fulfillment if available
                    if recipient.get("address"):
                        address = recipient["address"]
                        if address.get("address_line_1"):
                            customer_info["street"] = address["address_line_1"]
                        if address.get("locality"):
                            customer_info["city"] = address["locality"]
                        if address.get("postal_code"):
                            customer_info["zip"] = address["postal_code"]

                    break  # Use first recipient found

        # Method 3: Get cardholder name from tenders as last resort
        if not customer_info.get("name"):
            tenders = square_data.get("tenders", [])
            for tender in tenders:
                card_details = tender.get("card_details", {})
                if card_details:
                    card = card_details.get("card", {})
                    if card and card.get("cardholder_name"):
                        customer_info["name"] = card["cardholder_name"].strip()
                        break

        # Method 4: Try billing address from tenders
        if not customer_info.get("street"):
            tenders = square_data.get("tenders", [])
            for tender in tenders:
                if tender.get("billing_address"):
                    billing_addr = tender["billing_address"]
                    if billing_addr.get("address_line_1"):
                        customer_info["street"] = billing_addr["address_line_1"]
                    if billing_addr.get("locality"):
                        customer_info["city"] = billing_addr["locality"]
                    if billing_addr.get("postal_code"):
                        customer_info["zip"] = billing_addr["postal_code"]
                    break

        # Clean up empty values and normalize
        customer_info = {k: v for k, v in customer_info.items() if v}

        # Final normalization
        if customer_info.get("name"):
            customer_info["name"] = self._normalize_customer_name(customer_info["name"])

        _logger.info(f"Final extracted customer info: {customer_info}")
        return customer_info

    def _normalize_phone_number(self, phone):
        """Normalize phone number to standard format"""
        if not phone:
            return ""

        # Remove all non-digit characters except + at the beginning
        import re

        phone = re.sub(r"[^\d+]", "", phone)

        # Ensure it starts with + or country code
        if not phone.startswith("+"):
            # Assume French format if no country code
            if len(phone) == 10 and phone.startswith("0"):
                phone = "+33" + phone[1:]
            elif len(phone) == 9 and not phone.startswith("0"):
                phone = "+33" + phone

        return phone

    def _normalize_customer_name(self, name):
        """Normalize customer name by capitalizing properly"""
        if not name:
            return ""

        # Split by spaces and capitalize each word
        words = name.split()
        capitalized_words = []

        for word in words:
            # Handle hyphenated names
            if "-" in word:
                parts = word.split("-")
                capitalized_parts = [part.capitalize() for part in parts]
                capitalized_words.append("-".join(capitalized_parts))
            else:
                capitalized_words.append(word.capitalize())

        return " ".join(capitalized_words)

    def _create_customer_from_square_info(self, customer_info):
        """Create a new customer from Square info"""
        vals = {
            "name": customer_info.get("name", "Square Customer"),
            "email": customer_info.get("email", ""),
            "phone": customer_info.get("phone", ""),
            "is_company": False,
            "customer_rank": 1,
        }

        # Add address info if available
        if customer_info.get("street"):
            vals["street"] = customer_info["street"]
        if customer_info.get("city"):
            vals["city"] = customer_info["city"]
        if customer_info.get("zip"):
            vals["zip"] = customer_info["zip"]

        partner = self.env["res.partner"].create(vals)
        _logger.info(f"Created new customer: {partner.name}")
        return partner

    def _create_anonymous_customer(self):
        """Create anonymous customer for orders without customer info"""
        partner = self.env["res.partner"].create(
            {
                "name": "Square Customer (Anonymous)",
                "is_company": False,
                "customer_rank": 1,
            }
        )
        _logger.info(f"Created anonymous customer: {partner.name}")
        return partner

    def _fetch_and_create_order_lines(self, sale_order, square_order_id):
        """
        Fetch full order details from Square API and create order lines
        """
        try:
            # Use Square API client to fetch full order details
            square_api = self.env["square.api.client"]
            full_order_data = square_api.get_order(square_order_id)

            if not full_order_data:
                _logger.warning(
                    f"Could not fetch full order data from Square API for order {square_order_id}"
                )
                return

            # Extract payment information from full order data
            payment_id = self._extract_payment_id_from_square_order(full_order_data)

            # Update sale order with full data and payment_id
            update_vals = {"square_order_data": str(full_order_data)}
            if payment_id:
                update_vals["square_payment_id"] = payment_id
                _logger.info(
                    f"Extracted payment_id {payment_id} for order {square_order_id}"
                )
            sq_created = self._parse_square_order_created_at(full_order_data)
            if sq_created:
                update_vals["date_order"] = sq_created

            sale_order.write(update_vals)

            # Create order lines from full Square data
            self._create_order_lines_from_square(sale_order, full_order_data)

            self._apply_square_sales_team_override_from_full_order(
                sale_order, full_order_data
            )

        except Exception as e:
            _logger.error(
                f"Error fetching full order details from Square API: {str(e)}",
                exc_info=True,
            )
            # Don't fail the entire process if API call fails
            # The order will be created without lines, which can be handled later

    def _extract_payment_id_from_square_order(self, square_order_data):
        """
        Extract payment_id from Square order data
        In Square, payments can be associated with orders in different ways
        """
        try:
            # Check for tenders (payments) in the order
            tenders = square_order_data.get("tenders", [])
            if tenders and len(tenders) > 0:
                # Get the first tender's payment_id
                payment_id = tenders[0].get("id")
                if payment_id:
                    return payment_id

            # Alternative: check for payment_ids array
            payment_ids = square_order_data.get("payment_ids", [])
            if payment_ids and len(payment_ids) > 0:
                return payment_ids[0]

            # Alternative: check for a single payment_id field
            payment_id = square_order_data.get("payment_id")
            if payment_id:
                return payment_id

            _logger.debug(
                f"No payment_id found in Square order data for order {square_order_data.get('id')}"
            )
            return None

        except Exception as e:
            _logger.warning(f"Error extracting payment_id from Square order: {str(e)}")
            return None

    def _create_order_lines_from_square(self, sale_order, square_data):
        """Create order lines from Square line items"""
        line_items = square_data.get("line_items", [])

        if not line_items:
            _logger.warning(
                f"No line items found in Square order {square_data.get('id')}"
            )
            return

        for i, item in enumerate(line_items):
            try:
                self._create_single_order_line(sale_order, item)
            except Exception as e:
                _logger.error(
                    f"Error creating order line {i+1}: {str(e)}", exc_info=True
                )
                # Continue with other items as per requirements
                continue

    def _create_single_order_line(self, sale_order, line_item):
        """Create a single order line from Square line item"""
        item_name = line_item.get("name", "Unknown Item")
        catalog_object_id = line_item.get("catalog_object_id", "")
        quantity = int(line_item.get("quantity", "1"))

        # Calculate unit price using total_money (final price after all discounts)
        # Square amounts are in cents
        total_money = line_item.get("total_money", {})
        total_tax_money = line_item.get("total_tax_money", {})

        total_amount_cents = total_money.get("amount", 0)
        tax_amount_cents = total_tax_money.get("amount", 0)

        # Convert to currency units with proper rounding
        total_amount = round(float(total_amount_cents) / 100.0, 2)
        tax_amount = round(float(tax_amount_cents) / 100.0, 2)

        # Calculate unit price from total (already includes discounts)
        unit_price_final = total_amount / quantity if quantity > 0 else 0.0

        _logger.info(
            f"Square pricing for {item_name}: Total ${total_amount:.2f}, Tax ${tax_amount:.2f}, "
            f"Qty {quantity}, Unit price ${unit_price_final:.2f}"
        )

        # Try to find existing product by SKU - use catalog_object_id to get actual SKU
        product = None
        sku = None

        if catalog_object_id:
            product = self.env["product.product"].search(
                [("default_code", "=", catalog_object_id)], limit=1
            )
            if product:
                sku = catalog_object_id
                _logger.info(
                    f"Matched product by default_code == catalog_object_id '{catalog_object_id}'"
                )

        if catalog_object_id and not product:
            square_api = self.env["square.api.client"]
            catalog_result = square_api.get_catalog_object(catalog_object_id)

            if catalog_result["success"] and not catalog_result.get("not_found"):
                sku = catalog_result.get("sku")
                _logger.info(
                    f"Retrieved SKU '{sku}' for catalog object '{catalog_object_id}'"
                )
            else:
                _logger.warning(
                    f"Could not retrieve catalog object details for '{catalog_object_id}': {catalog_result}"
                )

        if sku and not product:
            product = self.env["product.product"].search(
                [("default_code", "=", sku)], limit=1
            )
            _logger.info(
                f"Product search for SKU '{sku}': {'Found' if product else 'Not found'}"
            )

        if not sku and not product:
            _logger.warning(
                f"No SKU found for item: {item_name} (catalog_object_id: {catalog_object_id})"
            )

        is_placeholder_product = False
        if not product:
            if sku:
                _logger.error(f"Product with SKU '{sku}' not found in Odoo.")
            else:
                _logger.error(
                    f"Could not determine SKU for catalog object '{catalog_object_id}' - product not found in Odoo."
                )

            if not product:
                # Create or get a placeholder product if none found
                _logger.info(
                    f"Creating or finding placeholder product for Square sales."
                )
                product = self._get_or_create_square_default_product()
                is_placeholder_product = True

        quantity = int(quantity) if quantity else 1
        if quantity <= 0:
            quantity = 1

        vat_tax = self.env["account.tax"].search(
            [
                ("amount", "=", 20.0),
                ("type_tax_use", "=", "sale"),
                ("amount_type", "=", "percent"),
                ("price_include", "=", False),
            ],
            limit=1,
        )

        if vat_tax:
            total_dec = Decimal(str(total_amount))
            rate_dec = Decimal("1.2")
            tax_rate_dec = Decimal("0.2")
            subtotal_precise = total_dec / rate_dec
            tax_calc = subtotal_precise * tax_rate_dec
            subtotal_amount_dec = total_dec - tax_calc
            unit_price = float(subtotal_amount_dec / Decimal(quantity))
            tax_cmds = [(6, 0, vat_tax.ids)]
        else:
            base_money = line_item.get("base_price_money") or {}
            base_cents = base_money.get("amount")
            if base_cents is not None:
                unit_price = (float(base_cents) / 100.0) / quantity
            else:
                unit_price = total_amount / quantity if quantity else total_amount
            tax_cmds = [(6, 0, product.taxes_id.ids)] if product.taxes_id else [(5, 0, 0)]

        line_vals = {
            "order_id": sale_order.id,
            "product_id": product.id,
            "name": product.name,
            "product_uom_qty": quantity,
            "price_unit": unit_price,
            "tax_id": tax_cmds,
        }

        square_line_id = line_item.get("uid")
        if square_line_id and hasattr(self.env["sale.order.line"], "square_line_id"):
            line_vals["square_line_id"] = square_line_id
        
        # Store catalog_object_id for exchange tracking
        if catalog_object_id and hasattr(self.env["sale.order.line"], "square_catalog_id"):
            line_vals["square_catalog_id"] = catalog_object_id
        
        if is_placeholder_product:
            line_vals["name"] = f"Produit automatiquement créé depuis Square: {item_name}"

        line = self.env["sale.order.line"].create(line_vals)
        sale_order._compute_amounts()

        return line

    def _get_or_create_square_default_product(self):
        """Get or create 'Vente Square' service product"""
        try:
            # Try to find existing product first
            product = self.env["product.product"].search(
                [("name", "=", "Vente Square"), ("type", "=", "service")], limit=1
            )

            if product:
                _logger.info(
                    f"Found existing 'Vente Square' service product: {product.id}"
                )
                return product

            # Create the service product if it doesn't exist
            product_vals = {
                "name": "Vente Square",
                "default_code": "VENTE_SQUARE",
                "type": "service",  # Service product
                "categ_id": self.env.ref("product.product_category_all").id,
                "list_price": 0.0,
                "sale_ok": True,
                "purchase_ok": False,
                "description": "Service product for Square sales integration",
            }

            product = self.env["product.product"].create(product_vals)
            _logger.info(f"Created 'Vente Square' service product: {product.id}")
            return product

        except Exception as e:
            _logger.error(f"Error getting or creating 'Vente Square' product: {str(e)}")
            raise e

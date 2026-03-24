# -*- coding: utf-8 -*-
import requests
import json
import logging
from datetime import datetime
from odoo import models, api, fields
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class SquareApiClient(models.Model):
    _name = "square.api.client"
    _description = "Square API Client"

    @api.model
    def _get_square_config(self):
        """Get Square configuration"""
        config = self.env["square.config"].search([], limit=1)
        if not config:
            raise UserError(
                "No Square configuration found. Please configure Square integration first."
            )
        return config

    @api.model
    def _get_api_base_url(self, environment):
        """Get Square API base URL based on environment"""
        if environment == "production":
            return "https://connect.squareup.com"
        else:
            return "https://connect.squareupsandbox.com"

    @api.model
    def _make_api_request(self, endpoint, method="GET", data=None):
        """Make a request to Square API"""
        config = self._get_square_config()
        base_url = self._get_api_base_url(config.square_environment)
        url = f"{base_url}{endpoint}"

        headers = {
            "Authorization": f"Bearer {config.square_access_token}",
            "Content-Type": "application/json",
            "Square-Version": "2024-08-21",  # Latest Square API version
        }

        try:
            if method == "GET":
                response = requests.get(url, headers=headers, timeout=30)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=data, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            _logger.error(f"Square API request failed: {str(e)}")
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_data = e.response.json()
                    _logger.error(f"Square API error response: {error_data}")
                except:
                    _logger.error(f"Square API error response (raw): {e.response.text}")
            raise UserError(f"Failed to communicate with Square API: {str(e)}")

    @api.model
    def get_order(self, order_id):
        """
        Retrieve a complete order from Square API

        Args:
            order_id (str): The Square order ID

        Returns:
            dict: Complete order data from Square API
        """
        try:
            endpoint = f"/v2/orders/{order_id}"
            response = self._make_api_request(endpoint)

            if "order" in response:
                order_data = response["order"]
                return order_data
            else:
                _logger.error(
                    f"No order data found in Square API response for order {order_id}"
                )
                return None

        except Exception as e:
            _logger.error(f"Error fetching order {order_id} from Square API: {str(e)}")
            return None

    @api.model
    def get_customer(self, customer_id):
        """
        Retrieve customer details from Square API

        Args:
            customer_id (str): The Square customer ID

        Returns:
            dict: Customer data from Square API
        """
        try:
            endpoint = f"/v2/customers/{customer_id}"
            response = self._make_api_request(endpoint)

            if "customer" in response:
                customer_data = response["customer"]
                _logger.info(
                    f"Retrieved customer {customer_id}: {customer_data.get('given_name', '')} {customer_data.get('family_name', '')}"
                )
                return customer_data
            else:
                _logger.error(
                    f"No customer data found in Square API response for customer {customer_id}"
                )
                return None

        except Exception as e:
            _logger.error(
                f"Error retrieving customer {customer_id} from Square API: {str(e)}"
            )
            return None

    @api.model
    def get_location_orders(self, location_id, limit=100):
        """
        Search orders by location

        Args:
            location_id (str): Square location ID
            limit (int): Maximum number of orders to retrieve

        Returns:
            list: List of orders
        """
        try:
            endpoint = "/v2/orders/search"
            data = {
                "location_ids": [location_id],
                "limit": limit,
                "return_entries": True,
            }

            response = self._make_api_request(endpoint, method="POST", data=data)

            if "orders" in response:
                orders = response["orders"]
                return orders
            else:
                _logger.warning(f"No orders found for location {location_id}")
                return []

        except Exception as e:
            _logger.error(
                f"Error searching orders for location {location_id}: {str(e)}"
            )
            raise

    @api.model
    def test_connection(self):
        """
        Test the Square API connection

        Returns:
            dict: Connection test result
        """
        try:
            # Try to fetch locations as a simple test
            endpoint = "/v2/locations"
            response = self._make_api_request(endpoint)

            if "locations" in response:
                locations = response["locations"]
                return {
                    "success": True,
                    "message": f"Connection successful. Found {len(locations)} locations.",
                    "locations": locations,
                }
            else:
                return {
                    "success": False,
                    "message": "Connection successful but no locations found.",
                }

        except Exception as e:
            _logger.error(f"Square API connection test failed: {str(e)}")
            return {"success": False, "message": f"Connection failed: {str(e)}"}

    @api.model
    def search_catalog_by_sku(self, product_sku):
        """
        Search Square catalog for a product by SKU

        Args:
            product_sku (str): Product SKU to search for

        Returns:
            dict: Search result with success status and catalog_object_id
        """
        try:
            endpoint = "/v2/catalog/search"
            data = {
                "object_types": ["ITEM_VARIATION"],
                "query": {
                    "exact_query": {
                        "attribute_name": "sku",
                        "attribute_value": product_sku,
                    }
                },
            }

            response = self._make_api_request(endpoint, method="POST", data=data)

            if not response.get("objects"):
                _logger.warning(
                    f"No objects found in Square catalog for SKU {product_sku}"
                )
                return {"success": True, "catalog_object_id": None, "not_found": True}

            catalog_object_id = response["objects"][0]["id"]
            return {
                "success": True,
                "catalog_object_id": catalog_object_id,
                "not_found": False,
            }

        except Exception as e:
            _logger.error(
                f"Error searching Square catalog for SKU {product_sku}: {str(e)}"
            )
            return {"success": False, "catalog_object_id": None, "not_found": False}

    def get_catalog_object(self, catalog_object_id):
        """
        Get catalog object details from Square

        Args:
            catalog_object_id (str): Square catalog object ID

        Returns:
            dict: Catalog object details including SKU
        """
        try:
            endpoint = f"/v2/catalog/object/{catalog_object_id}"
            response = self._make_api_request(endpoint)

            if not response.get("object"):
                _logger.warning(f"No catalog object found for ID {catalog_object_id}")
                return {"success": False, "sku": None, "not_found": True}

            catalog_object = response["object"]

            # Extract SKU from the catalog object
            sku = None
            if catalog_object.get("item_variation_data"):
                sku = catalog_object["item_variation_data"].get("sku")

            return {
                "success": True,
                "sku": sku,
                "catalog_object": catalog_object,
                "not_found": False,
            }

        except Exception as e:
            _logger.error(f"Error getting catalog object {catalog_object_id}: {str(e)}")
            return {"success": False, "sku": None, "not_found": False}

    @api.model
    def get_inventory(self, catalog_object_id):
        """
        Get inventory for a catalog object from Square

        Args:
            catalog_object_id (str): Square catalog object ID

        Returns:
            dict: Inventory data from Square API
        """
        try:
            endpoint = f"/v2/inventory/{catalog_object_id}"
            response = self._make_api_request(endpoint)
            return response
        except Exception as e:
            _logger.error(
                f"Error getting Square inventory for {catalog_object_id}: {str(e)}"
            )
            return None


    @api.model
    def set_physical_count(
        self, catalog_object_id, location_id, target_quantity, product_sku
    ):
        """
        Set exact inventory count in Square using PHYSICAL_COUNT

        Args:
            catalog_object_id (str): Square catalog object ID
            location_id (str): Square location ID
            target_quantity (int): Exact quantity to set
            product_sku (str): Product SKU for logging

        Returns:
            bool: Success status
        """
        try:
            endpoint = "/v2/inventory/changes/batch-create"

            # Get current timestamp in ISO 8601 format for Square API
            occurred_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            data = {
                "idempotency_key": f"odoo-count-{product_sku}-{fields.Datetime.now().strftime('%Y%m%d%H%M%S')}",
                "changes": [
                    {
                        "type": "PHYSICAL_COUNT",
                        "physical_count": {
                            "reference_id": f"odoo-sync-{product_sku}-{fields.Datetime.now().strftime('%Y%m%d%H%M%S')}",
                            "catalog_object_id": catalog_object_id,
                            "state": "IN_STOCK",
                            "location_id": location_id,
                            "quantity": str(abs(target_quantity)),
                            "occurred_at": occurred_at,
                        },
                    }
                ],
                "ignore_unchanged_counts": False,  # Always apply the count
            }

            response = self._make_api_request(endpoint, method="POST", data=data)

            if response:
                return True
            return False

        except Exception as e:
            _logger.error(
                f"Error setting Square inventory count for {product_sku}: {str(e)}"
            )
            return False

    @api.model
    def get_locations(self):
        """
        Get all Square locations

        Returns:
            list: List of Square locations
        """
        try:
            endpoint = "/v2/locations"
            response = self._make_api_request(endpoint)
            return response.get("locations", [])
        except Exception as e:
            _logger.error(f"Error getting Square locations: {str(e)}")
            return []

    @api.model
    def get_payment(self, payment_id):
        """
        Retrieve payment details from Square API

        Args:
            payment_id (str): The Square payment ID

        Returns:
            dict: Payment data from Square API
        """
        try:
            endpoint = f"/v2/payments/{payment_id}"
            response = self._make_api_request(endpoint)

            if "payment" in response:
                payment_data = response["payment"]
                _logger.info(f"Retrieved payment {payment_id}")
                return payment_data
            else:
                _logger.error(
                    f"No payment data found in Square API response for payment {payment_id}"
                )
                return None

        except Exception as e:
            _logger.error(
                f"Error fetching payment {payment_id} from Square API: {str(e)}"
            )
            return None

    @api.model
    def get_first_active_location_id(self):
        """
        Get the first active Square location ID

        Returns:
            str: Location ID or None if not found
        """
        try:
            locations = self.get_locations()

            if not locations:
                _logger.error("No Square locations found")
                return None

            # Get the first active location
            for location in locations:
                if location.get("status") == "ACTIVE":
                    location_id = location.get("id")
                    location_name = location.get("name", "Unknown")
                    return location_id

            # If no active location found, use the first one
            location_id = locations[0].get("id")
            location_name = locations[0].get("name", "Unknown")
            _logger.warning(
                f"No active location found, using first location: {location_name} (ID: {location_id})"
            )
            return location_id

        except Exception as e:
            _logger.error(f"Error getting Square location ID: {str(e)}")
            return None

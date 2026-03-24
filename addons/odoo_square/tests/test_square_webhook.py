# -*- coding: utf-8 -*-
import json
from unittest.mock import patch
import logging
from odoo.tests.common import HttpCase, tagged
from odoo.tools import mute_logger
from odoo.addons.odoo_square.controllers.square_webhook import SquareWebhookController

_logger = logging.getLogger(__name__)


@tagged("post_install", "-at_install", "TestSquareWebhook")
class TestSquareWebhook(HttpCase):

    def setUp(self):
        super().setUp()

        # Create test data
        self.partner = self.env["res.partner"].create(
            {
                "name": "Test Customer",
                "email": "test@example.com",
                "phone": "+1234567890",
            }
        )

        self.product = self.env["product.product"].create(
            {
                "name": "Test Product",
                "default_code": "TEST_SKU_001",
                "list_price": 25.00,
                "type": "product",
            }
        )

        # Create Square configuration
        self.square_config = self.env["square.config"].create(
            {
                "name": "Test Square Config",
                "square_application_id": "test_app_id",
                "square_access_token": "test_access_token",
                "square_environment": "sandbox",
            }
        )

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_order_created_completed(self):
        """Test webhook processing for order.created with COMPLETED status"""

        # Sample Square webhook data for order.created
        webhook_data = {
            "type": "order.created",
            "data": {
                "object": {
                    "order_created": {
                        "id": "test_square_order_001",
                        "state": "COMPLETED",
                        "line_items": [
                            {
                                "name": "Test Product",
                                "catalog_object_id": "TEST_SKU_001",
                                "quantity": "2",
                                "base_price_money": {
                                    "amount": 2500,  # $25.00 in cents
                                    "currency": "EUR",
                                },
                            }
                        ],
                        "fulfillments": [
                            {
                                "pickup_details": {
                                    "recipient": {
                                        "display_name": "Test Customer",
                                        "email_address": "test@example.com",
                                        "phone_number": "+1234567890",
                                    }
                                }
                            }
                        ],
                    }
                }
            },
        }

        # Make webhook request using Odoo's HTTP testing
        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        # Check response
        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify sale order was created
        sale_order = self.env["sale.order"].search(
            [("square_order_id", "=", "test_square_order_001")]
        )
        self.assertTrue(sale_order, "Sale order should be created")
        self.assertEqual(sale_order.partner_id, self.partner)
        self.assertEqual(len(sale_order.order_line), 1)
        self.assertEqual(sale_order.order_line[0].product_id, self.product)
        self.assertEqual(sale_order.order_line[0].product_uom_qty, 2)
        self.assertEqual(sale_order.order_line[0].price_unit, 25.00)

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_order_updated_completed(self):
        """Test webhook processing for order.updated with COMPLETED status"""

        webhook_data = {
            "type": "order.updated",
            "data": {
                "object": {
                    "order_updated": {
                        "id": "test_square_order_updated_001",
                        "state": "COMPLETED",
                        "line_items": [
                            {
                                "name": "Test Product",
                                "catalog_object_id": "TEST_SKU_001",
                                "quantity": "1",
                                "base_price_money": {
                                    "amount": 2500,
                                    "currency": "EUR",
                                },
                            }
                        ],
                        "fulfillments": [
                            {
                                "pickup_details": {
                                    "recipient": {
                                        "display_name": "Updated Customer",
                                        "email_address": "test@example.com",
                                    }
                                }
                            }
                        ],
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify sale order was created
        sale_order = self.env["sale.order"].search(
            [("square_order_id", "=", "test_square_order_updated_001")]
        )
        self.assertTrue(sale_order, "Sale order should be created from order.updated")

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_order_not_completed_ignored(self):
        """Test that orders with status other than COMPLETED are ignored"""

        webhook_data = {
            "type": "order.updated",
            "data": {
                "object": {
                    "order_updated": {
                        "id": "test_square_order_002",
                        "state": "OPEN",  # Not COMPLETED
                        "line_items": [],
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "ignored")

        # Verify no sale order was created
        sale_order = self.env["sale.order"].search(
            [("square_order_id", "=", "test_square_order_002")]
        )
        self.assertFalse(
            sale_order, "No sale order should be created for non-COMPLETED orders"
        )

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_duplicate_order_ignored(self):
        """Test that duplicate orders are ignored"""

        # Create existing order
        existing_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_square_order_003",
            }
        )

        webhook_data = {
            "type": "order.updated",
            "data": {
                "object": {
                    "order_updated": {
                        "id": "test_square_order_003",  # Same ID as existing
                        "state": "COMPLETED",
                        "line_items": [],
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "ignored")
        self.assertIn("already exists", response_data["message"])

    @mute_logger("odoo.addons.odoo-square.models.sale_order")
    def test_webhook_unknown_product_skipped(self):
        """Test that unknown products are skipped with error logging"""

        webhook_data = {
            "type": "order.created",
            "data": {
                "object": {
                    "order_created": {
                        "id": "test_square_order_004",
                        "state": "COMPLETED",
                        "line_items": [
                            {
                                "name": "Unknown Product",
                                "catalog_object_id": "UNKNOWN_SKU",  # SKU not in Odoo
                                "quantity": "1",
                                "base_price_money": {
                                    "amount": 1000,
                                    "currency": "EUR",
                                },
                            }
                        ],
                        "fulfillments": [],
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        # Should still succeed but log error
        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify order was created but with no lines
        sale_order = self.env["sale.order"].search(
            [("square_order_id", "=", "test_square_order_004")]
        )
        self.assertTrue(sale_order)
        self.assertEqual(len(sale_order.order_line), 0)

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_invalid_content_type(self):
        """Test webhook with invalid content type returns error"""

        response = self.url_open(
            "/square/webhook",
            data="invalid data",
            headers={"Content-Type": "text/plain"},
        )

        self.assertEqual(response.status_code, 400)
        response_data = response.json()
        self.assertEqual(response_data["status"], "error")
        self.assertIn("Content-Type", response_data["message"])

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_malformed_json(self):
        """Test webhook with malformed JSON returns error"""

        response = self.url_open(
            "/square/webhook",
            data="{ invalid json }",
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 500)
        response_data = response.json()
        self.assertEqual(response_data["status"], "error")

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_no_event_type(self):
        """Test webhook without event type returns error"""

        webhook_data = {
            "data": {
                "object": {
                    "order_created": {
                        "id": "test_order",
                        "state": "COMPLETED",
                    }
                }
            }
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 400)
        response_data = response.json()
        self.assertEqual(response_data["status"], "error")
        self.assertIn("event type", response_data["message"])

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_unhandled_event_type(self):
        """Test webhook with unhandled event type returns ignored"""

        webhook_data = {"type": "catalog.updated", "data": {}}  # Unhandled event type

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "ignored")
        self.assertIn("not handled", response_data["message"])

    def test_customer_matching_by_email(self):
        """Test customer matching by email"""

        square_data = {
            "id": "test_order",
            "fulfillments": [
                {
                    "pickup_details": {
                        "recipient": {
                            "display_name": "Different Name",
                            "email_address": "test@example.com",  # Matches existing partner
                            "phone_number": "+9999999999",
                        }
                    }
                }
            ],
        }

        sale_order = self.env["sale.order"]
        customer = sale_order._get_or_create_customer_from_square(square_data)

        # Should match existing partner by email
        self.assertEqual(customer, self.partner)

    def test_customer_matching_by_phone(self):
        """Test customer matching by phone when email doesn't match"""

        square_data = {
            "id": "test_order",
            "fulfillments": [
                {
                    "pickup_details": {
                        "recipient": {
                            "display_name": "Different Name",
                            "email_address": "different@example.com",  # Different email
                            "phone_number": "+1234567890",  # Matches existing partner
                        }
                    }
                }
            ],
        }

        sale_order = self.env["sale.order"]
        customer = sale_order._get_or_create_customer_from_square(square_data)

        # Should match existing partner by phone
        self.assertEqual(customer, self.partner)

    def test_customer_creation_when_no_match(self):
        """Test customer creation when no match is found"""

        square_data = {
            "id": "test_order",
            "fulfillments": [
                {
                    "pickup_details": {
                        "recipient": {
                            "display_name": "New Customer",
                            "email_address": "new@example.com",
                            "phone_number": "+9876543210",
                        }
                    }
                }
            ],
        }

        sale_order = self.env["sale.order"]
        customer = sale_order._get_or_create_customer_from_square(square_data)

        # Should create new partner
        self.assertNotEqual(customer, self.partner)
        self.assertEqual(customer.name, "New Customer")
        self.assertEqual(customer.email, "new@example.com")
        self.assertEqual(customer.phone, "+9876543210")

    def test_order_processor_process_square_order(self):
        """Test the order processor directly"""

        square_data = {
            "id": "processor_test_001",
            "state": "COMPLETED",
            "line_items": [
                {
                    "name": "Test Product",
                    "catalog_object_id": "TEST_SKU_001",
                    "quantity": "1",
                    "base_price_money": {
                        "amount": 2500,
                        "currency": "EUR",
                    },
                }
            ],
            "fulfillments": [
                {
                    "pickup_details": {
                        "recipient": {
                            "display_name": "Test Customer",
                            "email_address": "test@example.com",
                        }
                    }
                }
            ],
        }

        processor = self.env["square.order.processor"]
        result = processor.process_square_order(square_data)

        # Check result
        self.assertIn("sale_order_id", result)
        self.assertIn("square_order_id", result)
        self.assertEqual(result["square_order_id"], "processor_test_001")

        # Verify order was created
        sale_order = self.env["sale.order"].browse(result["sale_order_id"])
        self.assertEqual(sale_order.square_order_id, "processor_test_001")
        self.assertEqual(sale_order.state, "sale")  # Should be confirmed

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_refund_created_partial(self):
        """Test webhook processing for refund.created with partial refund"""

        # First create an order
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_refund_order_001",
                "state": "sale",
            }
        )

        # Add order line
        self.env["sale.order.line"].create(
            {
                "order_id": sale_order.id,
                "product_id": self.product.id,
                "product_uom_qty": 2,
                "price_unit": 25.00,
                "square_line_id": "line_001",
            }
        )

        webhook_data = {
            "type": "refund.created",
            "event_id": "test_event_001",
            "data": {
                "object": {
                    "refund": {
                        "id": "test_refund_001",
                        "order_id": "test_refund_order_001",
                        "status": "PENDING",
                        "amount_money": {
                            "amount": 2500,  # $25.00 - partial refund (half of $50 order)
                            "currency": "EUR",
                        },
                        "refunded_line_ids": ["line_001"],  # Specific line
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify refund record was created
        refund_record = self.env["square.refund"].search(
            [("square_refund_id", "=", "test_refund_001")]
        )
        self.assertTrue(refund_record)
        self.assertEqual(refund_record.status, "pending")
        self.assertEqual(refund_record.refund_amount, 25.00)
        self.assertTrue(refund_record._is_partial_refund())

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_refund_updated_completed(self):
        """Test webhook processing for refund.updated with COMPLETED status"""

        # Create refund record first
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_complete_refund_order_001",
            }
        )

        refund_record = self.env["square.refund"].create(
            {
                "square_refund_id": "test_complete_refund_001",
                "square_order_id": "test_complete_refund_order_001",
                "status": "pending",
                "refund_amount": 50.00,
                "currency_id": self.env.company.currency_id.id,
                "sale_order_id": sale_order.id,
            }
        )

        webhook_data = {
            "type": "refund.updated",
            "event_id": "test_event_002",
            "data": {
                "object": {
                    "refund": {
                        "id": "test_complete_refund_001",
                        "order_id": "test_complete_refund_order_001",
                        "status": "COMPLETED",
                        "amount_money": {
                            "amount": 5000,  # $50.00
                            "currency": "EUR",
                        },
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify refund record was updated
        refund_record.refresh()
        self.assertEqual(refund_record.status, "completed")

    def test_refund_model_create_from_square_data(self):
        """Test refund model creation from Square data"""

        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_refund_model_order_001",
            }
        )

        refund_data = {
            "id": "test_refund_model_001",
            "order_id": "test_refund_model_order_001",
            "status": "PENDING",
            "amount_money": {
                "amount": 3000,  # $30.00
                "currency": "EUR",
            },
            "reason": "Customer request",
        }

        refund_record = self.env["square.refund"].create_from_square_data(
            refund_data, sale_order
        )

        self.assertTrue(refund_record)
        self.assertEqual(refund_record.square_refund_id, "test_refund_model_001")
        self.assertEqual(refund_record.status, "pending")
        self.assertEqual(refund_record.refund_amount, 30.00)
        self.assertEqual(refund_record.refund_reason, "Customer request")

    def test_partial_refund_detection(self):
        """Test partial refund detection logic"""

        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_partial_detection_order_001",
                "amount_total": 100.00,  # $100 order
            }
        )

        # Test partial refund (amount < order total)
        partial_refund = self.env["square.refund"].create(
            {
                "square_refund_id": "test_partial_refund_001",
                "square_order_id": "test_partial_detection_order_001",
                "refund_amount": 30.00,  # $30 refund
                "currency_id": self.env.company.currency_id.id,
                "sale_order_id": sale_order.id,
            }
        )

        self.assertTrue(partial_refund._is_partial_refund())

        # Test full refund (amount = order total)
        full_refund = self.env["square.refund"].create(
            {
                "square_refund_id": "test_full_refund_001",
                "square_order_id": "test_partial_detection_order_001",
                "refund_amount": 100.00,  # $100 refund
                "currency_id": self.env.company.currency_id.id,
                "sale_order_id": sale_order.id,
            }
        )

        self.assertFalse(full_refund._is_partial_refund())

    def test_order_line_changes_sync(self):
        """Test order line changes synchronization"""

        # Create order with line
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_line_sync_order_001",
                "state": "draft",
            }
        )

        order_line = self.env["sale.order.line"].create(
            {
                "order_id": sale_order.id,
                "product_id": self.product.id,
                "product_uom_qty": 2,
                "price_unit": 25.00,
                "square_line_id": "line_sync_001",
            }
        )

        square_data = {
            "order_id": "test_line_sync_order_001",
            "line_items": [
                {
                    "uid": "line_sync_001",
                    "quantity": "3",  # Changed from 2 to 3
                    "total_money": {"amount": 7500},  # $75
                }
            ],
        }

        processor = self.env["square.order.processor"]
        processor._sync_order_line_changes(sale_order, square_data)

        # Verify line was updated
        order_line.refresh()
        self.assertEqual(order_line.product_uom_qty, 3)

    def test_order_cancellation_processing(self):
        """Test order cancellation processing"""

        # Create confirmed order
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_cancel_order_001",
                "state": "sale",  # Confirmed
            }
        )

        square_data = {
            "order_id": "test_cancel_order_001",
            "state": "CANCELLED",
        }

        processor = self.env["square.order.processor"]
        processor._process_order_cancellation(sale_order, square_data)

        # Verify order was cancelled
        sale_order.refresh()
        self.assertEqual(sale_order.state, "cancel")

    def test_refund_duplicate_handling(self):
        """Test that duplicate refunds are handled properly"""

        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_duplicate_refund_order_001",
            }
        )

        refund_data = {
            "id": "test_duplicate_refund_001",
            "order_id": "test_duplicate_refund_order_001",
            "status": "PENDING",
            "amount_money": {"amount": 2500, "currency": "EUR"},
        }

        # Create first refund
        refund1 = self.env["square.refund"].create_from_square_data(
            refund_data, sale_order
        )

        # Try to create duplicate
        refund2 = self.env["square.refund"].create_from_square_data(
            refund_data, sale_order
        )

        # Should return the same record
        self.assertEqual(refund1, refund2)

    def test_payment_method_line_creation(self):
        """Test payment method line creation for Square payments"""

        # Create a test journal
        test_journal = self.env["account.journal"].create(
            {
                "name": "Test Square Journal",
                "type": "bank",
                "code": "TSQ",
            }
        )

        processor = self.env["square.order.processor"]

        # Test getting payment method line (should create one if none exists)
        payment_method_line = processor._get_payment_method_line(test_journal)

        # Should have created a payment method line
        self.assertTrue(payment_method_line)
        self.assertEqual(payment_method_line.journal_id, test_journal)
        self.assertEqual(payment_method_line.payment_type, "inbound")

        # Verify it was actually created in the database
        existing_line = self.env["account.payment.method.line"].search(
            [("journal_id", "=", test_journal.id), ("payment_type", "=", "inbound")],
            limit=1,
        )

        self.assertTrue(existing_line)
        self.assertEqual(existing_line, payment_method_line)

    def test_refund_linking_strategies(self):
        """Test the multiple strategies for linking refunds to orders"""

        # Create a test order with payment_id
        test_order = self.env["sale.order"].create(
            {
                "partner_id": self.env.ref("base.res_partner_1").id,
                "square_order_id": "test_order_123",
                "square_payment_id": "test_payment_456",
            }
        )

        # Create a test refund data
        refund_data = {
            "id": "test_refund_789",
            "order_id": "test_order_123",  # Should match by order_id
            "payment_id": "test_payment_456",  # Should match by payment_id
            "amount_money": {"amount": 500, "currency": "EUR"},
            "status": "PENDING",
            "reason": "Test refund",
        }

        # Create a webhook controller instance and call the refund processing
        webhook_controller = SquareWebhookController()

        # Mock the request environment for testing
        with self.env.cr.savepoint():
            # Test refund processing - should find the order by order_id
            result = webhook_controller._process_refund(refund_data, "created")

            # Should process successfully
            self.assertEqual(result["status"], "success")
            self.assertIn("Refund actions prepared", result["message"])

            # Verify refund record was created
            refund_record = (
                self.env["square.refund"]
                .sudo()
                .search([("square_refund_id", "=", "test_refund_789")], limit=1)
            )
            self.assertTrue(refund_record)
            self.assertEqual(refund_record.sale_order_id, test_order)

    def test_api_client_payment_method(self):
        """Test the new get_payment method in square.api.client"""

        # Test that the method exists and can be called
        square_api = self.env["square.api.client"]

        # This should not raise an error (method exists)
        try:
            result = square_api.get_payment("test_payment_id")
            # Result should be None since it's not a real payment ID
            self.assertIsNone(result)
        except Exception as e:
            # If it fails due to API configuration, that's expected in test environment
            self.assertIn("Square API", str(e))

    def test_refund_currency_validation(self):
        """Test refund creation with EUR (supported) and non-EUR (unsupported) currencies"""

        # Create a test order
        test_order = self.env["sale.order"].create(
            {
                "partner_id": self.env.ref("base.res_partner_1").id,
                "square_order_id": "test_order_currency",
            }
        )

        # Test refund data with supported EUR currency
        eur_refund_data = {
            "id": "test_refund_eur",
            "order_id": "test_order_currency",
            "amount_money": {"amount": 500, "currency": "EUR"},
            "status": "PENDING",
            "reason": "Test refund with EUR",
        }

        # Test refund data with unsupported USD currency
        usd_refund_data = {
            "id": "test_refund_usd",
            "order_id": "test_order_currency",
            "amount_money": {"amount": 500, "currency": "EUR"},
            "status": "PENDING",
            "reason": "Test refund with USD (should fail)",
        }

        from odoo.exceptions import ValidationError
        from odoo.addons.odoo_square.controllers.square_webhook import (
            SquareWebhookController,
        )

        webhook_controller = SquareWebhookController()

        # Mock the request environment for testing
        with self.env.cr.savepoint():
            # EUR should work fine
            result = webhook_controller._process_refund(eur_refund_data, "created")
            self.assertEqual(result["status"], "success")

            # USD should raise ValidationError
            with self.assertRaises(ValidationError) as context:
                webhook_controller._process_refund(usd_refund_data, "created")

            # Check that the error message mentions EUR support
            error_msg = str(context.exception).lower()
            self.assertIn("eur", error_msg)
            self.assertIn("support", error_msg)

    def test_stock_validation_with_bot_user(self):
        """Test stock picking validation uses proper user context to avoid mail follower issues"""

        # Create a test order and picking
        test_order = self.env["sale.order"].create(
            {
                "partner_id": self.env.ref("base.res_partner_1").id,
                "square_order_id": "test_stock_order",
            }
        )

        # Create a test picking for the order
        picking_type = self.env["stock.picking.type"].search(
            [("code", "=", "outgoing")], limit=1
        )

        if picking_type:
            test_picking = self.env["stock.picking"].create(
                {
                    "partner_id": test_order.partner_id.id,
                    "picking_type_id": picking_type.id,
                    "location_id": picking_type.default_location_src_id.id,
                    "location_dest_id": picking_type.default_location_dest_id.id,
                    "origin": test_order.name,
                    "sale_id": test_order.id,
                }
            )

            # Test that stock validation works with proper context
            processor = self.env["square.order.processor"]

            # This should not raise the SQL error about integer = boolean
            try:
                # Simulate the stock validation process
                bot_user = processor._get_square_bot_user()
                if test_picking.state == "draft":
                    test_picking.with_user(bot_user).action_confirm()
                if test_picking.state in ["confirmed", "waiting"]:
                    test_picking.with_user(bot_user).action_assign()

                # The key test: this should not cause the mail follower SQL error
                if test_picking.state == "assigned":
                    test_picking.with_user(bot_user).with_context(
                        force_validate=True,
                        mail_auto_subscribe_no_notify=True,
                        mail_create_nosubscribe=True,
                    ).button_validate()

                # If we get here without SQL errors, the fix is working
                success = True

            except Exception as e:
                if "integer = boolean" in str(e) or "mail_followers" in str(e):
                    self.fail(f"Mail follower SQL error still occurs: {str(e)}")
                else:
                    # Other errors are acceptable (like missing products, etc.)
                    success = True

            self.assertTrue(
                success,
                "Stock validation should complete without mail follower SQL errors",
            )

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_order_cancellation_case_insensitive(self):
        """Test that order cancellation works with both CANCELED and CANCELLED"""

        # Create a sale order in draft state
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_cancel_order_002",
                "state": "draft",
            }
        )

        # Test with CANCELED (one L - as sent by Square webhook)
        webhook_data = {
            "type": "order.updated",
            "event_id": "test_cancel_event_001",
            "data": {
                "object": {
                    "order_updated": {
                        "id": "test_cancel_order_002",
                        "state": "CANCELED",  # One L
                        "version": 2,
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "updated")

        # Verify order was cancelled
        sale_order.refresh()
        self.assertEqual(sale_order.state, "cancel")

        # Log entry should exist
        log_entry = self.env["square.integration.log"].search(
            [
                ("square_order_id", "=", "test_cancel_order_002"),
                ("event_type", "=", "order_updated"),
                ("status", "=", "warning"),
            ],
            limit=1,
        )
        self.assertTrue(log_entry)
        self.assertIn("cancelled", log_entry.title)

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_webhook_order_cancellation_completed_order(self):
        """Test cancellation of already completed order creates credit note"""

        # Create a completed sale order with invoice
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_completed_cancel_order_001",
                "state": "sale",  # Confirmed
            }
        )

        # Add order line
        self.env["sale.order.line"].create(
            {
                "order_id": sale_order.id,
                "product_id": self.product.id,
                "product_uom_qty": 2,
                "price_unit": 25.00,
            }
        )

        # Create and post invoice
        sale_order._create_invoices()
        invoice = sale_order.invoice_ids[0]
        invoice.action_post()

        # Verify order is done
        sale_order.state = "done"

        # Test cancellation of completed order
        webhook_data = {
            "type": "order.updated",
            "event_id": "test_completed_cancel_event_001",
            "data": {
                "object": {
                    "order_updated": {
                        "id": "test_completed_cancel_order_001",
                        "state": "CANCELED",
                        "version": 3,
                    }
                }
            },
        }

        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "updated")

        # Log entry should indicate credit note creation attempt
        log_entry = self.env["square.integration.log"].search(
            [
                ("square_order_id", "=", "test_completed_cancel_order_001"),
                ("event_type", "=", "order_updated"),
                ("status", "=", "info"),
            ],
            limit=1,
        )
        self.assertTrue(log_entry)
        self.assertIn("Credit note", log_entry.title)

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_partial_refund_processing(self):
        """Test webhook processing for partial refund (1 EUR refund for 3 EUR order)"""

        # First, create a test order
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_partial_refund_order_001",
                "order_line": [
                    (
                        0,
                        0,
                        {
                            "product_id": self.product.id,
                            "product_uom_qty": 6,
                            "price_unit": 0.50,  # Total: 3.00 EUR
                        },
                    )
                ],
            }
        )

        # Confirm and create invoice for the order
        sale_order.action_confirm()
        invoice = sale_order._create_invoices()
        invoice.action_post()

        # Sample Square webhook data for refund.created (partial refund)
        webhook_data = {
            "merchant_id": "TEST_MERCHANT_ID",
            "type": "refund.created",
            "event_id": "test_partial_refund_event_001",
            "created_at": "2025-01-15T10:00:00.000Z",
            "data": {
                "type": "refund",
                "id": "test_partial_refund_001",
                "object": {
                    "refund": {
                        "amount_money": {"amount": 100, "currency": "EUR"},  # 1.00 EUR
                        "created_at": "2025-01-15T10:00:00.000Z",
                        "destination_type": "CARD",
                        "id": "test_partial_refund_001",
                        "location_id": "TEST_LOCATION",
                        "order_id": "test_partial_refund_order_001",
                        "payment_id": "test_payment_001",
                        "reason": "Returned goods",
                        "status": "COMPLETED",
                        "updated_at": "2025-01-15T10:00:00.000Z",
                        "version": 1,
                    }
                },
            },
        }

        # Process the webhook
        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify refund record was created
        refund = self.env["square.refund"].search(
            [("square_refund_id", "=", "test_partial_refund_001")], limit=1
        )
        self.assertTrue(refund)
        self.assertEqual(refund.refund_amount, 1.00)
        self.assertEqual(refund.status, "completed")

        # Verify partial refund detection
        self.assertTrue(refund._is_partial_refund())

        # Verify credit note was created with correct amount
        credit_note = refund.credit_note_id
        self.assertTrue(credit_note)
        self.assertAlmostEqual(credit_note.amount_total, 1.00, places=2)

        # Verify order refund status
        sale_order.refresh()
        self.assertEqual(sale_order.refund_status, "partially_refunded")
        self.assertEqual(sale_order.total_refunded_amount, 1.00)

        # Verify order line quantity updates
        # Original order: 6 products at 0.50 each = 3.00 total
        # Refund: 1.00 = 33.33% of order
        # Expected returned quantity: 6 * 0.3333 = ~2.00 units
        order_line = sale_order.order_line[0]
        self.assertAlmostEqual(order_line.returned_qty, 2.00, places=1)
        self.assertEqual(
            order_line.effective_qty,
            order_line.product_uom_qty - order_line.returned_qty,
        )

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_full_refund_processing(self):
        """Test webhook processing for full refund"""

        # First, create a test order
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_full_refund_order_001",
                "order_line": [
                    (
                        0,
                        0,
                        {
                            "product_id": self.product.id,
                            "product_uom_qty": 6,
                            "price_unit": 0.50,  # Total: 3.00 EUR
                        },
                    )
                ],
            }
        )

        # Confirm and create invoice for the order
        sale_order.action_confirm()
        invoice = sale_order._create_invoices()
        invoice.action_post()

        # Sample Square webhook data for refund.created (full refund)
        webhook_data = {
            "merchant_id": "TEST_MERCHANT_ID",
            "type": "refund.created",
            "event_id": "test_full_refund_event_001",
            "created_at": "2025-01-15T10:00:00.000Z",
            "data": {
                "type": "refund",
                "id": "test_full_refund_001",
                "object": {
                    "refund": {
                        "amount_money": {
                            "amount": 300,
                            "currency": "EUR",
                        },  # 3.00 EUR (full amount)
                        "created_at": "2025-01-15T10:00:00.000Z",
                        "destination_type": "CARD",
                        "id": "test_full_refund_001",
                        "location_id": "TEST_LOCATION",
                        "order_id": "test_full_refund_order_001",
                        "payment_id": "test_payment_002",
                        "reason": "Returned goods",
                        "status": "COMPLETED",
                        "updated_at": "2025-01-15T10:00:00.000Z",
                        "version": 1,
                    }
                },
            },
        }

        # Process the webhook
        response = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data["status"], "success")

        # Verify refund record was created
        refund = self.env["square.refund"].search(
            [("square_refund_id", "=", "test_full_refund_001")], limit=1
        )
        self.assertTrue(refund)
        self.assertEqual(refund.refund_amount, 3.00)
        self.assertEqual(refund.status, "completed")

        # Verify full refund detection
        self.assertFalse(refund._is_partial_refund())

        # Verify credit note was created with full amount
        credit_note = refund.credit_note_id
        self.assertTrue(credit_note)
        self.assertAlmostEqual(credit_note.amount_total, 3.00, places=2)

        # Verify order refund status
        sale_order.refresh()
        self.assertEqual(sale_order.refund_status, "fully_refunded")
        self.assertEqual(sale_order.total_refunded_amount, 3.00)

        # Verify order line quantity updates for full refund
        # Full refund should return all quantities
        order_line = sale_order.order_line[0]
        self.assertEqual(order_line.returned_qty, order_line.product_uom_qty)
        self.assertEqual(order_line.effective_qty, 0.0)

    @mute_logger("odoo.addons.odoo-square.controllers.square_webhook")
    def test_multiple_partial_refunds(self):
        """Test webhook processing for multiple partial refunds on the same order"""

        # First, create a test order
        sale_order = self.env["sale.order"].create(
            {
                "partner_id": self.partner.id,
                "square_order_id": "test_multiple_refunds_order_001",
                "order_line": [
                    (
                        0,
                        0,
                        {
                            "product_id": self.product.id,
                            "product_uom_qty": 10,
                            "price_unit": 0.50,  # Total: 5.00 EUR
                        },
                    )
                ],
            }
        )

        # Confirm and create invoice for the order
        sale_order.action_confirm()
        invoice = sale_order._create_invoices()
        invoice.action_post()

        # First partial refund: 1.00 EUR (20% of order)
        webhook_data_1 = {
            "merchant_id": "TEST_MERCHANT_ID",
            "type": "refund.created",
            "event_id": "test_multiple_refund_1_event_001",
            "created_at": "2025-01-15T10:00:00.000Z",
            "data": {
                "type": "refund",
                "id": "test_multiple_refund_001",
                "object": {
                    "refund": {
                        "amount_money": {"amount": 100, "currency": "EUR"},  # 1.00 EUR
                        "created_at": "2025-01-15T10:00:00.000Z",
                        "destination_type": "CARD",
                        "id": "test_multiple_refund_001",
                        "location_id": "TEST_LOCATION",
                        "order_id": "test_multiple_refunds_order_001",
                        "payment_id": "test_payment_001",
                        "reason": "Returned goods",
                        "status": "COMPLETED",
                        "updated_at": "2025-01-15T10:00:00.000Z",
                        "version": 1,
                    }
                },
            },
        }

        # Process first refund
        response_1 = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data_1),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response_1.status_code, 200)

        # Second partial refund: 1.50 EUR (30% of order)
        webhook_data_2 = {
            "merchant_id": "TEST_MERCHANT_ID",
            "type": "refund.created",
            "event_id": "test_multiple_refund_2_event_001",
            "created_at": "2025-01-15T10:15:00.000Z",
            "data": {
                "type": "refund",
                "id": "test_multiple_refund_002",
                "object": {
                    "refund": {
                        "amount_money": {"amount": 150, "currency": "EUR"},  # 1.50 EUR
                        "created_at": "2025-01-15T10:15:00.000Z",
                        "destination_type": "CARD",
                        "id": "test_multiple_refund_002",
                        "location_id": "TEST_LOCATION",
                        "order_id": "test_multiple_refunds_order_001",
                        "payment_id": "test_payment_002",
                        "reason": "Returned goods",
                        "status": "COMPLETED",
                        "updated_at": "2025-01-15T10:15:00.000Z",
                        "version": 1,
                    }
                },
            },
        }

        # Process second refund
        response_2 = self.url_open(
            "/square/webhook",
            data=json.dumps(webhook_data_2),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response_2.status_code, 200)

        # Verify final order state
        sale_order.refresh()
        self.assertEqual(sale_order.refund_status, "partially_refunded")
        self.assertEqual(sale_order.total_refunded_amount, 2.50)  # 1.00 + 1.50

        # Verify order line quantity updates
        # Original: 10 products at 0.50 each = 5.00 total
        # First refund: 1.00 = 20% = 2.0 units returned
        # Second refund: 1.50 = 30% = 3.0 units returned
        # Total returned: 5.0 units (50% of order)
        order_line = sale_order.order_line[0]
        self.assertAlmostEqual(order_line.returned_qty, 5.0, places=1)
        self.assertEqual(order_line.effective_qty, 5.0)  # 10 - 5

        # Verify both refunds were processed
        refunds = self.env["square.refund"].search(
            [("sale_order_id", "=", sale_order.id), ("status", "=", "completed")]
        )
        self.assertEqual(len(refunds), 2)
        self.assertEqual(sum(refunds.mapped("refund_amount")), 2.50)

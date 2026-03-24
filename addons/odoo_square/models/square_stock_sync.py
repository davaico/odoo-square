# -*- coding: utf-8 -*-
import logging
from odoo import models, fields, api

_logger = logging.getLogger(__name__)


class SquareStockSync(models.Model):
    _name = "square.stock.sync"
    _description = "Square Stock Synchronization Service"
    _rec_name = "name"

    name = fields.Char(string="Sync Name", default="Square Stock Sync", readonly=True)

    # Sync Statistics
    last_sync_date = fields.Datetime(string="Last Sync Date", readonly=True)
    total_syncs = fields.Integer(string="Total Syncs", default=0, readonly=True)
    successful_syncs = fields.Integer(
        string="Successful Syncs", default=0, readonly=True
    )
    failed_syncs = fields.Integer(string="Failed Syncs", default=0, readonly=True)

    # Configuration
    auto_sync_enabled = fields.Boolean(
        string="Auto Sync Enabled",
        default=True,
        help="Automatically sync stock changes to Square",
    )

    def _get_square_api_client(self):
        """Get Square API client instance"""
        return self.env["square.api.client"]

    def _search_square_catalog_by_sku(self, product_sku):
        """Search Square catalog for a product by SKU"""
        api_client = self._get_square_api_client()
        return api_client.search_catalog_by_sku(product_sku)

    def _get_configured_warehouse(self):
        """Get the configured warehouse from Square configuration (legacy method)"""
        api_client = self._get_square_api_client()
        config = api_client._get_square_config()
        return config.get_configured_warehouse()

    def _get_warehouse_for_location(self, square_location_id):
        """Get the warehouse mapped to a specific Square location"""
        api_client = self._get_square_api_client()
        config = api_client._get_square_config()
        if config:
            return config.get_warehouse_for_location(square_location_id)
        return self._get_configured_warehouse()

    def _get_square_location_id(self):
        """Get the first available Square location ID"""
        api_client = self._get_square_api_client()
        return api_client.get_first_active_location_id()

    def _is_square_sourced_move(self, stock_move):
        """Check if a stock move originated from a Square order"""
        # Check if the move is related to a sale order with square_order_id
        if stock_move.sale_line_id and stock_move.sale_line_id.order_id.square_order_id:
            return True

        # Check if the move has Square origin reference
        if stock_move.origin and "Square" in stock_move.origin:
            return True

        return False

    def _is_configured_warehouse_move(self, stock_move):
        """Check if the move involves the configured warehouse"""
        configured_warehouse = self._get_configured_warehouse()

        # Check source and destination locations
        warehouse_locations = (
            configured_warehouse.lot_stock_id | configured_warehouse.view_location_id
        )

        source_is_warehouse = stock_move.location_id in warehouse_locations
        dest_is_warehouse = stock_move.location_dest_id in warehouse_locations

        return source_is_warehouse or dest_is_warehouse

    def _should_sync_product(self, product):
        """Check if product should be synced to Square"""
        # Only sync products with SKU (default_code)
        if not product.default_code:
            _logger.info(f"Skipping product {product.name} - no SKU")
            return False

        # Only sync stockable products
        if product.type != "product":
            _logger.info(f"Skipping product {product.name} - not stockable")
            return False

        return True

    def _get_product_square_inventory(self, product_sku):
        """Get current inventory from Square for a product"""
        try:
            # Search for the product in Square catalog
            search_result = self._search_square_catalog_by_sku(product_sku)

            if not search_result["success"]:
                _logger.error(f"Failed to get Square catalog for SKU {product_sku}")
                return None

            if search_result["not_found"]:
                _logger.warning(
                    f"Product with SKU {product_sku} not found in Square catalog"
                )
                return None

            catalog_object_id = search_result["catalog_object_id"]

            # Get inventory for the catalog object
            api_client = self._get_square_api_client()
            return api_client.get_inventory(catalog_object_id)

        except Exception as e:
            _logger.error(f"Error getting Square inventory for {product_sku}: {str(e)}")
            return None

    def _update_square_inventory(self, product_sku, new_quantity):
        """Update inventory in Square for a specific product - Odoo is source of truth"""
        try:
            # Search for the product in Square catalog
            search_result = self._search_square_catalog_by_sku(product_sku)

            if not search_result["success"]:
                _logger.error(f"Failed to find Square catalog for SKU {product_sku}")
                return False

            if search_result["not_found"]:
                _logger.warning(
                    f"Product with SKU {product_sku} not found in Square catalog - skipping sync"
                )
                return True  # Return True to avoid retry, product doesn't exist in Square

            catalog_object_id = search_result["catalog_object_id"]

            # Get Square location ID
            location_id = self._get_square_location_id()
            if not location_id:
                _logger.error(
                    f"Cannot update inventory for {product_sku}: No Square location ID available"
                )
                return False

            _logger.info(
                f"Setting Square inventory for {product_sku} to {new_quantity} units (Odoo source of truth)"
            )

            # Simply set the exact quantity from Odoo - no adjustments needed
            api_client = self._get_square_api_client()
            result = api_client.set_physical_count(
                catalog_object_id, location_id, int(new_quantity), product_sku
            )

            if not result:
                _logger.error(f"Square API failed to set inventory for {product_sku}")
                self.env["square.integration.log"].log_error(
                    title=f"Square API Error - Sync refused",
                    error_message=f"Square API refused stock sync for {product_sku}",
                    technical_details=f"SKU: {product_sku}, Odoo Quantity: {new_quantity}",
                )

            return result

        except Exception as e:
            _logger.error(
                f"Error updating Square inventory for {product_sku}: {str(e)}"
            )
            return False

    def sync_product_stock(self, product):
        """Sync stock for a specific product to Square"""
        if not self._should_sync_product(product):
            return True

        try:
            # Get current stock quantity in configured warehouse
            configured_warehouse = self._get_configured_warehouse()
            stock_quant = self.env["stock.quant"].search(
                [
                    ("product_id", "=", product.id),
                    ("location_id", "child_of", configured_warehouse.lot_stock_id.id),
                ]
            )

            total_quantity = sum(quant.quantity for quant in stock_quant)

            _logger.info(
                f"Syncing {product.default_code} to Square: {total_quantity} units"
            )

            success = self._update_square_inventory(
                product.default_code, total_quantity
            )

            # Log to integration dashboard
            if success:
                self.env["square.integration.log"].log_square_event(
                    event_type="stock_sync",
                    title=f"Stock synchronized for {product.name}",
                    description=f"""
                        <h4>Square Stock Synchronization Successful</h4>
                        <ul>
                            <li><strong>Product:</strong> {product.name}</li>
                            <li><strong>SKU :</strong> <code>{product.default_code}</code></li>
                            <li><strong>Synchronized quantity:</strong> {total_quantity}</li>
                            <li><strong>Warehouse:</strong> {configured_warehouse.name}</li>
                        </ul>
                        <p><em>Stock automatically synchronized to Square</em></p>
                    """,
                    status="success",
                )
            else:
                self.env["square.integration.log"].log_error(
                    title=f"Stock sync failed for {product.name}",
                    error_message=f"Unable to synchronize stock for product {product.name} (SKU: {product.default_code}) vers Square",
                    technical_details=f"Produit: {product.name}, SKU: {product.default_code}, Quantité: {total_quantity}, Entrepôt: {configured_warehouse.name}",
                )

            # Update sync statistics
            self.total_syncs += 1
            if success:
                self.successful_syncs += 1
            else:
                self.failed_syncs += 1

            self.last_sync_date = fields.Datetime.now()

            return success

        except Exception as e:
            _logger.error(f"Error syncing product {product.default_code}: {str(e)}")

            # Log exception to integration dashboard
            self.env["square.integration.log"].log_error(
                title=f"Critical stock sync error for {product.name}",
                error_message=f"Exception during stock synchronization: {str(e)}",
                technical_details=f"Produit: {product.name}, SKU: {product.default_code}, Exception: {str(e)}",
            )

            self.total_syncs += 1
            self.failed_syncs += 1

    def sync_product_stock_for_location(self, product, square_location_id, quantity):
        """Sync stock for a specific product to a specific Square location"""
        if not self._should_sync_product(product):
            return True

        try:
            # Search for the product in Square catalog to get the catalog object ID
            search_result = self._search_square_catalog_by_sku(product.default_code)

            if not search_result["success"]:
                _logger.error(f"Failed to find Square catalog for SKU {product.default_code}")
                return False

            if search_result["not_found"]:
                _logger.warning(
                    f"Product with SKU {product.default_code} not found in Square catalog - skipping sync"
                )
                return True  # Return True to avoid retry, product doesn't exist in Square

            catalog_object_id = search_result["catalog_object_id"]

            _logger.info(
                f"Syncing {product.default_code} to Square location {square_location_id}: {quantity} units"
            )

            # Simply set the exact quantity from Odoo - no adjustments needed
            api_client = self._get_square_api_client()
            result = api_client.set_physical_count(
                catalog_object_id, square_location_id, int(quantity), product.default_code
            )

            if result:
                _logger.info(
                    f"Stock sync successful: Product {product.name} "
                    f"in location {square_location_id} -> {quantity} units"
                )

                # Log to integration dashboard
                self.env["square.integration.log"].log_square_event(
                    event_type="stock_sync",
                    title=f"Stock synchronized for {product.name}",
                    description=f"""
                        <p><strong>Square Stock Synchronization Successful</strong></p>
                        <ul>
                            <li>Product: <strong>{product.name}</strong></li>
                            <li>SKU : <code>{product.default_code}</code></li>
                            <li>Synchronized quantity: {quantity}</li>
                            <li>Square Location: {square_location_id}</li>
                        </ul>
                    """,
                    status="success",
                )

                # Update sync statistics
                self.total_syncs += 1
                self.successful_syncs += 1
                self.last_sync_date = fields.Datetime.now()

                return True
            else:
                _logger.error(f"Stock sync failed for {product.name}: API call returned False")

                # Log error to integration dashboard
                self.env["square.integration.log"].log_error(
                    title=f"Stock sync failed for {product.name}",
                    error_message=f"Unable to synchronize stock for product {product.name} vers Square",
                    technical_details=f"Produit: {product.name}, SKU: {product.default_code}, Quantité: {quantity}, Emplacement: {square_location_id}",
                )

                self.total_syncs += 1
                self.failed_syncs += 1
                self.last_sync_date = fields.Datetime.now()

                return False

        except Exception as e:
            _logger.error(f"Error syncing product {product.default_code}: {str(e)}")

            # Log exception to integration dashboard
            self.env["square.integration.log"].log_error(
                title=f"Critical stock sync error for {product.name}",
                error_message=f"Exception during stock synchronization: {str(e)}",
                technical_details=f"Produit: {product.name}, SKU: {product.default_code}, Exception: {str(e)}",
            )

            self.total_syncs += 1
            self.failed_syncs += 1
            self.last_sync_date = fields.Datetime.now()

            return False

    @api.model
    def sync_stock_changes(self, stock_move):
        """Main method to handle stock move changes"""
        if not self.auto_sync_enabled:
            _logger.debug("Auto sync disabled - skipping stock sync")
            return

        # Skip if Square-sourced move
        if self._is_square_sourced_move(stock_move):
            _logger.info(f"Skipping Square-sourced move: {stock_move.name}")
            return

        # Skip if move is not done
        if stock_move.state != "done":
            _logger.debug(f"Skipping non-done move: {stock_move.name}")
            return

        # Determine which warehouse this move affects
        affected_warehouse = self._get_affected_warehouse(stock_move)
        if not affected_warehouse:
            _logger.debug(f"No mapped warehouse affected by move: {stock_move.name}")
            return

        # Find the corresponding location mapping
        square_location_id = self._get_square_location_for_warehouse(affected_warehouse)
        if not square_location_id:
            _logger.debug(f"No Square location mapping found for warehouse {affected_warehouse.name}")
            return

        _logger.info(f"Processing stock move for Square sync: {stock_move.name} (warehouse: {affected_warehouse.name}, location: {square_location_id})")

        # Get current stock quantity for this warehouse
        stock_quant = self.env["stock.quant"].search(
            [
                ("product_id", "=", stock_move.product_id.id),
                ("location_id", "child_of", affected_warehouse.lot_stock_id.id),
            ]
        )
        total_quantity = sum(quant.quantity for quant in stock_quant)

        # Sync affected product to the specific location
        success = self.sync_product_stock_to_location(stock_move.product_id, affected_warehouse, square_location_id, total_quantity)

        # Log the stock move trigger event
        if success:
            _logger.info(
                f"Successfully synced {stock_move.product_id.default_code} to Square location {square_location_id}"
            )
            self.env["square.integration.log"].log_square_event(
                event_type="stock_sync",
                title=f"Stock synchronisé suite au mouvement {stock_move.name}",
                description=f"""
                    <h4>Stock Synchronization Triggered by Movement</h4>
                    <ul>
                        <li><strong>Stock movement:</strong> {stock_move.name}</li>
                        <li><strong>Product:</strong> {stock_move.product_id.name}</li>
                        <li><strong>SKU :</strong> <code>{stock_move.product_id.default_code}</code></li>
                        <li><strong>Moved quantity:</strong> {stock_move.quantity}</li>
                        <li><strong>From:</strong> {stock_move.location_id.name}</li>
                        <li><strong>To:</strong> {stock_move.location_dest_id.name}</li>
                        <li><strong>Affected warehouse:</strong> {affected_warehouse.name}</li>
                        <li><strong>Square Location:</strong> {square_location_id}</li>
                    </ul>
                    <p><em>Synchronisation déclenchée automatiquement par mouvement de stock</em></p>
                """,
                status="success",
            )
        else:
            _logger.error(
                f"Failed to sync {stock_move.product_id.default_code} to Square location {square_location_id}"
            )

    def _get_affected_warehouse(self, stock_move):
        """Determine which warehouse is affected by a stock move"""
        # Check if the move involves any of our mapped warehouses
        location_mappings = self._get_location_mappings()

        for mapping in location_mappings:
            warehouse_locations = (
                mapping.warehouse_id.lot_stock_id | mapping.warehouse_id.view_location_id
            )

            # Check if source or destination location is in this warehouse
            source_is_warehouse = stock_move.location_id in warehouse_locations
            dest_is_warehouse = stock_move.location_dest_id in warehouse_locations

            if source_is_warehouse or dest_is_warehouse:
                return mapping.warehouse_id

        return None

    def _get_square_location_for_warehouse(self, warehouse):
        """Get the Square location ID mapped to a specific warehouse"""
        location_mappings = self._get_location_mappings()

        for mapping in location_mappings:
            if mapping.warehouse_id == warehouse:
                return mapping.square_location_id

        return None

    def _get_location_mappings(self):
        """Get all configured location mappings from Square config"""
        api_client = self._get_square_api_client()
        config = api_client._get_square_config()
        if config and config.location_mapping_ids:
            return config.location_mapping_ids
        return self.env["square.location.mapping"]

    def manual_sync_all_products(self):
        """Manually sync all products to Square for all configured location mappings"""
        _logger.info("Starting manual sync of all products to Square")

        # Get all location mappings
        location_mappings = self._get_location_mappings()

        if not location_mappings:
            _logger.warning("No location mappings configured - falling back to legacy sync")
            # Fallback to legacy behavior
            configured_warehouse = self._get_configured_warehouse()
            _logger.info(f"Using fallback warehouse: {configured_warehouse.name}")

            return self._sync_products_for_warehouse(configured_warehouse, None)

        _logger.info(f"Found {len(location_mappings)} location mappings to sync")

        total_successful = 0
        total_failed = 0
        total_products = 0
        location_results = []

        # Sync each warehouse to its corresponding Square location
        for mapping in location_mappings:
            _logger.info(f"Syncing warehouse '{mapping.warehouse_id.name}' to Square location '{mapping.square_location_name}'")

            result = self._sync_products_for_warehouse(mapping.warehouse_id, mapping.square_location_id)
            location_results.append({
                'warehouse': mapping.warehouse_id.name,
                'square_location': mapping.square_location_name,
                'successful': result['successful'],
                'failed': result['failed'],
                'total': result['total']
            })

            total_successful += result['successful']
            total_failed += result['failed']
            total_products += result['total']

        _logger.info(f"Manual sync completed: {total_successful} successful, {total_failed} failed across {len(location_mappings)} locations")

        # Log manual sync completion to integration dashboard
        location_summary = ""
        for loc_result in location_results:
            location_summary += f"""
                <li><strong>{loc_result['warehouse']}</strong> → <em>{loc_result['square_location']}</em>
                    ({loc_result['successful']} réussis, {loc_result['failed']} échecs)</li>"""

        self.env["square.integration.log"].log_square_event(
            event_type="stock_sync",
            title=f"Synchronisation manuelle multi-entrepôts terminée",
            description=f"""
                <h4>Synchronisation Manuelle Stock Square Terminée</h4>
                <ul>
                    <li><strong>Emplacements synchronisés :</strong> {len(location_mappings)}</li>
                    <li><strong>Total produits traités :</strong> {total_products}</li>
                    <li><strong>Synchronisations réussies :</strong> {total_successful}</li>
                    <li><strong>Échecs :</strong> {total_failed}</li>
                </ul>
                <h5>Détail par emplacement :</h5>
                <ul>
                    {location_summary}
                </ul>
                <p><em>Synchronisation manuelle multi-entrepôts lancée depuis l'interface</em></p>
            """,
            status=(
                "success" if total_failed == 0 else "warning" if total_successful > 0 else "error"
            ),
        )

        return {"successful": total_successful, "failed": total_failed, "total": total_products, "locations": location_results}

    def _sync_products_for_warehouse(self, warehouse, square_location_id=None):
        """Sync all products for a specific warehouse to Square"""
        # Get all products with stock in the specified warehouse
        stock_quants = self.env["stock.quant"].search(
            [
                ("location_id", "child_of", warehouse.lot_stock_id.id),
                ("quantity", ">", 0),
            ]
        )

        _logger.info(
            f"Found {len(stock_quants)} stock quants in warehouse '{warehouse.name}' to sync. Only syncing products with default_code."
        )

        products = stock_quants.mapped("product_id").filtered(lambda p: p.default_code)

        _logger.info(f"Found {len(products)} products in warehouse '{warehouse.name}' to sync")

        successful = 0
        failed = 0

        for product in products:
            # Get current stock quantity for this warehouse
            stock_quant = self.env["stock.quant"].search(
                [
                    ("product_id", "=", product.id),
                    ("location_id", "child_of", warehouse.lot_stock_id.id),
                ]
            )
            total_quantity = sum(quant.quantity for quant in stock_quant)

            if square_location_id:
                # Sync to specific Square location
                if self.sync_product_stock_to_location(product, warehouse, square_location_id, total_quantity):
                    successful += 1
                else:
                    failed += 1
            else:
                # Fallback to legacy sync method
                if self.sync_product_stock(product):
                    successful += 1
                else:
                    failed += 1

        _logger.info(f"Warehouse '{warehouse.name}' sync completed: {successful} successful, {failed} failed")

        return {"successful": successful, "failed": failed, "total": len(products)}

    def sync_product_stock_to_location(self, product, warehouse, square_location_id, quantity):
        """Sync a specific product's stock to a specific Square location"""
        if not self._should_sync_product(product):
            return True

        try:
            # Search for the product in Square catalog to get the catalog object ID
            search_result = self._search_square_catalog_by_sku(product.default_code)

            if not search_result["success"]:
                _logger.error(f"Failed to find Square catalog for SKU {product.default_code}")
                return False

            if search_result["not_found"]:
                _logger.warning(
                    f"Product with SKU {product.default_code} not found in Square catalog - skipping sync"
                )
                return True  # Return True to avoid retry, product doesn't exist in Square

            catalog_object_id = search_result["catalog_object_id"]

            _logger.info(
                f"Syncing {product.default_code} from warehouse '{warehouse.name}' to Square location {square_location_id}: {quantity} units"
            )

            # Simply set the exact quantity from Odoo - no adjustments needed
            api_client = self._get_square_api_client()
            result = api_client.set_physical_count(
                catalog_object_id, square_location_id, int(quantity), product.default_code
            )

            if result:
                _logger.info(
                    f"Stock sync successful: Product {product.name} "
                    f"in warehouse '{warehouse.name}' -> Square location {square_location_id} -> {quantity} units"
                )

                # Log to integration dashboard
                self.env["square.integration.log"].log_square_event(
                    event_type="stock_sync",
                    title=f"Stock synchronized for {product.name}",
                    description=f"""
                        <p><strong>Square Stock Synchronization Successful</strong></p>
                        <ul>
                            <li>Product: <strong>{product.name}</strong></li>
                            <li>SKU : <code>{product.default_code}</code></li>
                            <li>Synchronized quantity: {quantity}</li>
                            <li>Entrepôt Odoo : <strong>{warehouse.name}</strong></li>
                            <li>Square Location: {square_location_id}</li>
                        </ul>
                    """,
                    status="success",
                )

                # Update sync statistics
                self.total_syncs += 1
                self.successful_syncs += 1
                self.last_sync_date = fields.Datetime.now()

                return True
            else:
                _logger.error(f"Stock sync failed for {product.name} in location {square_location_id}: API call returned False")

                # Log error to integration dashboard
                self.env["square.integration.log"].log_error(
                    title=f"Stock sync failed for {product.name}",
                    error_message=f"Unable to synchronize stock for product {product.name} vers Square",
                    technical_details=f"Produit: {product.name}, SKU: {product.default_code}, Quantité: {quantity}, Entrepôt: {warehouse.name}, Emplacement: {square_location_id}",
                )

                self.total_syncs += 1
                self.failed_syncs += 1
                self.last_sync_date = fields.Datetime.now()

                return False

        except Exception as e:
            _logger.error(f"Error syncing product {product.default_code} to location {square_location_id}: {str(e)}")

            # Log exception to integration dashboard
            self.env["square.integration.log"].log_error(
                title=f"Critical stock sync error for {product.name}",
                error_message=f"Exception during stock synchronization: {str(e)}",
                technical_details=f"Produit: {product.name}, SKU: {product.default_code}, Quantité: {quantity}, Entrepôt: {warehouse.name}, Emplacement: {square_location_id}, Exception: {str(e)}",
            )

            self.total_syncs += 1
            self.failed_syncs += 1
            self.last_sync_date = fields.Datetime.now()

            return False

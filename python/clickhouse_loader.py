"""
ClickHouse Loader - Efficient batch loading to ClickHouse
Handles optimized inserts with proper error handling and retries
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import structlog

from clickhouse_driver import Client as ClickHouseClient
from clickhouse_driver.errors import Error as ClickHouseError
from tenacity import retry, stop_after_attempt, wait_exponential


logger = structlog.get_logger()


class ClickHouseLoader:
    """
    Optimized ClickHouse loader for CDC events.
    
    Features:
    - Batch inserts
    - Automatic retries
    - Connection pooling
    - Error handling
    """
    
    # Table schemas for validation
    TABLE_SCHEMAS = {
        'customers': [
            'customer_id', 'first_name', 'last_name', 'email', 'phone',
            'address', 'city', 'state', 'zip_code', 'country',
            'date_of_birth', 'customer_since', 'is_active',
            'created_at', 'updated_at',
            'cdc_operation', 'cdc_timestamp', 'cdc_source_db', 'cdc_source_table',
            'event_time'
        ],
        'vehicles': [
            'vehicle_id', 'vin', 'make', 'model', 'year', 'color',
            'mileage', 'transmission', 'fuel_type', 'body_type',
            'engine_size', 'price', 'status', 'condition',
            'description', 'features', 'listed_date',
            'created_at', 'updated_at',
            'cdc_operation', 'cdc_timestamp', 'cdc_source_db', 'cdc_source_table',
            'event_time'
        ],
        'orders': [
            'order_id', 'customer_id', 'order_date',
            'total_amount', 'tax_amount', 'discount_amount', 'final_amount',
            'status', 'payment_status',
            'shipping_address', 'billing_address', 'notes',
            'created_at', 'updated_at',
            'cdc_operation', 'cdc_timestamp', 'cdc_source_db', 'cdc_source_table',
            'event_time'
        ],
        'order_items': [
            'order_item_id', 'order_id', 'vehicle_id',
            'quantity', 'unit_price', 'discount', 'subtotal',
            'warranty_plan', 'extended_warranty',
            'created_at', 'updated_at',
            'cdc_operation', 'cdc_timestamp', 'cdc_source_db', 'cdc_source_table',
            'event_time'
        ],
        'payments': [
            'payment_id', 'order_id', 'payment_date',
            'payment_method', 'amount', 'transaction_id',
            'payment_status', 'payment_provider',
            'card_last_four', 'authorization_code',
            'created_at', 'updated_at',
            'cdc_operation', 'cdc_timestamp', 'cdc_source_db', 'cdc_source_table',
            'event_time'
        ]
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ClickHouse loader.
        
        Args:
            config: ClickHouse connection configuration
        """
        self.config = config
        self.client = self._create_client()
        self.stats = {
            'inserts': 0,
            'errors': 0,
            'batches': 0
        }
        
        logger.info(
            "clickhouse_loader_initialized",
            host=config['host'],
            database=config['database']
        )
    
    def _create_client(self) -> ClickHouseClient:
        """Create ClickHouse client connection."""
        try:
            client = ClickHouseClient(
                host=self.config['host'],
                port=self.config.get('port', 9000),
                database=self.config['database'],
                user=self.config.get('user', 'default'),
                password=self.config.get('password', ''),
                settings={
                    'max_insert_block_size': 100000,
                    'insert_quorum': 1,
                }
            )
            
            # Test connection
            client.execute('SELECT 1')
            logger.info("clickhouse_connection_established")
            
            return client
            
        except ClickHouseError as e:
            logger.error("clickhouse_connection_failed", error=str(e))
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def load_batch(self, table: str, records: List[Dict[str, Any]]) -> bool:
        """
        Load a batch of records into ClickHouse.
        
        Args:
            table: Target table name
            records: List of records to insert
            
        Returns:
            True if successful, False otherwise
        """
        if not records:
            return True
        
        if table not in self.TABLE_SCHEMAS:
            logger.error("unknown_table", table=table)
            return False
        
        try:
            # Prepare data for batch insert
            columns = self.TABLE_SCHEMAS[table]
            data_to_insert = []
            
            for record in records:
                # Convert record to tuple matching column order
                row = []
                for col in columns:
                    value = record.get(col)
                    # Handle None/NULL values
                    if value is None:
                        row.append(None)
                    else:
                        row.append(value)
                data_to_insert.append(tuple(row))
            
            # Execute batch insert
            query = f"INSERT INTO {self.config['database']}.{table} ({', '.join(columns)}) VALUES"
            
            result = self.client.execute(query, data_to_insert)
            
            self.stats['inserts'] += len(records)
            self.stats['batches'] += 1
            
            logger.info(
                "batch_inserted",
                table=table,
                records=len(records),
                total_inserts=self.stats['inserts']
            )
            
            return True
            
        except ClickHouseError as e:
            logger.error(
                "batch_insert_failed",
                table=table,
                records=len(records),
                error=str(e)
            )
            self.stats['errors'] += 1
            raise
        except Exception as e:
            logger.error(
                "unexpected_error",
                table=table,
                error=str(e)
            )
            self.stats['errors'] += 1
            return False
    
    def execute_query(self, query: str) -> Optional[List]:
        """
        Execute a custom query.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results or None if error
        """
        try:
            result = self.client.execute(query)
            return result
        except ClickHouseError as e:
            logger.error("query_execution_failed", error=str(e), query=query)
            return None
    
    def get_table_count(self, table: str) -> Optional[int]:
        """
        Get row count for a table.
        
        Args:
            table: Table name
            
        Returns:
            Row count or None if error
        """
        query = f"SELECT count() FROM {self.config['database']}.{table}"
        result = self.execute_query(query)
        
        if result:
            return result[0][0]
        return None
    
    def optimize_table(self, table: str):
        """
        Run OPTIMIZE on a table to merge parts.
        
        Args:
            table: Table name
        """
        try:
            query = f"OPTIMIZE TABLE {self.config['database']}.{table} FINAL"
            self.client.execute(query)
            logger.info("table_optimized", table=table)
        except ClickHouseError as e:
            logger.error("optimize_failed", table=table, error=str(e))
    
    def get_stats(self) -> Dict[str, int]:
        """Get loader statistics."""
        return self.stats.copy()
    
    def close(self):
        """Close ClickHouse connection."""
        try:
            if self.client:
                self.client.disconnect()
                logger.info("clickhouse_connection_closed", stats=self.stats)
        except Exception as e:
            logger.error("connection_close_error", error=str(e))


class ClickHouseHealthCheck:
    """Health check utilities for ClickHouse."""
    
    @staticmethod
    def check_connection(config: Dict[str, Any]) -> bool:
        """
        Check if ClickHouse is reachable.
        
        Args:
            config: ClickHouse configuration
            
        Returns:
            True if healthy, False otherwise
        """
        try:
            client = ClickHouseClient(
                host=config['host'],
                port=config.get('port', 9000),
                database=config['database'],
                user=config.get('user', 'default'),
                password=config.get('password', '')
            )
            
            result = client.execute('SELECT 1')
            client.disconnect()
            
            return result == [(1,)]
            
        except Exception as e:
            logger.error("health_check_failed", error=str(e))
            return False
    
    @staticmethod
    def get_server_info(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get ClickHouse server information.
        
        Args:
            config: ClickHouse configuration
            
        Returns:
            Server info dictionary or None
        """
        try:
            client = ClickHouseClient(
                host=config['host'],
                port=config.get('port', 9000),
                database=config['database'],
                user=config.get('user', 'default'),
                password=config.get('password', '')
            )
            
            version = client.execute('SELECT version()')[0][0]
            uptime = client.execute('SELECT uptime()')[0][0]
            
            client.disconnect()
            
            return {
                'version': version,
                'uptime_seconds': uptime,
                'healthy': True
            }
            
        except Exception as e:
            logger.error("server_info_failed", error=str(e))
            return None

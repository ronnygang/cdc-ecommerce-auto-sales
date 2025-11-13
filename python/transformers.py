"""
CDC Transformers - Data transformation logic for CDC events
Handles transformation of Debezium CDC events to ClickHouse format
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional
import structlog

logger = structlog.get_logger()


class CDCTransformer:
    """
    Transform Debezium CDC events into ClickHouse-compatible format.
    
    Handles:
    - INSERT, UPDATE, DELETE operations
    - Data type conversions
    - Metadata extraction
    - NULL handling
    """
    
    # Table name mapping
    TABLE_MAPPING = {
        'customers': 'customers',
        'vehicles': 'vehicles',
        'orders': 'orders',
        'order_items': 'order_items',
        'payments': 'payments'
    }
    
    def __init__(self):
        """Initialize transformer."""
        self.stats = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'errors': 0
        }
    
    def transform(self, cdc_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform a CDC event into ClickHouse format.
        
        Args:
            cdc_event: Raw CDC event from Kafka
            
        Returns:
            Transformed data or None if should be skipped
        """
        try:
            payload = cdc_event['payload']
            
            # Extract table name from topic
            topic = cdc_event['topic']
            table_name = self._extract_table_name(topic)
            
            if not table_name:
                logger.warning("unknown_table", topic=topic)
                return None
            
            # Get operation type
            operation = payload.get('op', payload.get('__op'))
            
            if not operation:
                logger.warning("missing_operation", topic=topic)
                return None
            
            # Transform based on operation
            if operation == 'c' or operation == 'r':  # create or read (snapshot)
                self.stats['inserts'] += 1
                return self._transform_insert(table_name, payload, cdc_event)
            elif operation == 'u':  # update
                self.stats['updates'] += 1
                return self._transform_update(table_name, payload, cdc_event)
            elif operation == 'd':  # delete
                self.stats['deletes'] += 1
                return self._transform_delete(table_name, payload, cdc_event)
            else:
                logger.warning("unknown_operation", operation=operation, topic=topic)
                return None
                
        except Exception as e:
            logger.error("transformation_error", error=str(e), event=cdc_event)
            self.stats['errors'] += 1
            return None
    
    def _extract_table_name(self, topic: str) -> Optional[str]:
        """Extract table name from Kafka topic."""
        # Topic format: cdc.ecommerce.ecommerce_postgres.public.{table_name}
        parts = topic.split('.')
        if len(parts) >= 5:
            table = parts[-1]
            return self.TABLE_MAPPING.get(table)
        return None
    
    def _transform_insert(
        self, 
        table: str, 
        payload: Dict[str, Any],
        cdc_event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform INSERT/CREATE operation."""
        # Get the after state (new record)
        after_data = payload.get('after', payload)
        
        # Add CDC metadata
        result = self._add_cdc_metadata(after_data, payload, cdc_event)
        result['table'] = table
        result['cdc_operation'] = 'INSERT'
        
        return result
    
    def _transform_update(
        self, 
        table: str, 
        payload: Dict[str, Any],
        cdc_event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform UPDATE operation."""
        # Get the after state (updated record)
        after_data = payload.get('after', {})
        
        if not after_data:
            logger.warning("missing_after_data", table=table)
            return None
        
        # Add CDC metadata
        result = self._add_cdc_metadata(after_data, payload, cdc_event)
        result['table'] = table
        result['cdc_operation'] = 'UPDATE'
        
        return result
    
    def _transform_delete(
        self, 
        table: str, 
        payload: Dict[str, Any],
        cdc_event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform DELETE operation."""
        # Get the before state (deleted record)
        before_data = payload.get('before', {})
        
        if not before_data:
            logger.warning("missing_before_data", table=table)
            return None
        
        # Add CDC metadata
        result = self._add_cdc_metadata(before_data, payload, cdc_event)
        result['table'] = table
        result['cdc_operation'] = 'DELETE'
        
        # For deletes, we might want to mark as deleted rather than remove
        # ClickHouse ReplacingMergeTree will handle this appropriately
        
        return result
    
    def _add_cdc_metadata(
        self,
        data: Dict[str, Any],
        payload: Dict[str, Any],
        cdc_event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Add CDC metadata to the record."""
        result = data.copy()
        
        # Extract source metadata
        source = payload.get('source', {})
        
        # Add CDC fields
        result['cdc_timestamp'] = self._convert_timestamp(
            payload.get('ts_ms') or payload.get('source_ts_ms')
        )
        result['cdc_source_db'] = source.get('db', 'ecommerce_db')
        result['cdc_source_table'] = source.get('table', '')
        
        # Add event time for processing tracking
        result['event_time'] = datetime.now()
        
        return result
    
    def _convert_timestamp(self, ts_ms: Optional[int]) -> datetime:
        """
        Convert millisecond timestamp to datetime.
        
        Args:
            ts_ms: Timestamp in milliseconds
            
        Returns:
            datetime object
        """
        if ts_ms:
            return datetime.fromtimestamp(ts_ms / 1000.0)
        return datetime.now()
    
    def _convert_json_field(self, value: Any) -> Optional[str]:
        """Convert JSON/JSONB fields to string."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)
    
    def get_stats(self) -> Dict[str, int]:
        """Get transformation statistics."""
        return self.stats.copy()


class CustomerTransformer:
    """Specialized transformer for customer records."""
    
    @staticmethod
    def transform_customer(data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply customer-specific transformations."""
        result = data.copy()
        
        # Convert boolean to int for ClickHouse
        if 'is_active' in result:
            result['is_active'] = 1 if result['is_active'] else 0
        
        # Ensure date fields are properly formatted
        if 'date_of_birth' in result and result['date_of_birth']:
            # Handle date conversion if needed
            pass
        
        return result


class VehicleTransformer:
    """Specialized transformer for vehicle records."""
    
    @staticmethod
    def transform_vehicle(data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply vehicle-specific transformations."""
        result = data.copy()
        
        # Convert JSONB features to JSON string
        if 'features' in result and result['features']:
            if isinstance(result['features'], (dict, list)):
                result['features'] = json.dumps(result['features'])
        
        return result


class OrderTransformer:
    """Specialized transformer for order records."""
    
    @staticmethod
    def transform_order(data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply order-specific transformations."""
        result = data.copy()
        
        # Ensure numeric fields are properly typed
        numeric_fields = ['total_amount', 'tax_amount', 'discount_amount', 'final_amount']
        for field in numeric_fields:
            if field in result and result[field] is not None:
                result[field] = float(result[field])
        
        return result

"""
CDC Consumer - Kafka to ClickHouse Pipeline
Real-time Change Data Capture processor for Ecommerce Auto Sales

This module consumes CDC events from Kafka topics and loads them into ClickHouse
with proper transformations and error handling.
"""

import json
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, Any, Optional, List

from confluent_kafka import Consumer, KafkaError, KafkaException
from clickhouse_driver import Client as ClickHouseClient
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

from transformers import CDCTransformer
from clickhouse_loader import ClickHouseLoader


# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()


class CDCConsumer:
    """
    Kafka CDC Consumer that processes events and loads them into ClickHouse.
    
    Handles:
    - Multiple table events
    - INSERT, UPDATE, DELETE operations
    - Batch processing
    - Error recovery
    - Graceful shutdown
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CDC Consumer.
        
        Args:
            config: Configuration dictionary with Kafka and ClickHouse settings
        """
        self.config = config
        self.running = False
        self.transformer = CDCTransformer()
        self.loader = ClickHouseLoader(config['clickhouse'])
        
        # Kafka consumer configuration
        consumer_config = {
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': config['kafka']['group_id'],
            'auto.offset.reset': config['kafka'].get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 60000,
        }
        
        self.consumer = Consumer(consumer_config)
        
        # Subscribe to CDC topics
        topics = config['kafka']['topics']
        self.consumer.subscribe(topics)
        logger.info("consumer_initialized", topics=topics)
        
        # Batch configuration
        self.batch_size = config.get('batch_size', 100)
        self.batch_timeout_ms = config.get('batch_timeout_ms', 5000)
        self.message_batch = []
        
        # Metrics
        self.metrics = {
            'messages_processed': 0,
            'messages_failed': 0,
            'batches_committed': 0,
            'last_commit_time': None
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _process_message(self, msg) -> Optional[Dict[str, Any]]:
        """
        Process a single Kafka message.
        
        Args:
            msg: Kafka message
            
        Returns:
            Transformed data ready for ClickHouse or None if error
        """
        try:
            # Parse JSON payload
            value = msg.value().decode('utf-8')
            data = json.loads(value)
            
            # Extract CDC metadata
            cdc_event = {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp()[1] if msg.timestamp()[0] == 1 else None,
                'key': msg.key().decode('utf-8') if msg.key() else None,
                'payload': data
            }
            
            # Transform the event
            transformed_data = self.transformer.transform(cdc_event)
            
            if transformed_data:
                logger.debug(
                    "message_processed",
                    topic=msg.topic(),
                    offset=msg.offset(),
                    operation=transformed_data.get('cdc_operation')
                )
                return transformed_data
            else:
                logger.warning("message_skipped", topic=msg.topic(), offset=msg.offset())
                return None
                
        except json.JSONDecodeError as e:
            logger.error("json_decode_error", error=str(e), offset=msg.offset())
            self.metrics['messages_failed'] += 1
            return None
        except Exception as e:
            logger.error("message_processing_error", error=str(e), offset=msg.offset())
            self.metrics['messages_failed'] += 1
            raise
    
    def _process_batch(self):
        """Process accumulated message batch and load to ClickHouse."""
        if not self.message_batch:
            return
        
        try:
            # Group messages by table
            table_batches = {}
            for msg_data in self.message_batch:
                table = msg_data.get('table')
                if table:
                    if table not in table_batches:
                        table_batches[table] = []
                    table_batches[table].append(msg_data)
            
            # Load each table batch to ClickHouse
            for table, records in table_batches.items():
                success = self.loader.load_batch(table, records)
                if success:
                    logger.info(
                        "batch_loaded",
                        table=table,
                        records=len(records)
                    )
                else:
                    logger.error("batch_load_failed", table=table)
                    self.metrics['messages_failed'] += len(records)
            
            # Commit offsets
            self.consumer.commit(asynchronous=False)
            self.metrics['batches_committed'] += 1
            self.metrics['last_commit_time'] = datetime.now().isoformat()
            
            logger.info(
                "batch_committed",
                batch_size=len(self.message_batch),
                batches_committed=self.metrics['batches_committed']
            )
            
        except Exception as e:
            logger.error("batch_processing_error", error=str(e))
            raise
        finally:
            # Clear the batch
            self.message_batch.clear()
    
    def run(self):
        """
        Main consumer loop.
        
        Continuously polls Kafka for messages, processes them in batches,
        and loads them into ClickHouse.
        """
        self.running = True
        logger.info("consumer_started")
        
        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message, check if batch timeout reached
                    if self.message_batch:
                        self._process_batch()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("partition_eof", partition=msg.partition())
                    else:
                        logger.error("kafka_error", error=msg.error())
                    continue
                
                # Process message
                transformed_data = self._process_message(msg)
                
                if transformed_data:
                    self.message_batch.append(transformed_data)
                    self.metrics['messages_processed'] += 1
                
                # Check if batch is full
                if len(self.message_batch) >= self.batch_size:
                    self._process_batch()
            
            # Process remaining messages on shutdown
            if self.message_batch:
                logger.info("processing_remaining_batch")
                self._process_batch()
                
        except KafkaException as e:
            logger.error("kafka_exception", error=str(e))
            raise
        except Exception as e:
            logger.error("consumer_error", error=str(e))
            raise
        finally:
            logger.info("consumer_shutdown", metrics=self.metrics)
            self.consumer.close()
            self.loader.close()


def main():
    """Main entry point for CDC consumer."""
    
    # Configuration
    config = {
        'kafka': {
            'bootstrap_servers': 'localhost:29092',
            'group_id': 'cdc-clickhouse-consumer',
            'topics': [
                'cdc.ecommerce.ecommerce_postgres.public.customers',
                'cdc.ecommerce.ecommerce_postgres.public.vehicles',
                'cdc.ecommerce.ecommerce_postgres.public.orders',
                'cdc.ecommerce.ecommerce_postgres.public.order_items',
                'cdc.ecommerce.ecommerce_postgres.public.payments'
            ],
            'auto_offset_reset': 'earliest'
        },
        'clickhouse': {
            'host': 'localhost',
            'port': 9000,
            'database': 'ecommerce_analytics',
            'user': 'default',
            'password': 'clickhouse123'
        },
        'batch_size': 100,
        'batch_timeout_ms': 5000
    }
    
    # Initialize and run consumer
    consumer = CDCConsumer(config)
    
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
    except Exception as e:
        logger.error("fatal_error", error=str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()

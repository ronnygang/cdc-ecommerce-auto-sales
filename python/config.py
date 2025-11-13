"""
Configuration module for CDC Pipeline
Centralized configuration management
"""

import os
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str = 'localhost:29092'
    group_id: str = 'cdc-clickhouse-consumer'
    auto_offset_reset: str = 'earliest'
    topics: list = None
    
    def __post_init__(self):
        if self.topics is None:
            self.topics = [
                'cdc.ecommerce.ecommerce_postgres.public.customers',
                'cdc.ecommerce.ecommerce_postgres.public.vehicles',
                'cdc.ecommerce.ecommerce_postgres.public.orders',
                'cdc.ecommerce.ecommerce_postgres.public.order_items',
                'cdc.ecommerce.ecommerce_postgres.public.payments'
            ]


@dataclass
class ClickHouseConfig:
    """ClickHouse configuration."""
    host: str = 'localhost'
    port: int = 9000
    database: str = 'ecommerce_analytics'
    user: str = 'default'
    password: str = 'clickhouse123'


@dataclass
class PostgresConfig:
    """PostgreSQL configuration."""
    host: str = 'localhost'
    port: int = 5432
    database: str = 'ecommerce_db'
    user: str = 'postgres'
    password: str = 'postgres123'


@dataclass
class ConsumerConfig:
    """Consumer configuration."""
    batch_size: int = 100
    batch_timeout_ms: int = 5000
    log_level: str = 'INFO'


def get_config() -> Dict[str, Any]:
    """
    Get complete configuration from environment variables or defaults.
    
    Returns:
        Configuration dictionary
    """
    kafka_config = KafkaConfig(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
        group_id=os.getenv('KAFKA_GROUP_ID', 'cdc-clickhouse-consumer')
    )
    
    clickhouse_config = ClickHouseConfig(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
        database=os.getenv('CLICKHOUSE_DATABASE', 'ecommerce_analytics'),
        user=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse123')
    )
    
    postgres_config = PostgresConfig(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        database=os.getenv('POSTGRES_DATABASE', 'ecommerce_db'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres123')
    )
    
    consumer_config = ConsumerConfig(
        batch_size=int(os.getenv('BATCH_SIZE', '100')),
        batch_timeout_ms=int(os.getenv('BATCH_TIMEOUT_MS', '5000')),
        log_level=os.getenv('LOG_LEVEL', 'INFO')
    )
    
    return {
        'kafka': {
            'bootstrap_servers': kafka_config.bootstrap_servers,
            'group_id': kafka_config.group_id,
            'topics': kafka_config.topics,
            'auto_offset_reset': kafka_config.auto_offset_reset
        },
        'clickhouse': {
            'host': clickhouse_config.host,
            'port': clickhouse_config.port,
            'database': clickhouse_config.database,
            'user': clickhouse_config.user,
            'password': clickhouse_config.password
        },
        'postgres': {
            'host': postgres_config.host,
            'port': postgres_config.port,
            'database': postgres_config.database,
            'user': postgres_config.user,
            'password': postgres_config.password
        },
        'batch_size': consumer_config.batch_size,
        'batch_timeout_ms': consumer_config.batch_timeout_ms,
        'log_level': consumer_config.log_level
    }

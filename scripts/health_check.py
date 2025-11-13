"""
Health Check Script - CDC Pipeline
Validates all components of the CDC system
"""

import sys
import requests
import psycopg2
from clickhouse_driver import Client as ClickHouseClient
from confluent_kafka.admin import AdminClient
from typing import Dict, Any
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()


class HealthChecker:
    """Comprehensive health check for CDC pipeline components."""
    
    def __init__(self):
        """Initialize health checker."""
        self.results = {
            'postgres': False,
            'kafka': False,
            'kafka_connect': False,
            'clickhouse': False,
            'debezium_connector': False
        }
    
    def check_postgres(self) -> bool:
        """Check PostgreSQL health."""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='ecommerce_db',
                user='postgres',
                password='postgres123'
            )
            
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            result = cursor.fetchone()
            
            # Check tables
            cursor.execute("""
                SELECT count(*) FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            table_count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            logger.info("postgres_healthy", tables=table_count)
            return True
            
        except Exception as e:
            logger.error("postgres_unhealthy", error=str(e))
            return False
    
    def check_kafka(self) -> bool:
        """Check Kafka broker health."""
        try:
            admin_client = AdminClient({
                'bootstrap.servers': 'localhost:29092'
            })
            
            metadata = admin_client.list_topics(timeout=10)
            topic_count = len(metadata.topics)
            
            logger.info("kafka_healthy", topics=topic_count)
            return True
            
        except Exception as e:
            logger.error("kafka_unhealthy", error=str(e))
            return False
    
    def check_kafka_connect(self) -> bool:
        """Check Kafka Connect health."""
        try:
            response = requests.get('http://localhost:8083/', timeout=5)
            
            if response.status_code == 200:
                logger.info("kafka_connect_healthy")
                return True
            else:
                logger.error("kafka_connect_unhealthy", status=response.status_code)
                return False
                
        except Exception as e:
            logger.error("kafka_connect_unreachable", error=str(e))
            return False
    
    def check_debezium_connector(self) -> bool:
        """Check Debezium connector status."""
        try:
            connector_name = 'postgres-ecommerce-connector'
            response = requests.get(
                f'http://localhost:8083/connectors/{connector_name}/status',
                timeout=5
            )
            
            if response.status_code == 200:
                status = response.json()
                state = status.get('connector', {}).get('state', 'UNKNOWN')
                
                if state == 'RUNNING':
                    logger.info("debezium_connector_running", connector=connector_name)
                    return True
                else:
                    logger.warning("debezium_connector_not_running", state=state)
                    return False
            else:
                logger.error("debezium_connector_not_found", connector=connector_name)
                return False
                
        except Exception as e:
            logger.error("debezium_connector_check_failed", error=str(e))
            return False
    
    def check_clickhouse(self) -> bool:
        """Check ClickHouse health."""
        try:
            client = ClickHouseClient(
                host='localhost',
                port=9000,
                database='ecommerce_analytics',
                user='default',
                password='clickhouse123'
            )
            
            # Check connection
            result = client.execute('SELECT 1')
            
            # Check tables
            tables = client.execute('SHOW TABLES FROM ecommerce_analytics')
            table_count = len(tables)
            
            # Check row counts
            customers_count = client.execute('SELECT count() FROM ecommerce_analytics.customers')[0][0]
            vehicles_count = client.execute('SELECT count() FROM ecommerce_analytics.vehicles')[0][0]
            
            client.disconnect()
            
            logger.info(
                "clickhouse_healthy",
                tables=table_count,
                customers=customers_count,
                vehicles=vehicles_count
            )
            return True
            
        except Exception as e:
            logger.error("clickhouse_unhealthy", error=str(e))
            return False
    
    def run_all_checks(self) -> Dict[str, bool]:
        """Run all health checks."""
        print("\n" + "="*60)
        print("🏥 CDC Pipeline Health Check")
        print("="*60 + "\n")
        
        checks = [
            ('PostgreSQL (Source)', self.check_postgres, 'postgres'),
            ('Kafka Broker', self.check_kafka, 'kafka'),
            ('Kafka Connect', self.check_kafka_connect, 'kafka_connect'),
            ('Debezium Connector', self.check_debezium_connector, 'debezium_connector'),
            ('ClickHouse (Target)', self.check_clickhouse, 'clickhouse')
        ]
        
        for name, check_func, key in checks:
            print(f"Checking {name}...", end=' ')
            result = check_func()
            self.results[key] = result
            
            if result:
                print("✅ HEALTHY")
            else:
                print("❌ UNHEALTHY")
        
        print("\n" + "="*60)
        print("📊 Health Check Summary")
        print("="*60)
        
        healthy_count = sum(1 for v in self.results.values() if v)
        total_count = len(self.results)
        
        print(f"\nHealthy Components: {healthy_count}/{total_count}")
        
        if healthy_count == total_count:
            print("\n✅ All systems operational!")
            return self.results
        else:
            print("\n⚠️  Some systems require attention")
            print("\nFailed components:")
            for component, status in self.results.items():
                if not status:
                    print(f"  • {component}")
            return self.results
    
    def get_detailed_status(self) -> Dict[str, Any]:
        """Get detailed status of all components."""
        status = {
            'overall_health': all(self.results.values()),
            'components': self.results,
            'timestamp': str(structlog.processors.TimeStamper(fmt="iso"))
        }
        
        # Add component-specific details
        try:
            # Kafka topics
            admin_client = AdminClient({'bootstrap.servers': 'localhost:29092'})
            metadata = admin_client.list_topics(timeout=5)
            status['kafka_topics'] = list(metadata.topics.keys())
        except:
            pass
        
        try:
            # ClickHouse stats
            client = ClickHouseClient(
                host='localhost',
                port=9000,
                database='ecommerce_analytics',
                user='default',
                password='clickhouse123'
            )
            
            tables = ['customers', 'vehicles', 'orders', 'order_items', 'payments']
            row_counts = {}
            
            for table in tables:
                try:
                    count = client.execute(f'SELECT count() FROM ecommerce_analytics.{table}')[0][0]
                    row_counts[table] = count
                except:
                    row_counts[table] = 0
            
            status['clickhouse_row_counts'] = row_counts
            client.disconnect()
        except:
            pass
        
        return status


def main():
    """Main entry point."""
    checker = HealthChecker()
    results = checker.run_all_checks()
    
    # Exit with error if any check failed
    if not all(results.values()):
        sys.exit(1)
    
    # Get and display detailed status
    detailed_status = checker.get_detailed_status()
    
    print("\n" + "="*60)
    print("📈 Detailed Status")
    print("="*60)
    
    if 'kafka_topics' in detailed_status:
        print(f"\nKafka Topics: {len(detailed_status['kafka_topics'])}")
        for topic in detailed_status['kafka_topics']:
            if 'cdc.ecommerce' in topic:
                print(f"  • {topic}")
    
    if 'clickhouse_row_counts' in detailed_status:
        print("\nClickHouse Row Counts:")
        for table, count in detailed_status['clickhouse_row_counts'].items():
            print(f"  • {table}: {count:,} rows")
    
    print("\n" + "="*60)


if __name__ == '__main__':
    main()

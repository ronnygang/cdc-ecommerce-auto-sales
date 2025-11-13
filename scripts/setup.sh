#!/bin/bash

# ============================================================================
# Setup Script - CDC Pipeline
# Initializes Debezium connector and validates system health
# ============================================================================

set -e

echo "🚀 Starting CDC Pipeline Setup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
KAFKA_CONNECT_URL="http://localhost:8083"
CONNECTOR_CONFIG="../debezium/connectors/postgres-connector.json"
MAX_RETRIES=30
RETRY_INTERVAL=5

# ============================================================================
# Helper Functions
# ============================================================================

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

wait_for_service() {
    local url=$1
    local service_name=$2
    local retries=0
    
    print_info "Waiting for $service_name to be ready..."
    
    while [ $retries -lt $MAX_RETRIES ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            print_success "$service_name is ready"
            return 0
        fi
        
        retries=$((retries + 1))
        echo "Retry $retries/$MAX_RETRIES..."
        sleep $RETRY_INTERVAL
    done
    
    print_error "$service_name failed to start"
    return 1
}

# ============================================================================
# Health Checks
# ============================================================================

echo ""
echo "📋 Checking service health..."

# Check Kafka Connect
wait_for_service "$KAFKA_CONNECT_URL" "Kafka Connect"

# Check PostgreSQL
print_info "Checking PostgreSQL..."
if docker exec postgres-source pg_isready -U postgres > /dev/null 2>&1; then
    print_success "PostgreSQL is ready"
else
    print_error "PostgreSQL is not ready"
    exit 1
fi

# Check ClickHouse
print_info "Checking ClickHouse..."
if docker exec clickhouse-analytics clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
    print_success "ClickHouse is ready"
else
    print_error "ClickHouse is not ready"
    exit 1
fi

# ============================================================================
# Debezium Connector Setup
# ============================================================================

echo ""
echo "🔌 Setting up Debezium PostgreSQL Connector..."

# Check if connector already exists
CONNECTOR_NAME="postgres-ecommerce-connector"
if curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" | grep -q "name"; then
    print_info "Connector already exists. Deleting..."
    curl -X DELETE "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME"
    sleep 2
fi

# Create connector
print_info "Creating Debezium connector..."
RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    --data @"$CONNECTOR_CONFIG" \
    "$KAFKA_CONNECT_URL/connectors")

if echo "$RESPONSE" | grep -q "name"; then
    print_success "Debezium connector created successfully"
else
    print_error "Failed to create connector"
    echo "$RESPONSE"
    exit 1
fi

# Verify connector status
sleep 5
print_info "Checking connector status..."
STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status")

if echo "$STATUS" | grep -q '"state":"RUNNING"'; then
    print_success "Connector is running"
else
    print_error "Connector is not running"
    echo "$STATUS"
    exit 1
fi

# ============================================================================
# Kafka Topics Verification
# ============================================================================

echo ""
echo "📊 Verifying Kafka topics..."

sleep 10  # Wait for topics to be created

EXPECTED_TOPICS=(
    "cdc.ecommerce.ecommerce_postgres.public.customers"
    "cdc.ecommerce.ecommerce_postgres.public.vehicles"
    "cdc.ecommerce.ecommerce_postgres.public.orders"
    "cdc.ecommerce.ecommerce_postgres.public.order_items"
    "cdc.ecommerce.ecommerce_postgres.public.payments"
)

for topic in "${EXPECTED_TOPICS[@]}"; do
    if docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092 | grep -q "$topic"; then
        print_success "Topic exists: $topic"
    else
        print_error "Topic missing: $topic"
    fi
done

# ============================================================================
# Database Verification
# ============================================================================

echo ""
echo "🗄️  Verifying database setup..."

# PostgreSQL tables
print_info "Checking PostgreSQL tables..."
POSTGRES_TABLES=$(docker exec postgres-source psql -U postgres -d ecommerce_db -t -c "
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
")

if echo "$POSTGRES_TABLES" | grep -q "customers"; then
    print_success "PostgreSQL tables verified"
else
    print_error "PostgreSQL tables not found"
fi

# ClickHouse tables
print_info "Checking ClickHouse tables..."
CLICKHOUSE_TABLES=$(docker exec clickhouse-analytics clickhouse-client --query "SHOW TABLES FROM ecommerce_analytics")

if echo "$CLICKHOUSE_TABLES" | grep -q "customers"; then
    print_success "ClickHouse tables verified"
else
    print_error "ClickHouse tables not found"
fi

# ============================================================================
# Summary
# ============================================================================

echo ""
echo "================================================"
echo "✅ CDC Pipeline Setup Complete!"
echo "================================================"
echo ""
echo "📌 Access Points:"
echo "   • Kafka UI:        http://localhost:8080"
echo "   • Kafka Connect:   http://localhost:8083"
echo "   • PostgreSQL:      localhost:5432"
echo "   • ClickHouse:      localhost:9000 (native), localhost:8123 (http)"
echo ""
echo "🎯 Next Steps:"
echo "   1. Start Python consumer:"
echo "      cd python && python consumer.py"
echo ""
echo "   2. Generate test data:"
echo "      cd scripts && python test_data_generator.py"
echo ""
echo "   3. Monitor Kafka topics in UI:"
echo "      http://localhost:8080"
echo ""
echo "================================================"

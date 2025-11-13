-- ============================================================================
-- ClickHouse Schema - Ecommerce Analytics
-- Database: ecommerce_analytics
-- Purpose: OLAP analytical database optimized for real-time queries
-- ============================================================================

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS ecommerce_analytics;

USE ecommerce_analytics;

-- ============================================================================
-- CUSTOMERS TABLE - Optimized for Analytics
-- ============================================================================

CREATE TABLE IF NOT EXISTS customers
(
    customer_id UUID,
    first_name String,
    last_name String,
    email String,
    phone Nullable(String),
    address Nullable(String),
    city Nullable(String),
    state Nullable(String),
    zip_code Nullable(String),
    country String DEFAULT 'USA',
    date_of_birth Nullable(Date),
    customer_since DateTime,
    is_active UInt8,
    created_at DateTime,
    updated_at DateTime,
    
    -- CDC metadata
    cdc_operation String,
    cdc_timestamp DateTime64(3),
    cdc_source_db String,
    cdc_source_table String,
    
    -- Event time for processing
    event_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (customer_id, created_at)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- VEHICLES TABLE - Optimized for Analytics
-- ============================================================================

CREATE TABLE IF NOT EXISTS vehicles
(
    vehicle_id UUID,
    vin String,
    make String,
    model String,
    year UInt16,
    color Nullable(String),
    mileage UInt32,
    transmission String,
    fuel_type String,
    body_type String,
    engine_size Nullable(String),
    price Decimal(12, 2),
    status String,
    condition String,
    description Nullable(String),
    features Nullable(String), -- JSON stored as String
    listed_date Date,
    created_at DateTime,
    updated_at DateTime,
    
    -- CDC metadata
    cdc_operation String,
    cdc_timestamp DateTime64(3),
    cdc_source_db String,
    cdc_source_table String,
    
    -- Event time for processing
    event_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (vehicle_id, make, model, year)
SETTINGS index_granularity = 8192;

-- Index for VIN lookups
ALTER TABLE vehicles ADD INDEX idx_vin vin TYPE bloom_filter(0.01) GRANULARITY 1;

-- ============================================================================
-- ORDERS TABLE - Optimized for Analytics
-- ============================================================================

CREATE TABLE IF NOT EXISTS orders
(
    order_id UUID,
    customer_id UUID,
    order_date DateTime,
    total_amount Decimal(12, 2),
    tax_amount Decimal(12, 2),
    discount_amount Decimal(12, 2),
    final_amount Decimal(12, 2),
    status String,
    payment_status String,
    shipping_address Nullable(String),
    billing_address Nullable(String),
    notes Nullable(String),
    created_at DateTime,
    updated_at DateTime,
    
    -- CDC metadata
    cdc_operation String,
    cdc_timestamp DateTime64(3),
    cdc_source_db String,
    cdc_source_table String,
    
    -- Event time for processing
    event_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_id, customer_id, order_date)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- ORDER ITEMS TABLE - Optimized for Analytics
-- ============================================================================

CREATE TABLE IF NOT EXISTS order_items
(
    order_item_id UUID,
    order_id UUID,
    vehicle_id UUID,
    quantity UInt16,
    unit_price Decimal(12, 2),
    discount Decimal(12, 2),
    subtotal Decimal(12, 2),
    warranty_plan Nullable(String),
    extended_warranty UInt8,
    created_at DateTime,
    updated_at DateTime,
    
    -- CDC metadata
    cdc_operation String,
    cdc_timestamp DateTime64(3),
    cdc_source_db String,
    cdc_source_table String,
    
    -- Event time for processing
    event_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (order_item_id, order_id, vehicle_id)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- PAYMENTS TABLE - Optimized for Analytics
-- ============================================================================

CREATE TABLE IF NOT EXISTS payments
(
    payment_id UUID,
    order_id UUID,
    payment_date DateTime,
    payment_method String,
    amount Decimal(12, 2),
    transaction_id Nullable(String),
    payment_status String,
    payment_provider Nullable(String),
    card_last_four Nullable(String),
    authorization_code Nullable(String),
    created_at DateTime,
    updated_at DateTime,
    
    -- CDC metadata
    cdc_operation String,
    cdc_timestamp DateTime64(3),
    cdc_source_db String,
    cdc_source_table String,
    
    -- Event time for processing
    event_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(payment_date)
ORDER BY (payment_id, order_id, payment_date)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- MATERIALIZED VIEWS FOR REAL-TIME ANALYTICS
-- ============================================================================

-- Daily Sales Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, status)
AS
SELECT
    toDate(order_date) as order_date,
    status,
    payment_status,
    count() as total_orders,
    sum(final_amount) as total_revenue,
    avg(final_amount) as avg_order_value,
    sum(tax_amount) as total_tax,
    sum(discount_amount) as total_discounts
FROM orders
GROUP BY order_date, status, payment_status;

-- Vehicle Inventory Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_vehicle_inventory
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(updated_at)
ORDER BY (make, model, year, status)
AS
SELECT
    make,
    model,
    year,
    condition,
    status,
    count() as total_count,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    avg(mileage) as avg_mileage,
    max(updated_at) as last_updated
FROM vehicles
GROUP BY make, model, year, condition, status;

-- Customer Purchase Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_summary
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (customer_id, order_date)
AS
SELECT
    o.customer_id,
    toDate(o.order_date) as order_date,
    count() as total_orders,
    sum(o.final_amount) as total_spent,
    avg(o.final_amount) as avg_order_value,
    max(o.order_date) as last_order_date
FROM orders o
GROUP BY customer_id, order_date;

-- Payment Method Analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_payment_analytics
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(payment_date)
ORDER BY (payment_date, payment_method, payment_status)
AS
SELECT
    toDate(payment_date) as payment_date,
    payment_method,
    payment_status,
    count() as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as avg_transaction_amount
FROM payments
GROUP BY payment_date, payment_method, payment_status;

-- ============================================================================
-- ANALYTICAL QUERIES - EXAMPLES
-- ============================================================================

-- Query 1: Real-time Sales Dashboard (Last 30 days)
-- SELECT 
--     toDate(order_date) as date,
--     count() as orders,
--     sum(final_amount) as revenue,
--     avg(final_amount) as avg_order
-- FROM orders
-- WHERE order_date >= now() - INTERVAL 30 DAY
-- GROUP BY date
-- ORDER BY date DESC;

-- Query 2: Top Selling Vehicle Models
-- SELECT 
--     v.make,
--     v.model,
--     count() as units_sold,
--     sum(oi.subtotal) as total_revenue
-- FROM order_items oi
-- JOIN vehicles v ON oi.vehicle_id = v.vehicle_id
-- GROUP BY v.make, v.model
-- ORDER BY units_sold DESC
-- LIMIT 10;

-- Query 3: Customer Lifetime Value
-- SELECT 
--     c.customer_id,
--     c.first_name,
--     c.last_name,
--     count(o.order_id) as total_orders,
--     sum(o.final_amount) as lifetime_value
-- FROM customers c
-- JOIN orders o ON c.customer_id = o.customer_id
-- WHERE o.status = 'Completed'
-- GROUP BY c.customer_id, c.first_name, c.last_name
-- ORDER BY lifetime_value DESC;

-- Query 4: Inventory Status by Make
-- SELECT 
--     make,
--     status,
--     count() as count,
--     avg(price) as avg_price
-- FROM vehicles
-- GROUP BY make, status
-- ORDER BY make, status;

-- ============================================================================
-- PostgreSQL Schema - Ecommerce de Autos
-- Database: ecommerce_db
-- Purpose: OLTP transactional database with CDC enabled
-- ============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ============================================================================
-- TABLES
-- ============================================================================

-- Customers Table
CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50) DEFAULT 'USA',
    date_of_birth DATE,
    customer_since TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Vehicles Table
CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vin VARCHAR(17) UNIQUE NOT NULL,
    make VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INTEGER NOT NULL CHECK (year >= 1900 AND year <= 2030),
    color VARCHAR(50),
    mileage INTEGER DEFAULT 0 CHECK (mileage >= 0),
    transmission VARCHAR(20) CHECK (transmission IN ('Automatic', 'Manual', 'CVT', 'Electric')),
    fuel_type VARCHAR(20) CHECK (fuel_type IN ('Gasoline', 'Diesel', 'Electric', 'Hybrid', 'Plug-in Hybrid')),
    body_type VARCHAR(30) CHECK (body_type IN ('Sedan', 'SUV', 'Truck', 'Coupe', 'Convertible', 'Hatchback', 'Van')),
    engine_size VARCHAR(20),
    price DECIMAL(12, 2) NOT NULL CHECK (price >= 0),
    status VARCHAR(20) DEFAULT 'Available' CHECK (status IN ('Available', 'Reserved', 'Sold', 'Maintenance')),
    condition VARCHAR(20) DEFAULT 'New' CHECK (condition IN ('New', 'Used', 'Certified Pre-Owned')),
    description TEXT,
    features JSONB,
    listed_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders Table
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12, 2) NOT NULL CHECK (total_amount >= 0),
    tax_amount DECIMAL(12, 2) DEFAULT 0 CHECK (tax_amount >= 0),
    discount_amount DECIMAL(12, 2) DEFAULT 0 CHECK (discount_amount >= 0),
    final_amount DECIMAL(12, 2) GENERATED ALWAYS AS (total_amount + tax_amount - discount_amount) STORED,
    status VARCHAR(30) DEFAULT 'Pending' CHECK (status IN ('Pending', 'Confirmed', 'Processing', 'Completed', 'Cancelled', 'Refunded')),
    payment_status VARCHAR(30) DEFAULT 'Pending' CHECK (payment_status IN ('Pending', 'Paid', 'Failed', 'Refunded')),
    shipping_address TEXT,
    billing_address TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Items Table
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    vehicle_id UUID NOT NULL REFERENCES vehicles(vehicle_id),
    quantity INTEGER DEFAULT 1 CHECK (quantity > 0),
    unit_price DECIMAL(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount DECIMAL(12, 2) DEFAULT 0 CHECK (discount >= 0),
    subtotal DECIMAL(12, 2) GENERATED ALWAYS AS (quantity * unit_price - discount) STORED,
    warranty_plan VARCHAR(50),
    extended_warranty BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Payments Table
CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(order_id),
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payment_method VARCHAR(30) NOT NULL CHECK (payment_method IN ('Credit Card', 'Debit Card', 'Bank Transfer', 'Cash', 'Financing', 'Cryptocurrency')),
    amount DECIMAL(12, 2) NOT NULL CHECK (amount > 0),
    transaction_id VARCHAR(255),
    payment_status VARCHAR(20) DEFAULT 'Pending' CHECK (payment_status IN ('Pending', 'Completed', 'Failed', 'Refunded')),
    payment_provider VARCHAR(50),
    card_last_four VARCHAR(4),
    authorization_code VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR OPTIMIZATION
-- ============================================================================

-- Customers indexes
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_phone ON customers(phone);
CREATE INDEX idx_customers_active ON customers(is_active);
CREATE INDEX idx_customers_created ON customers(created_at);

-- Vehicles indexes
CREATE INDEX idx_vehicles_vin ON vehicles(vin);
CREATE INDEX idx_vehicles_status ON vehicles(status);
CREATE INDEX idx_vehicles_make_model ON vehicles(make, model);
CREATE INDEX idx_vehicles_year ON vehicles(year);
CREATE INDEX idx_vehicles_price ON vehicles(price);
CREATE INDEX idx_vehicles_condition ON vehicles(condition);

-- Orders indexes
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_payment_status ON orders(payment_status);

-- Order Items indexes
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_vehicle ON order_items(vehicle_id);

-- Payments indexes
CREATE INDEX idx_payments_order ON payments(order_id);
CREATE INDEX idx_payments_date ON payments(payment_date);
CREATE INDEX idx_payments_status ON payments(payment_status);
CREATE INDEX idx_payments_transaction ON payments(transaction_id);

-- ============================================================================
-- TRIGGERS FOR UPDATED_AT
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicles_updated_at BEFORE UPDATE ON vehicles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_order_items_updated_at BEFORE UPDATE ON order_items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_payments_updated_at BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- REPLICA IDENTITY FOR CDC
-- ============================================================================

-- Set REPLICA IDENTITY to FULL for all tables to capture all column changes
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE vehicles REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;

-- ============================================================================
-- INITIAL SAMPLE DATA
-- ============================================================================

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, phone, city, state, zip_code, date_of_birth)
VALUES 
    ('John', 'Doe', 'john.doe@email.com', '555-0101', 'New York', 'NY', '10001', '1985-03-15'),
    ('Jane', 'Smith', 'jane.smith@email.com', '555-0102', 'Los Angeles', 'CA', '90001', '1990-07-22'),
    ('Michael', 'Johnson', 'michael.j@email.com', '555-0103', 'Chicago', 'IL', '60601', '1988-11-30');

-- Insert sample vehicles
INSERT INTO vehicles (vin, make, model, year, color, mileage, transmission, fuel_type, body_type, engine_size, price, condition, description)
VALUES 
    ('1HGBH41JXMN109186', 'Tesla', 'Model 3', 2024, 'White', 0, 'Electric', 'Electric', 'Sedan', 'Electric Motor', 45000.00, 'New', 'Brand new Tesla Model 3 with autopilot'),
    ('2HGFC2F59LH123456', 'Toyota', 'Camry', 2023, 'Silver', 15000, 'Automatic', 'Gasoline', 'Sedan', '2.5L I4', 28000.00, 'Used', 'Well-maintained Toyota Camry'),
    ('5YJSA1E14HF123789', 'Ford', 'F-150', 2024, 'Blue', 0, 'Automatic', 'Gasoline', 'Truck', '5.0L V8', 55000.00, 'New', 'Powerful F-150 pickup truck');

-- ============================================================================
-- STATISTICS AND ANALYSIS
-- ============================================================================

-- Update statistics for query optimization
ANALYZE customers;
ANALYZE vehicles;
ANALYZE orders;
ANALYZE order_items;
ANALYZE payments;

-- Grant permissions for Debezium user
-- Note: This should be run separately with proper credentials
-- CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_password';
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
-- GRANT USAGE ON SCHEMA public TO debezium_user;

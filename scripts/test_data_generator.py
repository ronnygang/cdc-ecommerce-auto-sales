"""
Test Data Generator - Ecommerce CDC Pipeline
Generates realistic test data for the auto ecommerce system
Simulates real-world operations: INSERT, UPDATE, DELETE
"""

import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EcommerceDataGenerator:
    """Generate realistic test data for auto ecommerce."""
    
    # Sample data pools
    FIRST_NAMES = [
        'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer',
        'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara',
        'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah'
    ]
    
    LAST_NAMES = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
        'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez',
        'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson'
    ]
    
    CITIES = [
        ('New York', 'NY', '10001'),
        ('Los Angeles', 'CA', '90001'),
        ('Chicago', 'IL', '60601'),
        ('Houston', 'TX', '77001'),
        ('Phoenix', 'AZ', '85001'),
        ('Philadelphia', 'PA', '19019'),
        ('San Antonio', 'TX', '78201'),
        ('San Diego', 'CA', '92101'),
        ('Dallas', 'TX', '75201'),
        ('San Jose', 'CA', '95101')
    ]
    
    VEHICLE_MAKES = {
        'Tesla': ['Model 3', 'Model Y', 'Model S', 'Model X'],
        'Toyota': ['Camry', 'Corolla', 'RAV4', 'Highlander', 'Tacoma'],
        'Ford': ['F-150', 'Mustang', 'Explorer', 'Escape', 'Bronco'],
        'Honda': ['Civic', 'Accord', 'CR-V', 'Pilot'],
        'Chevrolet': ['Silverado', 'Equinox', 'Malibu', 'Tahoe'],
        'BMW': ['3 Series', '5 Series', 'X3', 'X5'],
        'Mercedes-Benz': ['C-Class', 'E-Class', 'GLC', 'GLE']
    }
    
    COLORS = ['White', 'Black', 'Silver', 'Gray', 'Blue', 'Red', 'Green', 'Yellow']
    
    TRANSMISSIONS = ['Automatic', 'Manual', 'CVT', 'Electric']
    FUEL_TYPES = ['Gasoline', 'Diesel', 'Electric', 'Hybrid', 'Plug-in Hybrid']
    BODY_TYPES = ['Sedan', 'SUV', 'Truck', 'Coupe', 'Convertible', 'Hatchback']
    
    PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'Bank Transfer', 'Cash', 'Financing']
    
    def __init__(self, db_config):
        """Initialize data generator with database connection."""
        self.db_config = db_config
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def generate_customer(self) -> dict:
        """Generate a random customer."""
        first_name = random.choice(self.FIRST_NAMES)
        last_name = random.choice(self.LAST_NAMES)
        city, state, zip_code = random.choice(self.CITIES)
        
        return {
            'first_name': first_name,
            'last_name': last_name,
            'email': f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@email.com",
            'phone': f"555-{random.randint(1000, 9999)}",
            'address': f"{random.randint(100, 9999)} Main Street",
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'date_of_birth': self._random_date(1960, 2000)
        }
    
    def generate_vehicle(self) -> dict:
        """Generate a random vehicle."""
        make = random.choice(list(self.VEHICLE_MAKES.keys()))
        model = random.choice(self.VEHICLE_MAKES[make])
        year = random.randint(2018, 2024)
        
        # Determine transmission and fuel type based on make
        if make == 'Tesla':
            transmission = 'Electric'
            fuel_type = 'Electric'
        else:
            transmission = random.choice(self.TRANSMISSIONS[:3])
            fuel_type = random.choice(self.FUEL_TYPES[:4])
        
        # Generate price based on make and year
        base_price = {
            'Tesla': 45000, 'BMW': 50000, 'Mercedes-Benz': 55000,
            'Toyota': 28000, 'Honda': 27000, 'Ford': 35000, 'Chevrolet': 32000
        }.get(make, 30000)
        
        price = base_price + (year - 2018) * 2000 + random.randint(-5000, 10000)
        
        return {
            'vin': self._generate_vin(),
            'make': make,
            'model': model,
            'year': year,
            'color': random.choice(self.COLORS),
            'mileage': random.randint(0, 50000) if year < 2024 else 0,
            'transmission': transmission,
            'fuel_type': fuel_type,
            'body_type': random.choice(self.BODY_TYPES),
            'engine_size': '2.5L I4' if fuel_type != 'Electric' else 'Electric Motor',
            'price': Decimal(str(price)),
            'condition': 'New' if year == 2024 else random.choice(['Used', 'Certified Pre-Owned']),
            'description': f"{year} {make} {model} in excellent condition"
        }
    
    def _generate_vin(self) -> str:
        """Generate a random VIN number."""
        chars = 'ABCDEFGHJKLMNPRSTUVWXYZ0123456789'
        return ''.join(random.choice(chars) for _ in range(17))
    
    def _random_date(self, start_year, end_year) -> str:
        """Generate random date."""
        start = datetime(start_year, 1, 1)
        end = datetime(end_year, 12, 31)
        delta = end - start
        random_days = random.randint(0, delta.days)
        return (start + timedelta(days=random_days)).date()
    
    def insert_customers(self, count: int):
        """Insert random customers."""
        cursor = self.conn.cursor()
        
        for i in range(count):
            customer = self.generate_customer()
            
            query = """
                INSERT INTO customers 
                (first_name, last_name, email, phone, address, city, state, zip_code, date_of_birth)
                VALUES (%(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(address)s, 
                        %(city)s, %(state)s, %(zip_code)s, %(date_of_birth)s)
                RETURNING customer_id
            """
            
            cursor.execute(query, customer)
            customer_id = cursor.fetchone()[0]
            
            logger.info(f"Inserted customer {i+1}/{count}: {customer_id}")
            time.sleep(0.1)  # Small delay to simulate real-time
        
        cursor.close()
    
    def insert_vehicles(self, count: int):
        """Insert random vehicles."""
        cursor = self.conn.cursor()
        
        for i in range(count):
            vehicle = self.generate_vehicle()
            
            query = """
                INSERT INTO vehicles 
                (vin, make, model, year, color, mileage, transmission, fuel_type, 
                 body_type, engine_size, price, condition, description)
                VALUES (%(vin)s, %(make)s, %(model)s, %(year)s, %(color)s, %(mileage)s,
                        %(transmission)s, %(fuel_type)s, %(body_type)s, %(engine_size)s,
                        %(price)s, %(condition)s, %(description)s)
                RETURNING vehicle_id
            """
            
            cursor.execute(query, vehicle)
            vehicle_id = cursor.fetchone()[0]
            
            logger.info(f"Inserted vehicle {i+1}/{count}: {vehicle_id}")
            time.sleep(0.1)
        
        cursor.close()
    
    def create_order(self, customer_id: str, vehicle_ids: list):
        """Create an order with items."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Create order
        total_amount = 0
        tax_rate = Decimal('0.08')
        
        # Get vehicle prices
        vehicle_prices = {}
        for vid in vehicle_ids:
            cursor.execute("SELECT vehicle_id, price FROM vehicles WHERE vehicle_id = %s", (vid,))
            result = cursor.fetchone()
            if result:
                vehicle_prices[result['vehicle_id']] = result['price']
                total_amount += result['price']
        
        tax_amount = total_amount * tax_rate
        
        order_query = """
            INSERT INTO orders (customer_id, total_amount, tax_amount, status, payment_status)
            VALUES (%s, %s, %s, 'Confirmed', 'Pending')
            RETURNING order_id
        """
        
        cursor.execute(order_query, (customer_id, total_amount, tax_amount))
        order_id = cursor.fetchone()['order_id']
        
        # Create order items
        for vehicle_id, price in vehicle_prices.items():
            item_query = """
                INSERT INTO order_items (order_id, vehicle_id, quantity, unit_price)
                VALUES (%s, %s, 1, %s)
            """
            cursor.execute(item_query, (order_id, vehicle_id, price))
            
            # Update vehicle status
            cursor.execute("UPDATE vehicles SET status = 'Reserved' WHERE vehicle_id = %s", (vehicle_id,))
        
        logger.info(f"Created order {order_id} for customer {customer_id}")
        cursor.close()
        return order_id
    
    def create_payment(self, order_id: str, amount: Decimal):
        """Create a payment for an order."""
        cursor = self.conn.cursor()
        
        payment_method = random.choice(self.PAYMENT_METHODS)
        
        query = """
            INSERT INTO payments 
            (order_id, payment_method, amount, payment_status, transaction_id)
            VALUES (%s, %s, %s, 'Completed', %s)
            RETURNING payment_id
        """
        
        transaction_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
        cursor.execute(query, (order_id, payment_method, amount, transaction_id))
        payment_id = cursor.fetchone()[0]
        
        # Update order payment status
        cursor.execute("UPDATE orders SET payment_status = 'Paid', status = 'Processing' WHERE order_id = %s", (order_id,))
        
        logger.info(f"Created payment {payment_id} for order {order_id}")
        cursor.close()
        return payment_id
    
    def simulate_operations(self, duration_seconds: int = 60):
        """Simulate random database operations."""
        logger.info(f"Starting simulation for {duration_seconds} seconds...")
        
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            operation = random.choice(['insert_customer', 'insert_vehicle', 'update_customer', 
                                      'update_vehicle', 'create_order'])
            
            try:
                if operation == 'insert_customer':
                    customer = self.generate_customer()
                    cursor.execute("""
                        INSERT INTO customers (first_name, last_name, email, phone, city, state, zip_code)
                        VALUES (%(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(city)s, %(state)s, %(zip_code)s)
                    """, customer)
                    logger.info(f"Operation: {operation}")
                
                elif operation == 'insert_vehicle':
                    vehicle = self.generate_vehicle()
                    cursor.execute("""
                        INSERT INTO vehicles (vin, make, model, year, color, price, condition)
                        VALUES (%(vin)s, %(make)s, %(model)s, %(year)s, %(color)s, %(price)s, %(condition)s)
                    """, vehicle)
                    logger.info(f"Operation: {operation}")
                
                elif operation == 'update_customer':
                    cursor.execute("SELECT customer_id FROM customers WHERE is_active = true ORDER BY RANDOM() LIMIT 1")
                    result = cursor.fetchone()
                    if result:
                        cursor.execute("UPDATE customers SET phone = %s WHERE customer_id = %s", 
                                     (f"555-{random.randint(1000, 9999)}", result['customer_id']))
                        logger.info(f"Operation: {operation}")
                
                elif operation == 'update_vehicle':
                    cursor.execute("SELECT vehicle_id FROM vehicles WHERE status = 'Available' ORDER BY RANDOM() LIMIT 1")
                    result = cursor.fetchone()
                    if result:
                        new_price = Decimal(str(random.randint(20000, 80000)))
                        cursor.execute("UPDATE vehicles SET price = %s WHERE vehicle_id = %s", 
                                     (new_price, result['vehicle_id']))
                        logger.info(f"Operation: {operation}")
                
                time.sleep(random.uniform(0.5, 2))
                
            except Exception as e:
                logger.error(f"Error in operation {operation}: {e}")
        
        cursor.close()
        logger.info("Simulation completed")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def main():
    """Main function to run data generation."""
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'ecommerce_db',
        'user': 'postgres',
        'password': 'postgres123'
    }
    
    generator = EcommerceDataGenerator(db_config)
    
    try:
        # Generate initial data
        logger.info("Generating initial test data...")
        generator.insert_customers(20)
        generator.insert_vehicles(50)
        
        # Simulate ongoing operations
        logger.info("Starting real-time operations simulation...")
        generator.simulate_operations(duration_seconds=300)  # 5 minutes
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        generator.close()


if __name__ == '__main__':
    main()

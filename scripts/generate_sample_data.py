"""
Sample data generator for testing the ETL pipeline.
Creates realistic e-commerce data for development and testing.
"""
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict
import os


class SampleDataGenerator:
    """Generates realistic sample data for e-commerce ETL testing."""
    
    def __init__(self):
        self.customers = []
        self.products = []
        self.orders = []
        self.payments = []
        
        # Sample data pools
        self.first_names = ["John", "Sarah", "Mike", "Emily", "David", "Lisa", "Chris", "Amanda", "Robert", "Jennifer"]
        self.last_names = ["Smith", "Johnson", "Davis", "Brown", "Wilson", "Miller", "Taylor", "Garcia", "Martinez", "Lopez"]
        self.cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
        self.states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
        self.categories = ["Electronics", "Home & Kitchen", "Sports & Outdoors", "Fashion", "Office Supplies"]
        self.brands = ["TechBrand", "HomeStyle", "SportMax", "StyleCorp", "PaperCorp"]
        self.payment_types = ["Credit Card", "PayPal", "Debit Card", "Cash"]
    
    def generate_customers(self, count: int = 50) -> List[Dict]:
        """Generate sample customer data."""
        customers = []
        
        for i in range(1, count + 1):
            customer = {
                "customer_id": f"CUST{i:03d}",
                "customer_name": f"{random.choice(self.first_names)} {random.choice(self.last_names)}",
                "email": f"customer{i}@email.com",
                "phone": f"555-{random.randint(1000, 9999)}",
                "address": f"{random.randint(100, 999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Maple'])} {random.choice(['St', 'Ave', 'Rd', 'Dr', 'Ln'])}",
                "city": random.choice(self.cities),
                "state": random.choice(self.states),
                "zip_code": f"{random.randint(10000, 99999)}",
                "age": random.randint(18, 70),
                "registration_date": (datetime.now() - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d")
            }
            customers.append(customer)
        
        self.customers = customers
        return customers
    
    def generate_products(self, count: int = 30) -> List[Dict]:
        """Generate sample product data."""
        products = []
        
        product_names = [
            "Wireless Headphones", "Smart Watch", "Coffee Mug", "Running Shoes", "Desk Lamp",
            "Phone Case", "Notebook Set", "Bluetooth Speaker", "Yoga Mat", "Kitchen Scale",
            "Backpack", "Pen Set", "Tablet Stand", "Scented Candles", "Gaming Mouse",
            "Water Bottle", "Fitness Tracker", "Desk Chair", "Monitor Stand", "Keyboard",
            "Wireless Charger", "Travel Mug", "Exercise Ball", "Desk Organizer", "LED Strip",
            "Laptop Bag", "Sticky Notes", "Phone Stand", "Essential Oils", "Gaming Headset"
        ]
        
        for i in range(1, count + 1):
            category = random.choice(self.categories)
            subcategories = {
                "Electronics": ["Audio", "Wearables", "Accessories", "Computer"],
                "Home & Kitchen": ["Drinkware", "Lighting", "Appliances", "Decor"],
                "Sports & Outdoors": ["Footwear", "Fitness", "Equipment"],
                "Fashion": ["Bags", "Accessories"],
                "Office Supplies": ["Stationery", "Organization"]
            }
            
            cost = round(random.uniform(5, 150), 2)
            price = round(cost * random.uniform(1.5, 3.0), 2)
            
            product = {
                "product_id": f"PROD{i:03d}",
                "product_name": product_names[i-1] if i <= len(product_names) else f"Product {i}",
                "category": category,
                "subcategory": random.choice(subcategories[category]),
                "brand": random.choice(self.brands),
                "price": price,
                "cost": cost,
                "description": f"High-quality {product_names[i-1].lower()} for everyday use" if i <= len(product_names) else f"Description for product {i}"
            }
            products.append(product)
        
        self.products = products
        return products
    
    def generate_orders(self, count: int = 100) -> List[Dict]:
        """Generate sample order data."""
        orders = []
        
        for i in range(1, count + 1):
            customer = random.choice(self.customers)
            product = random.choice(self.products)
            quantity = random.randint(1, 5)
            order_date = datetime.now() - timedelta(days=random.randint(1, 90))
            
            order = {
                "order_id": f"ORD{i:03d}",
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "order_date": order_date.strftime("%Y-%m-%d"),
                "payment_type": random.choice(self.payment_types)
            }
            orders.append(order)
        
        self.orders = orders
        return orders
    
    def generate_payments(self) -> List[Dict]:
        """Generate payment data based on orders."""
        payments = []
        
        for i, order in enumerate(self.orders, 1):
            total_amount = order["quantity"] * order["unit_price"]
            payment_date = datetime.strptime(order["order_date"], "%Y-%m-%d") + timedelta(days=random.randint(0, 2))
            
            payment = {
                "payment_id": f"PAY{i:03d}",
                "order_id": order["order_id"],
                "payment_method": order["payment_type"],
                "payment_amount": round(total_amount, 2),
                "payment_date": payment_date.strftime("%Y-%m-%d"),
                "payment_status": random.choice(["Completed", "Pending", "Failed"]) if random.random() > 0.9 else "Completed"
            }
            payments.append(payment)
        
        self.payments = payments
        return payments
    
    def save_to_csv(self, data: List[Dict], filename: str, output_dir: str = "data/raw") -> None:
        """Save data to CSV file."""
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        
        if data:
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            
            print(f"✓ Generated {len(data)} records in {filepath}")
    
    def generate_all_data(self, customers_count: int = 50, products_count: int = 30, orders_count: int = 200):
        """Generate all sample datasets."""
        print("🔄 Generating sample e-commerce data...")
        
        # Generate data
        self.generate_customers(customers_count)
        self.generate_products(products_count)
        self.generate_orders(orders_count)
        self.generate_payments()
        
        # Save to CSV files
        self.save_to_csv(self.customers, "customers.csv")
        self.save_to_csv(self.products, "products.csv")
        self.save_to_csv(self.orders, "orders.csv")
        self.save_to_csv(self.payments, "payments.csv")
        
        print("✅ Sample data generation completed!")
        
        # Print summary
        print(f"\nGenerated datasets:")
        print(f"- Customers: {len(self.customers)} records")
        print(f"- Products: {len(self.products)} records")
        print(f"- Orders: {len(self.orders)} records")
        print(f"- Payments: {len(self.payments)} records")


def main():
    """Main execution function."""
    generator = SampleDataGenerator()
    
    # Generate larger dataset for testing
    generator.generate_all_data(
        customers_count=100,
        products_count=50,
        orders_count=500
    )


if __name__ == "__main__":
    main()
import pandas as pd
from faker import Faker
import random

def extract_orders(n=100):
    fake = Faker()
    data = []
    for _ in range(n):
        data.append({
            'order_id': fake.uuid4(),
            'customer_id': fake.uuid4(),
            'product_id': fake.uuid4(),
            'quantity': random.randint(1, 10),
            'price': round(random.uniform(10.0, 500.0), 2),
            'discount': round(random.uniform(0.0, 50.0), 2),
            'order_date': fake.date_this_year(),
            'status': random.choice(['Completed', 'Pending', 'Cancelled']),
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer']),
            'shipping_address': fake.address()
        })
    return pd.DataFrame(data)

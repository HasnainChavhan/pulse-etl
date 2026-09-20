import pandas as pd
from faker import Faker
import random

def extract_products(n=20):
    fake = Faker()
    data = []
    categories = ['Electronics', 'Clothing', 'Home', 'Toys', 'Sports']
    for _ in range(n):
        data.append({
            'product_id': fake.uuid4(),
            'name': fake.word().capitalize() + " " + fake.word().capitalize(),
            'category': random.choice(categories),
            'subcategory': fake.word().capitalize(),
            'price': round(random.uniform(10.0, 1000.0), 2),
            'cost': round(random.uniform(5.0, 500.0), 2),
            'supplier': fake.company()
        })
    return pd.DataFrame(data)

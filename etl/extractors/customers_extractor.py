import pandas as pd
from faker import Faker
import random

def extract_customers(n=50):
    fake = Faker()
    data = []
    for _ in range(n):
        data.append({
            'customer_id': fake.uuid4(),
            'name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'city': fake.city(),
            'country': fake.country(),
            'signup_date': fake.date_this_decade(),
            'segment': random.choice(['Premium', 'Standard', 'Basic'])
        })
    return pd.DataFrame(data)

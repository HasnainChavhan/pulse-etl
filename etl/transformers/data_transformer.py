import pandas as pd
import numpy as np

def transform_orders(df):
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['revenue'] = df['quantity'] * df['price'] * (1 - df['discount']/100)
    df.drop_duplicates(subset=['order_id'], inplace=True)
    return df

def transform_customers(df):
    df['signup_date'] = pd.to_datetime(df['signup_date'])
    df.drop_duplicates(subset=['customer_id'], inplace=True)
    return df

def transform_products(df):
    df['profit_margin'] = (df['price'] - df['cost']) / df['price']
    df.drop_duplicates(subset=['product_id'], inplace=True)
    return df

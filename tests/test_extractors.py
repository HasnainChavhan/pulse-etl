from etl.extractors.orders_extractor import extract_orders
from etl.extractors.customers_extractor import extract_customers
from etl.extractors.products_extractor import extract_products

def test_extract_orders():
    df = extract_orders(5)
    assert len(df) == 5
    assert 'order_id' in df.columns

def test_extract_customers():
    df = extract_customers(5)
    assert len(df) == 5
    assert 'customer_id' in df.columns

def test_extract_products():
    df = extract_products(5)
    assert len(df) == 5
    assert 'product_id' in df.columns

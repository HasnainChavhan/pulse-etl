import pandas as pd
from etl.transformers.data_transformer import transform_orders

def test_transform_orders():
    data = {'order_id': ['1', '2'], 'quantity': [2, 1], 'price': [10.0, 20.0], 'discount': [0, 10], 'order_date': ['2023-01-01', '2023-01-02']}
    df = pd.DataFrame(data)
    transformed = transform_orders(df)
    assert 'revenue' in transformed.columns
    assert transformed.iloc[0]['revenue'] == 20.0
    assert transformed.iloc[1]['revenue'] == 18.0

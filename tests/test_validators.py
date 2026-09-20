import pytest
import pandas as pd
from etl.validators.data_quality import validate_dataframe, DataQualityError

def test_validate_dataframe():
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    report = validate_dataframe(df, ['a'])
    assert report['null_counts']['a'] == 0
    
    with pytest.raises(DataQualityError):
        validate_dataframe(df, ['c'])

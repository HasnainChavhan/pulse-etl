class DataQualityError(Exception):
    pass

def validate_dataframe(df, required_columns):
    report = {'missing_cols': [], 'null_counts': {}}
    for col in required_columns:
        if col not in df.columns:
            report['missing_cols'].append(col)
        else:
            report['null_counts'][col] = df[col].isnull().sum()
            
    if report['missing_cols']:
        raise DataQualityError(f"Missing columns: {report['missing_cols']}")
        
    return report

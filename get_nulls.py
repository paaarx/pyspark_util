from pyspark.sql.functions import count, when, isnull

def get_nulls(df):
    """
    https://github.com/paaarx
    Returns a DataFrame with null count.

    For every column in DataFrame, analyzes each row to search for null values
    and count how many are found.
    Only columns with null values will be returned.

    Parameters:
        df (DataFrame): The DataFrame to be analyzed.

    Returns:
        DataFrame: DataFrame with columns and null count.
    """

    expression = []

    for column in df.columns:
        expression.append(count(when(isnull(column), column)).alias(column))

    df_with_nulls = df.select(expression)

    column_list = []

    for key, value in df_with_nulls.collect()[0].asDict().items():
        if value > 0:
            column_list.append(key)

    return df_with_nulls.select(column_list)

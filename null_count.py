from pyspark.sql.functions import count, when, isnull


def null_count(df):
    """
    https://github.com/paaarx
    Returns a DataFrame with null count.
    For every column in DataFrame, analyzes each row to search for null values
    and count how many are found.

    Parameters:
        df (DataFrame): The DataFrame to be analyzed.
    Returns:
        DataFrame: DataFrame with columns and null count.
    """

    expression = []

    for column in df.columns:
        expression.append(count(when(isnull(column), column)).alias(column))

    df_with_nulls = df.select(expression)

    data = [(k, v) for k, v in df_with_nulls.collect()[0].asDict().items()]

    return spark.createDataFrame(data, ['column', 'count'])

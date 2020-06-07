from pyspark.sql.functions import count


def unique_count(df):
    """
    https://github.com/paaarx
    Returns a DataFrame with unique values count.
    For every column in DataFrame, analyzes each row to search for unique
    values and count how many are found.
    Parameters:
        df (DataFrame): The DataFrame to be analyzed.
    Returns:
        DataFrame: DataFrame with columns and unique values count.
    """

    def unique_count(df, asc):
    unique_count_dict = {}

    for column in df.columns:
        unique_count_dict[column] = df.select(column).distinct().count()

    data = [(k,)+(v,) for k, v in unique_count_dict.items()]

    return spark.createDataFrame(data, ['column', 'count'])

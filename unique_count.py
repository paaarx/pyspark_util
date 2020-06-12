from pyspark.sql.functions import count


def unique_count(df, column, ascending=False):
    """
    Returns a sorted DataFrame with unique values count.

    https://github.com/paaarx

    Parameters:
        df (DataFrame): The DataFrame to be analyzed.
        column (str): The DataFrame column to be analyzed.
        ascending (bool): Sort the DataFrame by value count (default False).

    Returns:
        DataFrame: DataFrame with two columns, first column is the original
        column from column parameter, second is 'count', the count from values
        for each value.
    """

    df = df \
        .groupby(column) \
        .count() \
        .orderBy('count', ascending=ascending)

    return df

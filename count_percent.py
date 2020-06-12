import pyspark.sql.functions as f
from pyspark.sql.functions import count, col
from pyspark.sql.window import Window


def count_percent(df, column, ascending=False):
    """
    Returns a sorted DataFrame with values count and percent of each value.

    https://github.com/paaarx
    The DataFrame is sorted by value count.

    Parameters:
        df (DataFrame): The DataFrame to be analyzed.
        column (str): The DataFrame column to be analyzed.
        ascending (bool): Sort the DataFrame by value count (default False).

    Returns:
        DataFrame: DataFrame with three columns, first column is the original
        column from column parameter, second is 'count', the count from values
        for each value, third is 'percent', the percent of each value.
    """

    # First need a count for each value
    df = df \
        .groupby(column) \
        .count()

    # Get a percent over each group
    df = df \
        .withColumn('percent',
                    f.round(((f.col('count') / f.sum('count')
                              .over(Window.partitionBy())) * 100), 2)) \
        .orderBy('count', ascending=ascending)

    return df

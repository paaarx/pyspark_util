from pyspark.sql.functions import monotonically_increasing_id

def fillna_by_type(df, string_fill, number_fill, date_fill):
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

    string_columns = []
    number_columns = []
    date_columns = []

    for data_type in df.dtypes:
        if data_type[1] == 'string':
            string_columns.append(data_type[0])
        if data_type[1] == 'float' \
            or data_type[1] == 'double' \
            or data_type[1] == 'int' \
            or data_type[1] == 'long' \
            or data_type[1] == 'bigint':
            number_columns.append(data_type[0])
        if data_type[1] == 'timestamp':
            date_columns.append(data_type[0])

    df_string = df.select(string_columns) \
        .fillna(string_fill, subset=string_columns) \
        .withColumn('index', monotonically_increasing_id())

    df_number = df.select(number_columns) \
        .fillna(number_fill, subset=number_columns) \
        .withColumn('index', monotonically_increasing_id())

    df_date = df.select(date_columns) \
        .fillna(number_fill, subset=date_columns) \
        .withColumn('index', monotonically_increasing_id())

    return df_string.join(df_number, 'index', 'outer') \
        .join(df_date, 'index', 'outer') \
        .drop('index')

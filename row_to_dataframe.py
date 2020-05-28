def row_to_dataframe(row, **kwargs):
    """
    https://github.com/paaarx
    Convert Row into DataFrame.

    For schema, you just need inform one parameter.
    DataFrame have the priority for schema.

    Parameters:
        row (Row): A Row object
        df (DataFrame): A DataFrame with SAME schema from Row
        schema (StructType): A StructType with SAME schema from Row
    Returns:
        DataFrame: DataFrame with schema applied.
    """

    df = kwargs.get('df')
    schema = kwargs.get('schema')
    if df:
        schema = df.schema

    rdd = sc.parallelize(row)

    return sqlContext.createDataFrame(rdd, schema)

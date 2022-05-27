def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import unix_timestamp, input_file_name, from_utc_timestamp, current_timestamp, lit, col

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = (df
          .withColumn("tpep_datetime", lit((unix_timestamp(col('tpep_dropoff_datetime')) - unix_timestamp(col('tpep_pickup_datetime')))/60).cast("int"))
          .withColumn("file_name", lit(input_file_name()))
          .withColumn("insert_timestamp", lit(from_utc_timestamp(current_timestamp(), 'IST'))))

    dfc_processed = DynamicFrame.fromDF(df, glueContext, "processed")
    return (DynamicFrameCollection({"CustomTransform0": dfc_processed}, glueContext))

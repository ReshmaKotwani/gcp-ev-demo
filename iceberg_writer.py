from pyspark.sql import DataFrame

def write_iceberg_table(
    df: DataFrame,
    catalog_name: str,
    namespace: str,
    table_name: str,
    mode: str = "overwrite"
):
   
    full_table_identifier = f"{catalog_name}.{namespace}.{table_name}"
    spark = df.sparkSession # Get the SparkSession from the DataFrame

    print(f"Attempting to write DataFrame to Iceberg table: {full_table_identifier}")
    print(f"Write mode: {mode}")

    try:
        # For HadoopCatalog, namespaces correspond to directories in the warehouse path.
        # Spark SQL CREATE NAMESPACE handles this.
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace}")
        print(f"Ensured namespace '{namespace}' exists in catalog '{catalog_name}'.")

        # Write the DataFrame to the Iceberg table
        df.write \
          .format("iceberg") \
          .mode(mode) \
          .saveAsTable(full_table_identifier)

        print(f"Successfully wrote data to Iceberg table: {full_table_identifier}")

    except Exception as e:
        print(f"Error writing to Iceberg table {full_table_identifier}: {e}")
        # Re-raise the exception for Dataproc job failure reporting
        raise e

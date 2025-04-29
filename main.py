from pyspark.sql import SparkSession
from config import bucket_name, file_path, iceberg_catalog_name, iceberg_warehouse_path, allowed_columns, iceberg_table_namespace, iceberg_table_name
from google.cloud import storage

from data_loader import get_json_data_from_gcs
from parser import build_schema_from_meta, create_dataframe
from transformer import apply_transformations, apply_quality_checks, select_allowed_columns
from iceberg_writer import write_iceberg_table
# from data_quality_checks import run_quality_checks

def create_spark_session_with_iceberg():
    """Creates and configures a SparkSession for Iceberg on GCS Hadoop Catalog."""

    iceberg_spark_runtime = "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2" 

    # Catalog Configuration (Using Hadoop Catalog on GCS)
    catalog_name = iceberg_catalog_name 
    warehouse_path = iceberg_warehouse_path 

    print("Creating SparkSession with Iceberg configuration...")
    builder = SparkSession.builder \
        .appName("EV Data Processing with Iceberg (GCS Catalog)") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        # If JARs are NOT bundled in Dataproc image:
        # .config("spark.jars.packages", iceberg_spark_runtime) \

    spark = builder.getOrCreate()

    print("SparkSession created.")
    print(f"Using Iceberg catalog '{catalog_name}' with warehouse '{warehouse_path}'")
    return spark

if __name__ == "__main__":
    try:
        data, meta = get_json_data_from_gcs(bucket_name, file_path)
        print("\nSample data row:", data[0] if data else "No data found")
        print("\nSample meta column:", meta[0] if meta else "No metadata found")
    except Exception as e:
        print(f"\nFailed to get JSON data: {e}")
    
    spark = None # Initialize spark variable
    try:
        # --- Setup Spark Session ---
        spark = create_spark_session_with_iceberg()

        # --- Step 1: Get Raw Data ---
        print("\n--- Step 1: Reading Raw JSON Data ---")
        data_list, meta_columns = get_json_data_from_gcs(bucket_name, file_path)
        if not data_list or not meta_columns:
             raise ValueError("Failed to retrieve data or metadata from GCS.")

        # --- Step 2: Build Schema and Create DataFrame ---
        print("\n--- Step 2: Parsing Data and Creating DataFrame ---")
        schema = build_schema_from_meta(meta_columns)
        df = create_dataframe(spark, data_list, schema)

        print("\nInitial DataFrame Schema:")
        df.printSchema()
        print("\nSample Data (first 5 rows):")
        df.show(5, truncate=False)

        # --- Step 3 and 4: Transformation and Data Quality Checks ---
        print("\n--- Steps 3 and 4 Transform and Data Quality checks placeholder ---")
        df_transformed = apply_transformations(df)
        df_quality_checked = apply_quality_checks(df_transformed)

        df_final = select_allowed_columns(df_quality_checked, allowed_columns)

        print("\nFinal DataFrame Schema (after column selection):")
        df_final.printSchema()
        print("\nSample Data (Final):")
        df_final.show(5, truncate=False)

         # --- Step 5: Write to Iceberg Table ---
        write_iceberg_table(
            df=df_final,
            catalog_name=iceberg_catalog_name,
            namespace=iceberg_table_namespace,
            table_name=iceberg_table_name,
            mode="overwrite" 
        )
        print("--- Iceberg write operation complete. ---")
    
    except Exception as e:
        print(f"\nPipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        raise # Re-raise the exception for Dataproc job failure reporting
    finally:
        if spark:
            print("\nStopping Spark session.")
            spark.stop()

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, lit, trim, to_date
from pyspark.sql.types import StringType, DateType, DoubleType, LongType, BooleanType

def apply_quality_checks(df: DataFrame):
   
    print("Applying data quality checks...")
    # 1. Count Nulls in key columns
    key_columns = ['vin_1_10', 'dol_vehicle_id', 'model_year', 'make', 'model']
    print("Null counts per key column:")
    df.select([count(when(col(c).isNull(), c)).alias(c + '_nulls') for c in key_columns]).show()

    # 2. Check for duplicates (e.g., based on VIN or DOL Vehicle ID)
    unique_id = 'dol_vehicle_id' # Or 'vin_1_10' - check data uniqueness
    total_count = df.count()
    distinct_count = df.select(unique_id).distinct().count()
    print(f"Total Rows: {total_count}, Distinct '{unique_id}': {distinct_count}")
    if total_count != distinct_count:
        print(f"Warning: Duplicate '{unique_id}' found.")
        # df.groupBy(unique_id).count().filter("count > 1").show()

    # 3. Check valid range (e.g., Model Year)
    print("Checking Model Year range...")
    df.selectExpr("min(model_year) as min_year", "max(model_year) as max_year").show()

    # 4. Trim whitespace from string columns
    string_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for sc in string_columns:
        df = df.withColumn(sc, trim(col(sc)))
    print("Trimmed whitespace from string columns.")

    # Example: Filter out rows with null critical identifiers
    df = df.na.drop(subset=["vin_1_10", "dol_vehicle_id"])
    print(f"Rows after dropping null IDs: {df.count()}")

    return df

def apply_transformations(df: DataFrame):
    
     print("Applying transformations...")
     # Example: Convert date string column if it exists and follows a pattern
     if 'expected_date' in df.columns:
         print("Attempting to parse 'expected_date'...")
         # The format 'yyyy-MM-dd'T'HH:mm:ss' seems common in Socrata APIs
         df = df.withColumn("expected_date_parsed", to_date(col("expected_date"), "yyyy-MM-dd'T'HH:mm:ss"))
         # Check how many failed parsing
         failed_parses = df.filter(col("expected_date_parsed").isNull() & col("expected_date").isNotNull()).count()
         if failed_parses > 0:
             print(f"Warning: {failed_parses} rows failed 'expected_date' parsing.")

     # Add other transformations like cleaning specific text fields, deriving new columns etc.
     return df

def select_allowed_columns(df: DataFrame, allowed_columns: list) -> DataFrame:
    
    print(f"Selecting columns based on predefined ALLOWED_COLUMNS list ({len(allowed_columns)} specified)...")

    # Find which allowed columns actually exist in the DataFrame at this point
    columns_to_select = [c for c in allowed_columns if c in df.columns]
    selected_set = set(columns_to_select)
    original_set = set(df.columns)
    dropped_columns = original_set - selected_set
    # Check if any columns listed in config were NOT found in the DataFrame
    missing_allowed_columns = set(allowed_columns) - original_set

    if missing_allowed_columns:
        print(f"Warning: Columns specified in ALLOWED_COLUMNS list but not found in the DataFrame: {sorted(list(missing_allowed_columns))}")
    if dropped_columns:
        print(f"Dropping {len(dropped_columns)} columns that are not in ALLOWED_COLUMNS: {sorted(list(dropped_columns))}")

    if not columns_to_select:
        raise ValueError("No columns to select! Check ALLOWED_COLUMNS and DataFrame schema.")

    print(f"Selecting {len(columns_to_select)} columns for the final table.")
    return df.select(*columns_to_select) # Select only the columns found in both lists
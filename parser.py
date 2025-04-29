from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType 

def map_data_type(meta_type_name: str):
  
    type_name_lower = meta_type_name.lower() if meta_type_name else 'text'

    if type_name_lower == 'number':
        # Defaulting to DoubleType for flexibility.
        return DoubleType()
    elif type_name_lower == 'text':
        return StringType()
    elif type_name_lower == 'checkbox': # e.g., 'true'/'false' strings
         return BooleanType() # Or StringType if raw value needed
    else:
        # Default to StringType for unknown types.
        return StringType()

def build_schema_from_meta(meta_columns: list) -> StructType:

    fields = []
    seen_names = set() # To handle rare duplicate field names
    print("Building Spark schema from metadata...")
    for i, col_info in enumerate(meta_columns):
        col_name = col_info.get('fieldName')
        if not col_name:
             print(f"Warning: Column definition at position {i} is missing 'fieldName'. Using default name 'col_{i}'.")
             col_name = f'col_{i}' # Generate a placeholder name

        # Clean the column name (replace non-alphanumeric chars) - Important for Iceberg/Parquet compatibility
        cleaned_col_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in col_name)
        if cleaned_col_name != col_name:
            print(f"Info: Cleaned column name from '{col_name}' to '{cleaned_col_name}'.")
            col_name = cleaned_col_name

        # Handle potential duplicate names after cleaning
        original_col_name = col_name
        suffix = 1
        while col_name in seen_names:
            print(f"Warning: Duplicate column name '{original_col_name}' detected after cleaning/generation. Appending suffix.")
            col_name = f"{original_col_name}_{suffix}"
            suffix += 1
        seen_names.add(col_name)

        data_type = map_data_type(col_info.get('dataTypeName'))
        fields.append(StructField(col_name, data_type, True))

    if not fields:
         raise ValueError("Could not generate any fields for the schema from metadata.")

    print(f"Schema built successfully with {len(fields)} fields.")
    return StructType(fields)

def _convert_row_types(raw_row: list, schema: StructType) -> Row | None:
    
    if len(raw_row) != len(schema.fields):
        return None # Skip this row

    converted_values = {}
    for i, field in enumerate(schema.fields):
        raw_value = raw_row[i]
        field_name = field.name
        target_type = field.dataType

        # Handle nulls
        if raw_value is None:
            converted_values[field_name] = None
            continue

        # Attempt type conversion
        try:
            if isinstance(target_type, (LongType, DoubleType)): 
                # Convert to float first, then target type if needed
                num_val = float(raw_value)
                if isinstance(target_type, LongType):
                    converted_values[field_name] = int(num_val) # Potential truncation/rounding
                else:
                    converted_values[field_name] = num_val
            elif isinstance(target_type, BooleanType):
                 if isinstance(raw_value, str):
                     # Handle common string representations of boolean
                     converted_values[field_name] = raw_value.lower() in ['true', 'yes', '1', 't']
                 else:
                     converted_values[field_name] = bool(raw_value) # Standard Python bool conversion
            elif isinstance(target_type, StringType):
                converted_values[field_name] = str(raw_value)
            else: # Includes Date/Timestamp initially kept as String
                converted_values[field_name] = str(raw_value)
        except (ValueError, TypeError) as e:
            # Conversion failed - set to null for this field
            converted_values[field_name] = None

    return Row(**converted_values)


def create_dataframe(spark: SparkSession, data_list: list, schema: StructType) -> DataFrame:
    """Creates a Spark DataFrame from a list of lists using the provided schema."""
    if not data_list:
        print("Warning: Input data_list is empty. Returning empty DataFrame.")
        return spark.createDataFrame([], schema)

    # Parallelize the raw Python list data into an RDD
    raw_rdd = spark.sparkContext.parallelize(data_list)

    # Map RDD: Convert each raw list row into a Spark Row object with type handling
    row_rdd = raw_rdd.map(lambda row: _convert_row_types(row, schema))

    # Filter out rows that couldn't be processed (returned None from helper)
    valid_row_rdd = row_rdd.filter(lambda x: x is not None)

    # Create the DataFrame from the RDD of Row objects and the schema
    df = spark.createDataFrame(valid_row_rdd, schema)

    final_count = df.count() # Action to materialize and count
    print(f"DataFrame created successfully with {final_count} rows.")
   
    return df
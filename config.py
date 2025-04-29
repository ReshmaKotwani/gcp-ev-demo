bucket_name = "ev-demo-source"
file_path = "ElectricVehiclePopulationData.json"
code_bukcet = "ev-code"
processed_bucket = "ev-data-curated"

iceberg_catalog_name = "gcp_catalog"
iceberg_warehouse_path = f"gs://{processed_bucket}/Warehouse"
iceberg_table_namespace = "ev_data"
iceberg_table_name = "vehicles_population_data"

bigquery_dataset = "vehicle_data_lakehouse" # Example BQ dataset
bigquery_table = "electric_vehicles_iceberg_ext" # Example BQ external table name

allowed_columns = [
    'vin_1_10',
    'county',
    'city',
    'state',
    'zip_code', # Renamed/cleaned from 'postal_code' if necessary
    'model_year', # This should be LongType/DoubleType after Step 2 parsing
    'make',
    'model',
    'ev_type', # Renamed/cleaned from 'electric_vehicle_type'
    'cafv_type', # Renamed/cleaned from 'clean_alternative_fuel_vehicle_eligibility'
    'electric_range', # Should be DoubleType
    'base_msrp', # Should be DoubleType
    'legislative_district', # Should be DoubleType
    'dol_vehicle_id', # String ID
    'geocoded_column', # String containing point geometry? Check content.
    'electric_utility',
    # 'transaction_date_parsed',
    # 'make_standardized',
]
create_ddl = """
CREATE EXTERNAL TABLE IF NOT EXISTS
{database}.{table} (
    {columns}
)
"""

partitions = """{partitions}"""

data_format = """
STORED AS PARQUET
LOCATION '{root_bucket}{key}/'
tblproperties ("parquet.compress"="SNAPPY")
"""

parquet_athena_table_ddl = """
CREATE EXTERNAL TABLE IF NOT EXISTS
{database}.{table} (
    {columns}
)
{partitions}
STORED AS PARQUET
LOCATION '{root_bucket}{key}/'
tblproperties ("parquet.compress"="SNAPPY")
"""

table_columns = {
"sources": """
    source_id INT, 
    source_system_type STRING, 
    source_system STRING, 
    source_reporting_unit STRING, 
    source_reporting_unit_name STRING""",
    
"reporters": """
    reporter_id INT,
    nwcg_reporting_agency STRING,
    nwcg_reporting_unit_id STRING,
    nwcg_reporting_unit_name STRING
""",
    
"fire_names": """
    fire_name_id INT,
    fire_name STRING,
    ics_209_name STRING,
    fire_code STRING,
    mtbs_fire_name STRING
""",
    
"fire_causes": """
    fire_cause_id INT,
    stat_cause_code INT,
    stat_cause_descr STRING
""",
    
"fire_sizes": """
    fire_size_id INT,
    fire_size_class STRING,
    lower_bound DOUBLE,
    upper_bound DOUBLE
""",
    
"owners": """
    owner_id INT,
    owner_code INT,
    owner_descr STRING
""",
    
"locations": """
    location_id INT,
    state STRING,
    fips_name STRING
""",

"dates": """
    date_dim_id INT,
    date_type STRING,
    date_actual DATE,
    epoch INT,
    day_name STRING,
    day_of_week INT,
    day_of_month INT,
    day_of_year INT,
    week_of_month STRING,
    week_of_year INT,
    month_actual INT,
    month_name STRING,
    month_name_abbreviated STRING,
    quarter_actual INT,
    quarter_name STRING,
    year_actual INT
""",
    
"weather_types": """
    weather_type_id INT,
    type STRING
""",
    
"fires_fact": """
    fire_id INT,
    fpa_id STRING,
    discovery_date_id INT,
    cont_date_dim_id INT,
    source_id INT,
    reporter_id INT,
    fire_name_id INT,
    fire_size_id INT,
    fire_cause_id INT,
    owner_id INT,
    location_id INT,
    fire_size DECIMAL(10, 2),
    latitude DECIMAL(11, 8),
    longitude DECIMAL(11, 8),
    discovery_timestamp STRING,
    cont_timestamp STRING
""",
    
"weather_fact": """
    weather_outlier_id STRING,
    weather_date_dim_id INT,
    weather_type_id INT,
    longitude STRING,
    latitude STRING,
    max_temp STRING,
    min_temp STRING
"""
}

fact_table_partitions = {
    "fires_fact": "PARTITIONED BY (year STRING)",
    "weather_fact": "PARTITIONED BY (year STRING)"
}
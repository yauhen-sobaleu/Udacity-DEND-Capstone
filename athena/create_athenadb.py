import boto3
from time import sleep
from athena_ddl import *

# read in config file
with open("../src/main/resources/application.conf", "r") as ins:
    array = []
    for line in ins:
        array.append(line.replace("\n", '').replace('spark.', '').split("="))

config = dict(array)

# create athena session
session = boto3.Session(
    aws_access_key_id=config['awskey'],
    aws_secret_access_key=config['awssecret']
)
ath = session.client('athena')

# global parameters
DATABASE = config['athena.dbname']
ROOT_BUCKET = f"s3://{config['rootbucket']}/"
ATHENA_LOGS_OUTPUT = f"{ROOT_BUCKET}{config['athena.logsoutputbucket']}/"

print(f"Root buckets is: {ROOT_BUCKET}")
print(f"Athena database name is: {DATABASE}")
print(f"Athena logs output path is: {ATHENA_LOGS_OUTPUT}")

def get_query_status_response(query_execution_id):
    response = ath.get_query_execution(QueryExecutionId=query_execution_id)
    return response
    
def wait_for_query_to_complete(query_execution_id):
    is_query_running = True
    backoff_time = 5
    while is_query_running:
        response = get_query_status_response(query_execution_id)
        status = response["QueryExecution"]["Status"][
            "State"
        ]  # possible responses: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
        if status == "SUCCEEDED":
            is_query_running = False
        elif status in ["CANCELED", "FAILED"]:
            raise ValueError("Query failed")
        elif status in ["QUEUED", "RUNNING"]:
            sleep(backoff_time)
        else:
            raise ValueError("Query failed")
                
def run_query(query: str, database: str = DATABASE, logs_output: str = ATHENA_LOGS_OUTPUT) -> str:
    """Run a query on Amazon Athena
    
    Arguments:
        query {string} -- actual query to run on Amazon Athena service
    
    Keyword Arguments:
        database {string} -- name of a database (default: {DATABASE})
        logs_output {string} -- s3 bucket where to store athena logs (default: {ATHENA_LOGS_OUTPUT})
    
    Returns:
        string -- result of an HTTP requests
    """
    response = ath.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': logs_output,
        }
    )
    
    query_execution_id = response["QueryExecutionId"]
    
    wait_for_query_to_complete(query_execution_id)

def create_athena_table(table: str, columns: str, partitions: str = None, database: str = DATABASE, root_bucket: str = ROOT_BUCKET):
    """Create a table in Amazon Athena
    
    Arguments:
        table {string} -- name of Amazon Athena table
        columns {string} -- ddl string with column names and their data types in the resulting Amazon Athena table
    
    Keyword Arguments:
        partitions {string} -- ddl string with parquet partitioning format (default: {None})
        database {string} -- database name (default: {DATABASE})
        root_bucket {string} -- s3 root bucket (default: {ROOT_BUCKET})
    """
    print(f"Creating {table} table")
    
    run_query(f"DROP TABLE IF EXISTS {table}")
    
    # concatenate a DDL string
    if partitions:
        athena_create_table_ddl = create_ddl + partitions.format(partitions=partitions) + data_format
    else:
        athena_create_table_ddl = create_ddl + data_format

    # run a query specifying values in placeholders
    run_query(athena_create_table_ddl.format(
        database=database,
        table=table,
        columns=columns,
        root_bucket=root_bucket,
        key=table
        )
    )
    # refresh partition metadata
    run_query(f"MSCK REPAIR TABLE {table}")
    
    print(f"Table {table} has been created")


# create a database
run_query(query=f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# create tables
for table, columns in table_columns.items():
    if table.endswith("fact"):
        create_athena_table(table, columns, fact_table_partitions[table])
    else:
        create_athena_table(table, columns)

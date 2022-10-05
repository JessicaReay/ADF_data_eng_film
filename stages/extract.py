import boto3
import io
import pandas as pd
from sqlalchemy import create_engine
from stages.authentication import get_credentials

def extract():

    sections = ["postgresql", "aws_s3", "csv"]
    credential_names = [
        ["database", "user", "password", "host", "port"], 
        ["service_name", "region_name", "aws_access_key_id", "aws_secret_access_key", "s3_bucket"],
        ["source", "target"]
    ]

    credentials = get_credentials(sections, credential_names)

    warehouse_credentials = credentials[sections[0]]
    datalake_credentials = credentials[sections[1]]
    local_credentials = credentials[sections[2]]

   # Postgres
    if None not in warehouse_credentials and "" not in warehouse_credentials:
        database, user, password, host, port = warehouse_credentials
        warehouse_df = warehouse_extraction("IMDB_movie_data", database, user, password, host)
    else:
        raise Exception("Extraction failed: error with DB credentials")
    
    # AWS S3
    if None not in datalake_credentials and "" not in datalake_credentials: 
        service_name, region_name, aws_access_key_id, aws_secret_access_key, s3_bucket = datalake_credentials
        datalake_df = datalake_extraction("IMDB-Movie-Data-S3.csv", service_name, region_name, aws_access_key_id, aws_secret_access_key, s3_bucket)
    else:
        raise Exception("Extraction failed: error with DB credentials")
    
    local_df = local_extraction(local_credentials[0])

    return [warehouse_df, datalake_df, local_df]

def warehouse_extraction(table, database, user, password, host):
        dbConnection = None 
        try:
            # Create an engine instance
            alchemyEngine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}', pool_recycle=3600)

            # Connect to PostgreSQL server
            dbConnection = alchemyEngine.connect()

            # Read data from PostgreSQL database table and load into a DataFrame instance
            sql = f"select * from \"{table}\""
            warehouse_df = pd.read_sql(sql, dbConnection)

        finally:
            dbConnection.close()
        
        return warehouse_df

def datalake_extraction(file_name, service_name, region_name, aws_access_key_id, aws_secret_access_key, s3_bucket):
    
    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    string_io = io.BytesIO()
    s3_resource.Object(s3_bucket, file_name).download_fileobj(string_io)
    s3_contents = string_io.getvalue()
    
    datalake_df = pd.read_csv(io.BytesIO(s3_contents))
    
    return datalake_df

def local_extraction(filepath):
    local_df = pd.read_csv(filepath)
    return local_df
    

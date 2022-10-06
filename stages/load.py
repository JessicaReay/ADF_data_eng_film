import boto3
import pandas as pd
from sqlalchemy import create_engine
from stages.authentication import get_credentials

'''
This file contains functions to load dataframes into seperate forms
- load_warehouse: Inputs the dataframe and table name and outputs the dataframe into the corresponding table in database. 
- load datalake: Inputs the datafile from path provided, outputs the file into a AWS S3 Bucket
- export_report: Inputs two dataframes (one with the report format and one with the raw merged data), outputs into an excel file in the corresponding path
'''

def load_warehouse(df, table_name):
    section = "postgresql"
    credential_names = ["database", "user", "password", "host", "port"]
    credentials = get_credentials([section], [credential_names])[section]

    if None not in credentials and "" not in credentials:
        database, user, password, host, port = credentials

        try:
            # Create an engine instance
            alchemyEngine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}', pool_recycle=3600)
            # Connect to PostgreSQL server
            dbConnection = alchemyEngine.connect()
            # Upload data to sql database
            df.to_sql(table_name, dbConnection, if_exists='replace') #if data already exists, drop the table before inserting new values

        finally:
            dbConnection.close()

    else:
        raise Exception("Upload failed: error with credentials")


def load_datalake(file_name):

    section = "aws_s3"
    credential_names = ["service_name", "region_name", "aws_access_key_id", "aws_secret_access_key", "s3_bucket"]
    credentials = get_credentials([section], [credential_names])[section]

    if None not in credentials and "" not in credentials:
        service_name, region_name, aws_access_key_id, aws_secret_access_key, s3_bucket = credentials
        
        s3_resource = boto3.resource(
            service_name=service_name,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        s3_resource.Object(s3_bucket, file_name).upload_file(file_name)
    
    else:
        raise Exception("Upload failed: error with credentials")

def export_report(report_df, final_df):

    writer = pd.ExcelWriter("report output/report.xlsx")

    report_df.to_excel(writer, sheet_name= "Genre Ranking")
    final_df.to_excel(writer, sheet_name= "Raw")
    writer.close()


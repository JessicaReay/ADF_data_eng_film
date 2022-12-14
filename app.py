import logging
import pandas as pd
from stages.extract import extract
from stages.transform import transform_merge, generate_report
from stages.load import load_warehouse, load_datalake, export_report
from stages.reporting import send_email
 
def app():
    logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG)
    print("Commencing ETL...")

    # 1. load IMDB-Movie-Data-Postgres.csv into Postgres database
    print("Loading IMDB-Movie-Data-Postgres into Postgres database table...")
    logging.info("Starting loading Postgres csv file into database")
    table_name = 'IMDB_movie_data'
    to_load_warehouse_df = pd.read_csv("C:\\Users\\jessi\\Documents\\ADF_data_eng_film\\ADF_data_eng_film\\data_files\\IMDB-Movie-Data-Postgres.csv") 
    load_warehouse(to_load_warehouse_df, table_name)
    logging.info("Finished loading csv into database")
    print("Load completed!")

    # 2. load IMDB-Movie-Data-S3.csv into AWS S3 Bucket
    print("Loading IMDB-Movie-Data-S3 into  AWS S3 Bucket...")
    logging.info("Starting loading S3 csv file into s3 bucket")
    file_name = "data_files\IMDB-Movie-Data-S3.csv"
    load_datalake(file_name)
    logging.info("Finished loading csv into s3 bucket")
    print("Load completed!")

    # 3. Extract the files into dataframes
    print("Extracting data from sources...")
    logging.info("Starting extraction from Postgres, AWS S3 bucket and Local file")
    dataframes = extract()
    logging.info("Finished extraction!")
    print("Data Extracted!")

    # 4. merge the seperate dataframes into one dataframe
    print("Transforming seperate dataframe into one dataframe...")
    logging.info("Starting transformation to merge seperate csv files")
    final_df = transform_merge(dataframes)
    logging.info("Finished Merging Transformation!")
    print("Finished Merging Transformation!")

    # 5. generate the daily report from the merged dataframe to final the top ranking movie genres
    print("Generating daily report...")
    logging.info("Start generating a report to find the top ranking genres")
    report_df = generate_report(final_df)
    logging.info("Finished generating the daily report!")
    print("Report generated!")

    # 6. Export the final report into excel sheet which includes a tab with the top ranking movie genres and a tab with the raw merged dataframe 
    print("Exporting report into excel sheet...")
    logging.info("Exporting report excel file into output folder")
    export_report(report_df, final_df)
    logging.info("Finished exporting file, ready for daily email")
    print("Exported completed!")

    # 7. Send an daily email which include the top 5 genres in the body and the attachment of the exported excel file
    print("Sending report via email...")
    logging.info("Send the Report in daily morning email")
    send_email(report_df)
    logging.info("Email sent")
    print("Email sent!")    
    print("Completed ETL")

if __name__ == "__main__":
    app()
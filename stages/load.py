import io
import boto3
import pandas as pd
from sqlalchemy import create_engine
from stages.authentication import get_credentials

def export_report(report_df, final_df):

    writer = pd.ExcelWriter("report output/report.xlsx")

    report_df.to_excel(writer, sheet_name= "Genre Ranking")
    final_df.to_excel(writer, sheet_name= "Raw")
    writer.close()
import logging
from stages.extract import extract
from stages.transform import transform_merge, generate_report
from stages.load import export_report
from stages.reporting import send_email

def app():
    logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG)

    #add logging for loading into postgres and aws

    logging.info("Starting extraction...")
    dataframes = extract()
    logging.info("Finished extraction!")

    logging.info("Starting transformation to merge csv files...")
    final_df= transform_merge(dataframes)
    logging.info("Finished Transformation!")

    logging.info("Start generating a report...")
    report_df = generate_report(final_df)
    logging.info("Finished loading into data warehouse!")

    export_report(report_df, final_df)

    logging.info("send email...")
    send_email(report_df)
    logging.info("Finished loading to s3!")


if __name__ == "__main__":
    app()
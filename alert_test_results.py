from test_app.alerting import check_failure_file, check_for_failures
import logging

def failures():
    print('Reading if failed tests...')
    logging.info("Checking for failed tests...")
    check_for_failures(f'C:\\Users\\jessi\\Documents\\ADF_data_eng_film\\ADF_data_eng_film\\test_app\\')
    logging.info("finishing checking for failures")

    print('Alerting any failed tests...')
    logging.info("sending any failure alerts")
    check_failure_file("C:\\Users\\jessi\\Documents\\ADF_data_eng_film\\ADF_data_eng_film\\test_app\\send_failure_alert\\")
    logging.info("alerts sent..")
    print('All Checks Completed!')

if __name__ == "__main__":
    failures()


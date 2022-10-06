from test_app.alerting import check_failure_file, check_for_failures
import logging

'''
This file checks if the any of the validation tests resulted in a failure by reading the daily txt file (reads the date on the file) extracted from pytest run.
If founds failues sends an alert via email and attachs the txt file with the failure. 
'''


def failures():
    print('Checking for any failed tests...')
    logging.info("Checking for failed tests...")
    check_for_failures(f'C:\\Users\\jessi\\Documents\\ADF_data_eng_film\\ADF_data_eng_film\\test_app\\')
    logging.info("Finished Examining tests")

    logging.info("Setting up alerts")
    check_failure_file("C:\\Users\\jessi\\Documents\\ADF_data_eng_film\\ADF_data_eng_film\\test_app\\send_failure_alert\\")
    logging.info("Checks completed")
    print('All Checks Completed!')

if __name__ == "__main__":
    failures()


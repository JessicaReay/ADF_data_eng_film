from stages.authentication import get_credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import datetime
from datetime import date
import os

'''
This file alerts any failures
-check_for_failures: Inputs: directory with pytest results, outputs: if pytest results contain failures: txt file with the pytest results to path 'send_failure_alert', if no failures: print 'no failures'
-check_failure_file: Inputs: directory 'send_failure_alert' with txt file, if today date is in file send alert, if no file in directory with today dates- print 'no failure'
-send_alert: sends email with subject alert and date and pytest txt file attached 
'''

def check_for_failures(directory):
    test_results = [fname for fname in os.listdir(directory) if '.txt' in fname]
    for txt_file in test_results:
        file_path = directory + txt_file
        with open(file_path) as f:
            text = f.read()
        if 'FAILURES' in text:
            failure_directory = f'C:\\Users\\jessi\\Documents\\ADF_data_eng_film\\ADF_data_eng_film\\test_app\\send_failure_alert\\'
            today = str(date.today())
            out_file = f'{failure_directory}{txt_file}_Failed_Results_{today}.txt'
            alert_file = open(out_file,"w+")
            alert_file.write(txt_file)
            alert_file.write(text)
            alert_file.close()
        else:
            print("No failed tests found in file:")
            print(txt_file)

def check_failure_file(dir):
    for file in os.listdir(dir):
        filetime = datetime.datetime.fromtimestamp(
                os.path.getctime(dir + file))   
        if filetime.date() == date.today():
            send_alert()
        else:
            print("no failure")

def send_alert():
        today = str(date.today())
        failure_directory = f'C:\\Users\\jessi\\Documents\\ADF_data_eng_film\\ADF_data_eng_film\\test_app\\send_failure_alert\\'

        for file in os.listdir(failure_directory):
            filetime = datetime.datetime.fromtimestamp(
                 os.path.getctime(failure_directory + file))   
        if filetime.date() == date.today():
            print(file)

        file_location = f'{failure_directory}{file}'

        sections = ["stmp"]
        credential_names = [["smtp_port", "smtp_server", "smtp_sender_email", "smtp_receiver_email", "smtp_password"]
        ]

        credentials = get_credentials(sections, credential_names)
        stmp_credentials = credentials[sections[0]]

        subject = f"Alert failure {today}"
        message = f'See Failure Alert attached'

        msg = MIMEMultipart()
        msg['From'] = stmp_credentials[2]
        msg['To'] = stmp_credentials[3]
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))

        # Setup the attachment
        filename = os.path.basename(file_location)
        attachment = open(file_location, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

        # Attach the attachment to the MIMEMultipart object
        msg.attach(part)

        server = smtplib.SMTP(stmp_credentials[1], stmp_credentials[0]) #server, port
        server.starttls()
        server.login(stmp_credentials[2], stmp_credentials[4]) #sender email, password
        text = msg.as_string()
        server.sendmail(stmp_credentials[2], stmp_credentials[3], text) #sender email, reciever email
        server.quit()
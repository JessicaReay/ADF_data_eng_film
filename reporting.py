import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
from tabulate import tabulate
import datetime
from stages.authentication import get_credentials

def table(report_df):  

    top_5_data = [[report_df.Rank[0], report_df.index[0], report_df.Revenue_Millions[0]],
    [report_df.Rank[1], report_df.index[1], report_df.Revenue_Millions[1]],
    [report_df.Rank[2], report_df.index[2], report_df.Revenue_Millions[2]],
    [report_df.Rank[3], report_df.index[3], report_df.Revenue_Millions[3]],
    [report_df.Rank[4], report_df.index[4], report_df.Revenue_Millions[4]]]
    
    return top_5_data

def send_email(report_df):
    
    today_date = datetime.datetime.now().strftime('%d %b %Y')

    sections = ["csv", "stmp"]
    credential_names = [["source", "target"],
    ["smtp_port", "smtp_server", "smtp_sender_email", "smtp_receiver_email", "smtp_password"]
    ]

    credentials = get_credentials(sections, credential_names)

    csv_credentials = credentials[sections[0]]
    stmp_credentials = credentials[sections[1]]

    subject = f"Daily Profitable Genres Report for the {today_date}"
    message = f'Hello, \n\n Please find the top 5 Ranked Genres for {today_date} as follows: \n\n {(tabulate(table(report_df), headers=["Rank", "Genre", "Revenue_Millions"]))} \n\n I have also attached a csv file with the rest of the data. \n\n Kind Regards.'

    file_location = csv_credentials[1] #target file path

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

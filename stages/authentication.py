import configparser

'''
This authentication file is used to get the credentials from the config file.

Function inputs: The section in the config file ["postgresql", "aws_s3", "csv", "stmp"] and corresponding credentials defined in the config file. 
Function outputs: The neccessary credential associated with the corresponding credentials 

'''

def get_credentials(sections, credentials):

    config = configparser.ConfigParser()

    # read the configuration file
    config.read('multi_config.ini')

    # get all the connections
    collect_config = {}
    
    #loop through config file and collect all credentials 
    for i, section in enumerate(sections):
        section_credentials = []
        for credential in credentials[i]:
            section_credentials.append(config.get(section, credential))
        collect_config[f"{section}"] = section_credentials
    
    return collect_config

if __name__ == "__main__": 
    sections = ["postgresql", "aws_s3", "csv", "stmp"]
    credential_names = [
        ["database", "user", "password", "host", "port"], 
        ["service_name", "region_name", "aws_access_key_id", "aws_secret_access_key", "s3_bucket"],
        ["source", "target"],
        ["smtp_port", "smtp_server", "smtp_sender_email", "smtp_receiver_email", "smtp_password"]
    ]
    
    # ensuring that the credentials were successful 
    print("Getting credentials...")
    credentials = get_credentials(sections, credential_names)
    print("Collection of credentials were successful!")
    database, user, password, host, port = credentials[sections[0]]
    print("Successful Warehouse (Postgres SQL) credentials collection")
    service_name, region_name, aws_access_key_id, aws_secret_access_key, s3_bucket = credentials[sections[1]]
    print("Sucessful Datalake (AWS S3) credentials collection")
    print(database, user, password, host, port, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    source, target = credentials[sections[2]]
    print("Successful Local (CSV) credentials collection")
    smtp_port, smtp_server, smtp_sender_email, smtp_receiver_email, smtp_password = credentials[sections[3]]
    print("Sucessful Email (smtp) credentials collection")
    print(database, user, password, host, port, service_name, region_name, aws_access_key_id, aws_secret_access_key, s3_bucket, source, target, smtp_port, smtp_server, smtp_sender_email, smtp_receiver_email, smtp_password)
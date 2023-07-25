#Importing libraries
import boto3
import json
import os
from hashlib import sha256
import psycopg2

#Setting up AWS region and endpoint for local stack SQS
aws_region = 'us-east-1'
endpoint_url = 'http://localhost:4566/'

#Assigning a variable to login behavior queue data
queue_name = 'login-queue'

#Setting dummy credentials to access local stack
def set_dummy_credentials():
    """
    A helper function to set dummy AWS credentials for Local Stack.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = 'dummy_access_key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'dummy_secret_key'

#Function to read messages from SQS Queue
def sqs_queue_read_messages():
    """
    Retrieve messages from AWS SQS queue which are stored in Local Stack
    """
    #Setting dummy credentials
    set_dummy_credentials()

    #Creating SQS Client
    sqs_client = boto3.client('sqs', region_name = aws_region, endpoint_url = endpoint_url)

    #Getting the queue url
    response = sqs_client.get_queue_url(QueueName = queue_name)
    queue_url = response['QueueUrl']
    #print("Queue URL:", queue_url)

    #Receiving messages from the SQS Queue
    response = sqs_client.receive_message(
        QueueUrl = queue_url,
        MaxNumberOfMessages = 100,
        VisibilityTimeout = 30,
        WaitTimeSeconds = 20,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
    )

    #Printing number of messages retrieved
    print('\nNumber of messages retrieved:', len(response['Messages']))

    #Checking if there are any messages
    if 'Messages' in response:
        messages = response['Messages']
        for message in messages:

            #Loading JSON data
            json_data = json.loads(message['Body'])
            process_json_data(json_data)
    
    else:
        print('\nNo messages found in the queue.')

#Function for masking Personal Identifiable Information (PII)
def pii_masking(json_data):
    """
    Mask the PII fields 'ip' and 'device_id' in JSON Data.

    Args:
        json_data (dict): JSON data containing user login behavior.

    Returns:
        dict: JSON data with masked PII fields
    """
    #Making a copy of json_data
    json_data_masked = json_data.copy()
    pii_fields = ['device_id', 'ip']

    #Looping through PII fields and masking them using SHA256 hashing
    for field in pii_fields:
        if field in json_data_masked:
            orignal_val = json_data_masked[field]
            masked_val = sha256(orignal_val.encode()).hexdigest()
            json_data_masked[field] = masked_val
    
    return json_data_masked

#Function to process retrieved json data
def process_json_data(json_data):
    """
    Process and transform JOSN data before inserting it into postgres database

    Args:
        json_data (dict): JSON data containing user login behavior.
    """
    #Error Handling
    if 'foo' in json_data and json_data['foo'] == 'oops_wrong_msg_type':
        print('\nError: Received a message with an invalid message type.')
    else:
        #Masking PII fields
        json_data_masked = pii_masking(json_data)
        #print(json_data)
        #print(json_data_masked)

        #Flattening masked json data
        flatten_data = flatten_json_data(json_data_masked)
        #print(flatten_data)

        #Inserting flatten json data in postgres
        insert_postgres(flatten_data)

#Fuction to flatten json data
def flatten_json_data(json_data_masked):
    """
    Flatten JSON data by converting nested keys into dot-separated strings.

    Args:
        json_data_masked (dict): JSON data containing user login beahvior with masked PII.

    Returns:
        dict: Flattened JSON data
    """
    flatten_data = {}

    #Looping through masked json data and flattening it
    for key, value in json_data_masked.items():
        if isinstance(value, dict):
            flat_val = flatten_json_data(value)
            flatten_data.update({f"{key}.{subkey}": subvalue for subkey, subvalue in flat_val.items()})
        else:
            flatten_data[key] = value
    
    return flatten_data

#Function to alter the data type of app_version
def alter_app_version_data_type():
    """
    Alter the data type of app_version from 'int' to 'varchar' in order to handle values like 8.9.2, 9.3.1, etc.
    """
    #Connecting to Postgres Database
    conn = psycopg2.connect(
        host = 'localhost',
        port = 5432,
        database = 'postgres',
        user = 'postgres',
        password = 'postgres'
    )

    #Creating a cursor to execute SQL queries
    cur = conn.cursor()

    #SQL Query to alter the data type of app_version to varchar (as int data type can't handle values like 8.9.2, 9.3.1, etc.)
    alter_query = "ALTER TABLE user_logins ALTER COLUMN app_version TYPE varchar"

    try:
        #Executing the query
        cur.execute(alter_query)

        #Commiting
        conn.commit()
        print('Altered the data type of app_version')

    except psycopg2.Error as e:
        conn.rollback()
        print('Error:', e)
    
    #Closing the connection
    conn.close()

#Function to insert flatten json data to postgres
def insert_postgres(flatten_data):
    """
    Insert the flatten JSON data into the postgres database.

    Args:
        flatten_data (dict): Flatten JSON data containing user login behavior with masked PII.
    """
    #Connecting to Postgres Database
    conn = psycopg2.connect(
        host = 'localhost',
        port = 5432,
        database = 'postgres',
        user = 'postgres',
        password = 'postgres'
    )

    #Creating a cursor to execute SQL queries
    cur = conn.cursor()

    #Preparing the data for insertion in user_logins table
    user_id = flatten_data['user_id']
    device_type = flatten_data['device_type']
    masked_ip = flatten_data['ip']
    masked_device_id = flatten_data['device_id']
    locale = flatten_data['locale']
    app_version = str(flatten_data['app_version'])

    #Handling missing 'create_date' field
    create_date = flatten_data.get('create_date', None)

    #SQL query for insertion
    insert_query = "INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_insert = (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)

    #Executing the query
    cur.execute(insert_query, data_insert)

    #Commiting
    conn.commit()

    #Closing the connection
    conn.close()

#Function to print all rows stored in the user_logins table
def print_user_logins_table():
    """
    Print the contents of the user_logins table after insertion of flatten JSON data (To verify the table is created and populated)
    """
    #Connecting to Postgres Database
    conn = psycopg2.connect(
        host = 'localhost',
        port = 5432,
        database = 'postgres',
        user = 'postgres',
        password = 'postgres'
    )

    #Creating a cursor to execute SQL queries
    cur = conn.cursor()

    #SQL query to retrieve all rows from user_logins table
    select_query = "SELECT * FROM user_logins"

    #Executing the query
    cur.execute(select_query)

    #Fetching all the rows
    rows = cur.fetchall()

    #Headers for the column
    print('\nPrinting TABLE: user_logins\n')
    print('user_id | device_type | masked_ip | masked_device_id | locale | app_version | create_date')

    #Looping and printing rows
    for row in rows:
        user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date = row
        print(f"{user_id} | {device_type} | {masked_ip} | {masked_device_id} | {locale} | {app_version} | {create_date}")
    
    #Closing the connection
    cur.close()
    conn.close()

if __name__ == "__main__":

    #Altering the data type of app_version
    alter_app_version_data_type()

    #Retrieving messaged from AWS SQS Queue
    sqs_queue_read_messages()

    #Printing the contents of user_logins table (To verify the table is created and populated)
    print_user_logins_table()


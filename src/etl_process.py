import boto3
import pandas as pd
import json
import logging as log
import base64
import os
import warnings
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime

load_dotenv('./.env')
warnings.filterwarnings('ignore')

log.basicConfig(level=log.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
def extract():
    """
    Input: None
    Output: list of messages from 'extract' method
    This method is used to recieve messages from sqs and
    extract the body of the message and
    return list of messages as output
    """

    try:
        endpoint_url = base64.b64decode(os.getenv("endpoint_url")).decode('utf-8')
        sqs = boto3.client('sqs', endpoint_url=endpoint_url, region_name = 'us-east-1')
        response = sqs.receive_message(QueueUrl=endpoint_url)
        messages_list = []
        if 'Messages' in response:
            for message in response['Messages']:
                message_body = json.loads(message['Body'])
                messages_list.append(message_body)
                log.info("Received message:%s", message['Body'])
        else:
            log.info("No messages found in the queue.")
    
        if messages_list:
            return messages_list
        return []
    except Exception as e:
        log.error("Error occured in 'extract' function:%s ",e)
        raise

def transform(received_messages):
    """
    Input: list of messages from 'extract' method
    Output: Pandas Dataframe
    Description: This method is used to transform recieved messages and
                 mask columns 'devide_id', 'ip' and 
                 return dataframe as a output
    Assumptions:
                App version is in below format (Major.Minor.Patch) -> will be converted to (MajorMinorPatch)
    """
    try:
        df = pd.DataFrame(received_messages)
        log.info("encoding ip, device_id columns")
        df['masked_ip'] = df.ip.str.encode('utf-8', 'strict').apply(base64.b64encode)
        df['masked_device_id'] = df.device_id.str.encode('utf-8', 'strict').apply(base64.b64encode)
        df['masked_ip'] = df['masked_ip'].str.decode('utf-8')
        df['masked_device_id'] = df['masked_device_id'].str.decode('utf-8')

        df['app_version'] = df['app_version'].str.replace('.', '').astype(int)
        df['create_date'] = datetime.now().strftime("%Y-%m-%d")
        df = df.drop(['ip', 'device_id'], axis=1)

        return df
    except Exception as e:
        log.error("Error occured in 'transform' function:%s ",e)
        raise

def load_message(transform_df,conn):
    """
    Input: Pandas Dataframe
    Output: 
    This method is used to load messages into
    postgres table 'user_logins'
    """
    try:
        log.info("loading df into table")  
        transform_df.to_sql('user_logins', conn, if_exists='append', index=False)
        log.info("data loaded into table successfully")  
    except Exception as e:
        log.error("Error occured in 'load_message' function:%s ",e)
        raise

def retrieve_messages(conn):
    """
    Input: db connection
    Output: Dataframe
    This method is used to retreive messages from
    postgres table 'user_logins'
    """
    try:
        log.info("establishing the connection")
        
        query = "select * from user_logins"
        df = pd.read_sql(query,conn)
        log.info("retrieved data before decoding:%s", df)

        # decode columns to retrieve original valus
        df['ip'] = df.masked_ip.apply(base64.b64decode).str.decode('utf-8')
        df['device_id'] = df.masked_device_id.apply(base64.b64decode).str.decode('utf-8')
        
        log.info("retrieved data after decoding:%s", df)
        return df

    except Exception as e:
        log.error("Error occured in 'retrieve_messages' function:%s ",e)
        raise

if __name__ == '__main__':
    try:
        log.info("establishing the connection")
        connection_string = base64.b64decode(os.getenv("connection_string")).decode('utf-8')
        conn = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres').connect()
        log.info("DB connection established")
        # call extract method to retrieve messages
        received_messages = extract()
        # perform validations - if no messages received then skip the transformation and loading
        if received_messages:
            # perform transformation on recieved messages
            transform_df = transform(received_messages)
            # load data to db
            output = load_message(transform_df,conn)
        else:
            log.info("No messages to transform")
        
        # Additional function to retrieve original values for masked columns
        df = retrieve_messages(conn)
    except Exception as e:
        log.error("Error occured in main function:%s ",e)
    finally:
        log.info("closing connection")
        conn.close()
import boto3
import simplejson as json
import pandas as pd
import sqlalchemy
import batchproc as bp
import time

# Redshift Credentials

database = {
    'drivername': 'postgresql+psycopg2',
    'host': 'host',
    'username': 'rs_un',
    'password': 'rs_pw',
    'port': '5439',
    'database': 'analytics',
    }

# Create Redshift Connection

con = sqlalchemy.create_engine(sqlalchemy.engine.url.URL(**database))

# Connect to S3 via boto & get obejcts

session = boto3.Session(profile_name='leaflink')
s3 = session.resource('s3')
bucket = s3.Bucket('leafliink-data-interview-exercise')
objects = [obj.key for obj in bucket.objects.all() if len(obj.key) > 3]


# Parse Objects & write to table

def extract_and_load_data(obj):

    file = s3.Object('leafliink-data-interview-exercise', obj)
    split_json = file.get()['Body'].read().decode('utf-8').splitlines()
    data = [pd.json_normalize(json.loads(i)) for i in split_json]
    all_data = pd.concat(data)

    all_data.to_sql(
        'table_name',
        schema='table_schema',
        con=con,
        chunksize=10000,
        method='multi',
        if_exists='append',
        index=False,
        )

    return obj + ' loaded at {}'.format(str(time.time()))


if __name__ == '__main__':

    # Generate process to extract & load the data & write progress to a log file.
    log_filename = 'batch_process.log'
    processor = bp.BatchProcessor(objects, extract_and_load_data,
                                  log_filename=log_filename)

    # apply our data-processing function, extract & load data from the s3 bucket.
    processor.start()

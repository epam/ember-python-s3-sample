import boto3
import botocore
import os
import pandas as pd
from io import BytesIO, StringIO
import argparse
import gzip
import json
from datetime import datetime
import pytz

def get_s3_client():
    # keys can be set as environment variables or hardcoded here
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    return s3_client

def get_last_timestamp(filename):
    filename = os.path.basename(filename).rsplit('.', 2)[0]
    date, time, _ = filename.split('_')
    year, month, day = map(int, date.split('-'))
    hour, minute, second = map(int, time.split('-'))
    dt = datetime(year, month, day, hour, minute, second)
    return int(dt.replace(tzinfo=pytz.utc).timestamp())

def get_s3_keys(bucket, prefix, start_time):
    client = get_s3_client()
    keys = []
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            # not necessary, but speeds up execution
            if get_last_timestamp(key) < start_time:
                continue # we make use of the key format to avoid downloading too many objects https://ember.deltixlab.com/docs/dw/s3/#batches--objects
            keys.append(key)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return keys

def retrieve_objects(bucket, keys, start_time, end_time):
    start_time = pd.to_datetime(start_time, unit='s')
    end_time = pd.to_datetime(end_time, unit='s')
    client = get_s3_client()

    # clearing content of the output file
    with open('output.csv', 'w') as f_output:
        pass

    with open('output.csv', 'a') as f_output:
        for key in keys:
            try:
                response = client.get_object(Bucket=bucket, Key=key)
                data = StringIO(gzip.open(BytesIO(response['Body'].read()), 'rt').read())
                df = pd.read_json(data, lines=True)
                if df['Timestamp'].iloc[-1] < start_time:
                    continue
                elif df['Timestamp'].iloc[0] > end_time:
                    break
                # filtering out TRADES only
                df = df[df['Type'] == "OrderTradeReportEvent"]
                df = df[(df['Timestamp'] >= start_time) & (df['Timestamp'] <= end_time)]
                # additional processing may be applied here using pandas, e.g. drop columns, change date format
                # for example   df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                # or            df['Timestamp'] = df['Timestamp'].dt.strftime('%d-%m-%Y')
                if not df.empty:
                    df.to_csv(f_output, header=f_output.tell() == 0, index=False)
            except botocore.exceptions.ClientError as e:
                print(f"Failed to read data from {key}: {e}")
            except KeyError as e:
                print(f"Failed to extract timestamp from line {line} in {key}: {e}")
            except ValueError:
                print(f"Failed to parse JSON from {key}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_time", type=int, help="the start time in Unix time format in seconds")
    parser.add_argument("--end_time", type=int, help="the end time in Unix time format in seconds")
    parser.add_argument("--bucket", type=str, help="the name of the S3 bucket")
    parser.add_argument("--prefix", type=str, help="the prefix of the files in the S3 bucket")
    args = parser.parse_args()

    if not args.bucket:
        print("Please provide the name of the S3 bucket")
        return
    if not args.prefix:
        print("Please provide the prefix of the files")
        return
    # bucket name and prefix are obtained from command line argument
    bucket = args.bucket
    prefix = args.prefix

    if args.start_time and args.end_time:
        start_time = args.start_time
        end_time = args.end_time
        if start_time >= end_time:
            print('Error: End time must be greater than start time')
            return
        # search for keys matching the specified prefix
        keys = get_s3_keys(bucket, prefix, start_time)
        retrieve_objects(bucket, keys, start_time, end_time)
    else:
        print('Please provide both --start_time and --end_time as Unix timestamps')

if __name__ == "__main__":
    main()

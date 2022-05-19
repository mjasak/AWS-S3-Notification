import numpy as np
import nasdaqdatalink as nasdaq
import pandas as pd
import zipfile
import os
from api_key import api_key, AWS_ID
import boto3
import logging
from botocore.exceptions import ClientError
import json

down = False

filepath = 'bigmacAll.csv'
filename = 'data.csv'

def download_csv(path):
    nasdaq.ApiConfig.api_key = api_key
    df = pd.DataFrame()
    df = nasdaq.get('ECONOMIST/BIGMAC_ROU')
    df.reset_index(inplace=True)

    nasdaq.bulkdownload("ECONOMIST")

    if not os.path.exists("economist_dir"):
        os.mkdir("economist_dir")
    with zipfile.ZipFile('ECONOMIST.zip', 'r') as zip_ref:
        zip_ref.extractall("./economist_dir")

    names = ['code'] + list(df.columns)

    filepath = "./economist_dir/" + os.listdir('./economist_dir')[0]
    filepath
    df = pd.read_csv(filepath, names=names)

    df.to_csv(path) 

if down:
    download_csv(filepath)


bucket_name = "onwelo-mj-task-s3-bucket"
region = "us-east-1"
topic_name = 'onwelo-mj-s3-event-notification'


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# 0. Create s3 boto client connection
s3_client = boto3.client('s3')

# 1. Create bucket

res = s3_client.list_buckets()
buckets = [b['Name'] for b in res['Buckets']]

if bucket_name not in buckets:
    result = create_bucket(bucket_name)
    print("Creating bucket: ", result)
else:
    print("Bucket already existing")

# 1.1 Block public access to s3 bucket

response_public = s3_client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        },
    )
    
print("Blocked public access for a bucket")
bucket_arn = "arn:aws:s3:::" + bucket_name
sns_arn = "arn:aws:sns:" + region +":" + AWS_ID + ":" + topic_name
print("Bucket ARN: ", bucket_arn)
print("SNS Topic ARN: ", sns_arn)
# 2. Create SNS Topic

sns_client = boto3.client('sns')

policy = {"Version": "2012-10-17","Id": "example-ID",
"Statement": [{
    "Sid": "example-statement-ID","Effect": "Allow",
    "Principal": {
        "AWS": "*"},
    "Action": "SNS:Publish",
    "Resource": sns_arn,
    "Condition": {
        "StringEquals": {
            "aws:SourceAccount": AWS_ID},
        "ArnLike": {
            "aws:SourceArn": bucket_arn
}}}]}

print("Updated policy for topic")

response = sns_client.create_topic(
    Name=topic_name,
    Attributes={
        'Policy': json.dumps(policy), 
    },
)
print("Created SNS topic")

# 2.1 Create Subscription

sns_client.subscribe(
    TopicArn=sns_arn,
    Protocol='email',
    Endpoint='jasakmichal@gmail.com'
)

sns_client.subscribe(
    TopicArn=sns_arn,
    Protocol='email',
    Endpoint='kamila.fidziukiewicz@onwelo.com'
)

print("Added subscription")
# 3. Create event notification in S3 bucket

response = s3_client.put_bucket_notification_configuration(
    Bucket=bucket_name,
    NotificationConfiguration={
        'TopicConfigurations': [
            {
                'TopicArn': sns_arn,
                'Events': [
                    's3:ObjectCreated:*',
                ],

            },
        ],

    },
    SkipDestinationValidation=True
)

print("Created event notification")
print("Subscription confirmed? (Y/n)")
conf = input()
if conf.upper() == 'Y':
    # 4 Upload file into S3
    s3_client.upload_file(filepath, bucket_name, filename) 
    print("File uploaded")
else:
    pass

s3_client.upload_file(filepath, bucket_name, filename) 
print("File uploaded")

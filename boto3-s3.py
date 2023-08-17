import logging
import boto3
from botocore.exceptions import ClientError
import os

# Let's use Amazon S3
s3 = boto3.resource('s3')

def create_bucket(bucket_name, region=None):
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
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            pass
        else:
            logging.error(e)
            return False
    return True

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# Specify the bucket name
bucket_name = 'de-study-group'

# Create the S3 bucket if it doesn't exist
if create_bucket(bucket_name):
    print(f"S3 bucket '{bucket_name}' created successfully.")
else:
    print(f"S3 bucket '{bucket_name}' already exists.")

# Upload the file to the S3 bucket
file_path = 'olist_order_items_dataset.csv'
if upload_file(file_path, bucket_name, 'olist_order_items_dataset.csv'):
    print("File uploaded to S3 successfully.")
else:
    print("File upload to S3 failed.")

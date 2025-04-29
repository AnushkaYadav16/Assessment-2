import logging
import boto3
from botocore.exceptions import ClientError
import random
import string

s3=boto3.client('s3')

bucket_name = "objectswithtagsandmetadata"
num_objects = 2000
prefix = 'sample-data/'

def create_bucket(bucket_name, region):
    """Creates an S3 bucket if it doesn't already exist."""
    try:
        s3.head_bucket(Bucket=bucket_name)  
        print(f"Bucket '{bucket_name}' already exists.")
        upload_to_s3()
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket '{bucket_name}' does not exist. Creating now...")            
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region} 
            )
            print(f"Bucket '{bucket_name}' created.")
            upload_to_s3()
        else:
            print(f"Error checking or creating bucket: {e}")
            raise e
        
def generate_random_content(size=1024):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))


def upload_to_s3():
    # Upload objects to S3
    for i in range(num_objects):
        object_key = f"{prefix}object_{i}.txt"  # Unique key for each object
        content = generate_random_content(size=1024)  # 1 KB of random content
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=content)
        print(f"Uploaded {object_key}")

    print(f"Successfully uploaded {num_objects} objects to the bucket '{bucket_name}'.")

create_bucket(bucket_name,'ap-south-1')
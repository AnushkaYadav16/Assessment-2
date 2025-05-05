import boto3
from botocore.exceptions import ClientError
import random
import string
import argparse


s3 = boto3.client('s3')

bucket_name = None
# num_objects = 100
num_objects = random.randint(200,250)
prefix = 'sample-data/'

def create_bucket(bucket_name, region):
    """Creates an S3 bucket if it doesn't already exist."""
    try:
        s3.head_bucket(Bucket=bucket_name)  
        print(f"Bucket '{bucket_name}' already exists.")
        delete_objects_from_bucket() 
        upload_to_s3()
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket '{bucket_name}' does not exist. Creating now...")            
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region} 
            )
            upload_to_s3()
        else:
            print(f"Error checking or creating bucket: {e}")
            raise e

def generate_random_content(size=1024):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def generate_random_metadata():
    authors = ["J.K. Rowling", "George Orwell", "Isaac Asimov", "Margaret Atwood", "William Shakespeare"]
    genres = ["Fantasy", "Science Fiction", "Biography", "Historical Fiction", "Romance"]
    editions = ["First Edition", "Revised Edition", "Digital Edition", "Special Edition", "Paperback Edition"]
    year = str(random.randint(1990, 2023))
    lang = ["English", "Spanish", "French", "German", "Italian"]
    rate = str(round(random.uniform(1, 5), 1))  
    
    metadata = {
        "author": random.choice(authors),
        "genre": random.choice(genres),
        "edition": random.choice(editions),
        "publish_year": year, 
        "language": random.choice(lang),
        "rating": rate  
    }
    return metadata

def generate_random_tags():
    categories = ["Novel", "Research Paper", "Short Story", "Essay", "Anthology"]
    status = ["Published", "Draft", "Under Review", "Archived"]
    regions = ["US", "Europe", "Asia", "Africa", "Australia"]
    levels = ["Public", "Private", "Restricted"]
    projects = ["Project_A", "Project_B", "Project_C", "Project_D"]
    
    tags = {
        "category": random.choice(categories),
        "status": random.choice(status),
        "region": random.choice(regions),
        "access_level": random.choice(levels),
        "project": random.choice(projects)
    }
    return tags

def delete_objects_from_bucket():
    """Deletes all objects from the S3 bucket."""
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name)
        
        print("Checking for objects to be deleted...")
        if 'Contents' in objects:
            for obj in objects['Contents']:
                object_key = obj['Key']
                s3.delete_object(Bucket=bucket_name, Key=object_key)
        
            print(f"All objects deleted from bucket '{bucket_name}'.")
        else:
            print(f"No objects found to delete in bucket '{bucket_name}'.")
    
    except ClientError as e:
        print(f"Error deleting objects: {e}")
        raise e

def upload_to_s3():
    print("Uploading objects to bucket...")
    for i in range(num_objects):
        object_key = f"object_{i}.txt"  
        content = generate_random_content(size=1024)  
        
        metadata = generate_random_metadata()
        tags = generate_random_tags()
        
        s3.put_object(
            Bucket=bucket_name, 
            Key=object_key, 
            Body=content,
            Metadata=metadata
        )
        
        tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
        s3.put_object_tagging(
            Bucket=bucket_name,
            Key=object_key,
            Tagging={'TagSet': tag_set}
        )

        
    print(f"Successfully uploaded {num_objects} objects to the bucket '{bucket_name}'.")

def parse_multi_value_filters(pairs_list):
    filters = {}
    if pairs_list:
        for item in pairs_list:
            if '=' in item:
                key, values = item.split('=', 1)
                filters[key.strip()] = set(v.strip() for v in values.split(','))
    return filters

def delete_objects_by_condition(tag_filters, metadata_filters):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)

    # print(f"Searching for objects matching tags {tag_filters} and metadata {metadata_filters}...")

    deleted_count = 0
    for page in page_iterator:
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']

            try:
                tag_response = s3.get_object_tagging(Bucket=bucket_name, Key=key)
                # print("tag: ", tag_response['TagSet'])
                tags = {tag['Key']: tag['Value'] for tag in tag_response['TagSet']}
            except ClientError as e:
                print(f"Error retrieving tags for {key}: {e}")
                continue

            try:
                head = s3.head_object(Bucket=bucket_name, Key=key)
                # print("metadata: ",head['Metadata'])
                metadata = head['Metadata']
            except ClientError as e:
                print(f"Error retrieving metadata for {key}: {e}")
                continue

            # Match each tag key and value
            tag_match = all(
                key in tags and tags[key] in values
                for key, values in tag_filters.items()
            )

            # Match each metadata key and value
            metadata_match = all(
                key in metadata and metadata[key] in values
                for key, values in metadata_filters.items()
            )
            
            if tag_match and metadata_match:
                print(f"Deleting object: {key},\n Object tag: {tags}, Object metadata: {metadata}")
                s3.delete_object(Bucket=bucket_name, Key=key)
                deleted_count += 1

    print(f"Deleted {deleted_count} matching objects.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete S3 objects based on tags and metadata with multiple values.")
    parser.add_argument('--bucket', required=True, help='S3 bucket name') 
    parser.add_argument('--tags', nargs='*', help='Tag filters: key=val1,val2')
    parser.add_argument('--metadata', nargs='*', help='Metadata filters: key=val1,val2')

    args = parser.parse_args()

    bucket_name = args.bucket
    tag_filters = parse_multi_value_filters(args.tags)
    metadata_filters = parse_multi_value_filters(args.metadata)

    create_bucket(bucket_name, 'ap-south-1')
    delete_objects_by_condition(tag_filters, metadata_filters)





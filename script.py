import boto3
from botocore.exceptions import ClientError
import random
import string

s3 = boto3.client('s3')

bucket_name = "objectswithtagsandmetadata"
num_objects = 200
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
    
    metadata = {
        "author": random.choice(authors),
        "genre": random.choice(genres),
        "edition": random.choice(editions),
        "publish_year": str(random.randint(1990, 2023)), 
        "language": random.choice(["English", "Spanish", "French", "German", "Italian"]),
        "rating": str(round(random.uniform(1, 5), 1))  
    }
    return metadata

def generate_random_tags():
    categories = ["Novel", "Research Paper", "Short Story", "Essay", "Anthology"]
    status = ["Published", "Draft", "Under Review", "Archived"]
    regions = ["US", "Europe", "Asia", "Africa", "Australia"]
    
    tags = {
        "category": random.choice(categories),
        "status": random.choice(status),
        "region": random.choice(regions),
        "access_level": random.choice(["Public", "Private", "Restricted"]),
        "project": random.choice(["Project_A", "Project_B", "Project_C", "Project_D"])
    }
    return tags

def delete_objects_from_bucket():
    """Deletes all objects from the S3 bucket."""
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name)
        
        print("Deleting existing objects...")
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
        object_key = f"{prefix}object_{i}.txt"  
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

create_bucket(bucket_name, 'ap-south-1')

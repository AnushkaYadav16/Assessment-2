import unittest
from unittest.mock import patch
import boto3
from moto import mock_aws
import warnings
import gc
import script

warnings.simplefilter("ignore", ResourceWarning)

@mock_aws
class TestS3OperationsWithMoto(unittest.TestCase):
    """
    This class contains unit tests for S3 operations in the script.py file using Moto
    to mock AWS S3 interactions.
    """

    @classmethod
    def tearDownClass(cls):
        """
        This method is called once after all tests have been completed. Here we manually trigger garbage collection.
        """
        gc.collect()
        # logger.info("Garbage collection completed after all tests.")

    def setUp(self):
        """
        It sets up the mock AWS environment and creates a S3 bucket.
        """
        self.region = 'ap-south-1'
        self.bucket_name = 'test-bucket'
        self.new_bucket = 'new-bucket'
        self.s3_client = boto3.client('s3', region_name=self.region)

        self.s3_client.create_bucket(
            Bucket=self.bucket_name,
            CreateBucketConfiguration={'LocationConstraint': self.region}
        )

        script.s3 = self.s3_client
        # logger.info(f"Test bucket '{self.bucket_name}' created in region {self.region}.")

    @patch('sys.argv', new=['script.py', 'test-bucket', 'ap-south-1'])
    def test_create_bucket_when_bucket_exists(self):
        """
        It checks if the script can handle the case where the bucket is already present
        without throwing an error.
        """
        script.bucket_name = self.bucket_name
        script.region = self.region

        script.create_bucket(self.bucket_name, self.region)

        response = self.s3_client.head_bucket(Bucket=self.bucket_name)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        # logger.info(f"Bucket '{self.bucket_name}' exists and is accessible.")

    @patch('sys.argv', new=['script.py', 'new-bucket', 'ap-south-1'])
    def test_create_bucket_when_bucket_does_not_exist(self):
        """
        This will check if the script creates the bucket successfully when it doesn't
        already exist in the mock S3 service.
        """
        script.bucket_name = self.new_bucket
        script.region = self.region

        script.create_bucket(self.new_bucket, self.region)

        buckets = self.s3_client.list_buckets()['Buckets']
        bucket_names = [b['Name'] for b in buckets]
        self.assertIn(self.new_bucket, bucket_names)
        # logger.info(f"Bucket '{self.new_bucket}' created successfully.")

    @patch('sys.argv', new=['script.py', 'test-bucket', 'ap-south-1'])
    def test_delete_objects_from_bucket(self):
        """
        This test checks if the objects are properly deleted from the bucket
        after invoking the `delete_objects_from_bucket` method from the script.
        """
        self.s3_client.put_object(Bucket=self.bucket_name, Key='object1.txt', Body='data')
        self.s3_client.put_object(Bucket=self.bucket_name, Key='object2.txt', Body='data')

        script.bucket_name = self.bucket_name
        script.region = self.region

        script.delete_objects_from_bucket()

        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        self.assertNotIn('Contents', response)
        # logger.info(f"All objects deleted from bucket '{self.bucket_name}'.")

    def test_parse_multi_value_filters(self):
        """
        This test verifies that the function correctly parses input like
        'key=value1,value2' into a dictionary with sets.
        """
        input_list = ['genre=Fantasy,Science Fiction', 'language=English,French']
        expected_output = {
            'genre': {'Fantasy', 'Science Fiction'},
            'language': {'English', 'French'}
        }

        result = script.parse_multi_value_filters(input_list)
        self.assertEqual(result, expected_output)
        # logger.info("Multi-value filters parsed successfully.")

    @patch('sys.argv', new=['script.py', 'test-bucket', 'ap-south-1'])
    def test_delete_objects_by_condition(self):
        """
        This test ensures that the script deletes the objects that meet the
        specified conditions based on metadata and tags.
        """
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key='object_1.txt',
            Body='data',
            Metadata={'language': 'English'}
        )
        self.s3_client.put_object_tagging(
            Bucket=self.bucket_name,
            Key='object_1.txt',
            Tagging={'TagSet': [{'Key': 'region', 'Value': 'Europe'}]}
        )

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key='object_2.txt',
            Body='data',
            Metadata={'language': 'Spanish'}
        )
        self.s3_client.put_object_tagging(
            Bucket=self.bucket_name,
            Key='object_2.txt',
            Tagging={'TagSet': [{'Key': 'region', 'Value': 'US'}]}
        )

        script.bucket_name = self.bucket_name
        script.region = self.region

        tag_filters = {'region': {'Europe'}}
        metadata_filters = {'language': {'English'}}

        script.delete_objects_by_condition(tag_filters, metadata_filters)

        remaining_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        keys = [obj['Key'] for obj in remaining_objects.get('Contents', [])]

        self.assertNotIn('object_1.txt', keys)
        self.assertIn('object_2.txt', keys)
        # logger.info("Objects deleted by condition (tags and metadata) successfully.")

if __name__ == '__main__':
    unittest.main()

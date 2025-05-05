import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
import script  

class TestS3Operations(unittest.TestCase):

    @patch('script.upload_to_s3')
    @patch('script.delete_objects_from_bucket')
    @patch('script.s3')
    def test_create_bucket_exists(self, mock_s3, mock_delete_objects, mock_upload):
        mock_s3.head_bucket.return_value = {}

        script.create_bucket('test-bucket', 'ap-south-1')

        mock_s3.head_bucket.assert_called_once_with(Bucket='test-bucket')
        mock_delete_objects.assert_called_once()
        mock_upload.assert_called_once()

    @patch('script.upload_to_s3')
    @patch('script.s3')
    def test_create_bucket_does_not_exist(self, mock_s3, mock_upload):
        error_response = {'Error': {'Code': '404'}}
        mock_s3.head_bucket.side_effect = ClientError(error_response, 'head_bucket')

        script.create_bucket('objectswithtagsandmetadata', 'ap-south-1')

        mock_s3.create_bucket.assert_called_once_with(
            Bucket='objectswithtagsandmetadata',
            CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
        )
        mock_upload.assert_called_once()

    @patch('script.s3')
    def test_delete_objects_from_bucket_with_objects(self, mock_s3):
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'object_1.txt'}, {'Key': 'object_2.txt'}]  
        }

        script.delete_objects_from_bucket()

        self.assertEqual(mock_s3.delete_object.call_count, 2)

    def test_parse_multi_value_filters(self):
        input_list = ['genre=Fantasy,Science Fiction', 'language=English,French']
        expected_output = {
            'genre': {'Fantasy', 'Science Fiction'},
            'language': {'English', 'French'}
        }

        result = script.parse_multi_value_filters(input_list)
        self.assertEqual(result, expected_output)

    @patch('script.s3')
    def test_delete_objects_by_condition(self, mock_s3):
        mock_paginator = MagicMock()
        mock_page_iterator = [
            {
                'Contents': [{'Key': 'object_1.txt'}, {'Key': 'object_2.txt'}]  
            }
        ]
        mock_paginator.paginate.return_value = mock_page_iterator
        mock_s3.get_paginator.return_value = mock_paginator

        mock_s3.get_object_tagging.side_effect = [
            {'TagSet': [{'Key': 'region', 'Value': 'Europe'}]},
            {'TagSet': [{'Key': 'region', 'Value': 'US'}]}
        ]
        mock_s3.head_object.side_effect = [
            {'Metadata': {'language': 'English'}},
            {'Metadata': {'language': 'Spanish'}}
        ]

        tag_filters = {'region': {'Europe'}}
        metadata_filters = {'language': {'English'}}

        script.delete_objects_by_condition(tag_filters, metadata_filters)

        mock_s3.delete_object.assert_called_once_with(
            Bucket=script.bucket_name, Key='object_1.txt'  
        )


if __name__ == '__main__':
    unittest.main()

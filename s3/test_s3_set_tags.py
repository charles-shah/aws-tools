from s3_set_tags import set_tag_one_object, set_tags
import unittest
import boto3
import botocore
import os, json
import optparse

from moto import mock_s3
MY_BUCKET = "my_bucket"
MY_PREFIX = "mock_folder"
MY_KEY = "test.html"

@mock_s3
class TestS3Tags(unittest.TestCase):

    def setUp(self) -> None:
        self.tags = {'Key': 'department', 'Value': 'Finance'}
        self.key = MY_KEY
        self.verbose = False

        os.environ['AWS_ACCESS_KEY_ID'] = 'fake_access_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'fake_secret_key'

        self.client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            )
        try:
            s3 = boto3.resource(
                "s3",
                region_name="eu-west-1",
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
                )
            s3.meta.client.head_bucket(Bucket=MY_BUCKET)
        except botocore.exceptions.ClientError:
            pass
        else:
            err = "{bucket} should not exist.".format(bucket=MY_BUCKET)
            raise EnvironmentError(err)
        self.client.create_bucket(Bucket=MY_BUCKET)
        self.client.put_object(Bucket=MY_BUCKET, Key=MY_KEY, Body="Hello World")

    def tearDown(self) -> None:
        s3 = boto3.resource(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            )
        bucket = s3.Bucket(MY_BUCKET)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

    def test_set_tags(self):

        options = optparse.Values()
        options.key = self.key
        options.verbose = False
        options.bucket = MY_BUCKET
        options.tags = json.dumps(self.tags)
        options.client = self.client

        set_tags(options)

    def test_set_tag_one_object(self):

        set_tag_one_object(client=self.client, bucket=MY_BUCKET, key=self.key,
                           verbose=self.verbose, tags_str=json.dumps(self.tags) )

        response = self.client.get_object_tagging(
            Bucket=MY_BUCKET,
            Key=self.key)

            # Read current tags
        current_tagset = response.get('TagSet', [])
        self.assertIn(self.tags, current_tagset)


if __name__ == "__main__":
    unittest.main()
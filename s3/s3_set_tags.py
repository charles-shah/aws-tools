__author__ = 'charles'
'''
    Utility to add Tags to s3 object. This is different than bucket level tag.
    This utility can be used to add tags to existing objects.
    Object level Tags can be used to classify objects in different paths under a category and then apply lifecycle rule etc.
    eg: Delete All files tagged with department 'internal' after 30 days of creation; here 'internal' is a tag for the objects.

    Requires: boto3 version 1.4.4 or higher
    AWS credentials are read from environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
'''

from optparse import OptionParser
from boto3.session import Session
import os, sys, json

def parse_args():
    '''

    '''

    usage = "usage: %prog can be run with the following options"
    parser = OptionParser(usage=usage)

    parser.add_option("-k", "--key", dest="key", default = None, help = "keyname")
    parser.add_option("-b", "--bucket", dest="bucket", default = None, help="bucket name")
    parser.add_option("-f", "--filename", dest="filename", default = None, help = "file containing list of keys")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="print verbose messages")
    parser.add_option("-t", "--tags", dest="tags", default=None, help="Tags json str to be added {'Key':k1, 'Value':v1}")

    (options,args) = parser.parse_args()

    if not options.key and not options.filename:
        print("Expects --key or --filename")
        sys.exit(-1)

    if not options.bucket:
        print("Bucket name required ")
        sys.exit(-1)

    if not options.tags:
        print("Tags json is required")
        sys.exit(-1)

    return (options, args)

def set_tags(options):

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_key or not aws_secret:
        print("AWS credentials not found")
        sys.exit(-1)
    session = Session(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    client = session.client('s3')

    if options.key:
        set_tag_one_object(client=client, bucket=options.bucket, key=options.key,
                           verbose=options.verbose, tags_str=options.tags )
    elif options.filename:
        with open(options.filename, 'r') as f:
            contents = f.readlines()
            for line in contents:
                print(line.strip())
                set_tag_one_object(client=client, bucket=options.bucket, key=line.strip(),
                               verbose=options.verbose, tags_str=options.tags)


def set_tag_one_object(client, bucket, key, verbose, tags_str):
    '''
        Single object tags
    '''
    try:
        response = client.get_object_tagging(
            Bucket=bucket,
            Key=key)

        # Read current tags
        current_tagset = response.get('TagSet', [])


        if verbose:
            print(key, end=",")
            if current_tagset:
                print(" current tags: " + str(current_tagset))

        tags_dict = json.loads(tags_str)
        current_tagset.append(tags_dict)

        response = client.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={
                'TagSet': current_tagset
            }
        )
        if verbose:
            if response.get("ResponseMetadata",{}).get("HTTPStatusCode") == 200:
                print("Success " + key)

    except Exception as e:
        print("Exception " + str(e))


if __name__ == "__main__":

    (options, args) = parse_args()
    set_tags(options)


__author__ = 'charles'
'''
    Utility to remove the delete marker from s3 object.
    This can be used to 'undelete' a file which on a bucket which has versioning enabled. (and hence prev version)

    AWS credentials are read from environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
'''

from optparse import OptionParser
import sys, os
import boto
import boto.s3.prefix
from boto.s3 import deletemarker


def print_or_log(msg, level=None):
    '''
        Print or log to enable easy switch between print and log.
    :param msg: Message to log
    :param level: Log level (info, debug...)
    :return:
    '''

    print msg

def parse_args():
    '''

    '''

    usage = "usage: %prog can be run with the following options"
    parser = OptionParser(usage=usage)

    parser.add_option("-k", "--key", dest="key", default = None, help = "key")
    parser.add_option("-b", "--bucket", dest="bucket", default = None, help="bucket name")
    parser.add_option("-f", "--filename", dest="filename", default = None, help = "file containing list of keys/prefix")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="print verbose messages")

    (options,args) = parser.parse_args()

    if not options.key and not options.filename:
        print_or_log("Expects --key or --filename")
        sys.exit(-1)

    if not options.bucket:
        print_or_log("Bucket name required ")
        sys.exit(-1)

    return (options, args)


def undelete(options):
    '''
    Undelete the files
    :param options:
    :return: None
    '''
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    conn = boto.connect_s3(aws_key, aws_secret)
    if not conn:
        print_or_log("Could not get connection to bucket ")
        sys.exit(1)

    bucket_obj = conn.get_bucket(options.bucket)

    if options.key:
        undelete_one_object(bucket_obj=bucket_obj, key=options.key)
    elif options.filename:
        with open(options.filename, 'r') as f:
            contents = f.readlines()
            for line in contents:
                undelete_one_object(bucket_obj=bucket_obj, key=line.strip())

def undelete_one_object(bucket_obj, key):
    '''
    Undelete individual file and restore prev version.
    :param bucket_obj:
    :param key: the s3 key to undelete
    :return:
    '''
    for version in bucket_obj.list_versions(prefix=key):
        try:
           if isinstance(version, deletemarker.DeleteMarker) and version.is_latest:
               print_or_log(version.name.split('/')[3] + "  " + version.last_modified + "  " + version.version_id)
               print_or_log(bucket_obj.delete_key(version.name, version_id=version.version_id))

        except Exception as e:
           print_or_log("Exception " + str(e))

if __name__ == "__main__":

    (options, args) = parse_args()
    undelete(options)

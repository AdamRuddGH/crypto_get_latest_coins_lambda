"""
aws s3 related scripts
"""

import boto3

def write_to_storage(data, bucket, filename_path):
    """"
    will write data to a storage location
    """
    print("performing putObject")
    client = boto3.client('s3')
    return client.put_object(Body=data, Bucket=bucket, Key=filename_path)
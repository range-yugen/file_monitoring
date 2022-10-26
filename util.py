import boto3
import pandas as pd
import numpy as np
import json
import botocore

def get_boto3_client(env, client_type, access_key, region):
    if env == "prod":
        return boto3.client(client_type, region_name=region)

def read_file(s3, bucket, file_path, env):
    if env == "prod":
        file_obj = s3.get_object(Bucket=bucket, Key=file_path)
        file = pd.read_csv(file_obj["Body"])
    return file

def write_file_to_json(bucket, s3, env, file_path, file, dir_name):
    if env == "prod":
        uploadbytestream = bytes(json.dumps(file, indent=2).encode("UTF-8"))
        s3.put_object(Bucket=bucket, Key=file_path, Body=uploadbytestream)            
# -*- coding: utf-8 -*-

# pip install boto3 pyarrow openpyxl

import os
import boto3
from io import BytesIO
import pandas as pd

class MinIOConnector:
    
    def __init__(self, aws_access_key_id, aws_secret_access_key, endpoint_url):
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._endpoint_url = endpoint_url
        self.client = boto3.client(
            's3', 
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            endpoint_url=self._endpoint_url,
            verify=False
        )

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    @classmethod
    def connect(cls, minio_config):
        aws_access_key_id = minio_config.get('aws_access_key_id')
        aws_secret_access_key = minio_config.get('aws_secret_access_key')
        endpoint_url = minio_config.get('endpoint_url')
        return cls(aws_access_key_id, aws_secret_access_key, endpoint_url)

    def get(self, key, bucket_name):
        response = self.client.get_object(Bucket=bucket_name, Key=key)
        return response

    def get_df(self, key, bucket_name):
        file_ext = os.path.splitext(key)[-1]
        response = self.get(key, bucket_name)
        df = pd.DataFrame()
        if file_ext == '.csv':
            df = pd.read_csv(response.get("Body"), encoding='cp949')
        elif file_ext == '.xlsx':
            df = pd.read_excel(BytesIO(response['Body'].read()), engine='openpyxl')
        elif file_ext == '.parquet':
            df = pd.read_parquet(BytesIO(response['Body'].read()), engine='pyarrow')
        else:
            raise RuntimeError(f"Ivaild File Extension ({file_ext})")
        return df
    
    def upload(self, src_file_path, dst_file_key, bucket_name):
        self.client.upload_file(Filename=src_file_path, Bucket=bucket_name, Key=dst_file_key)
        
    def get_keys(self, prefix, bucket_name):
        objects = self.client.list_objects(Bucket=bucket_name, Prefix=prefix)
        return [obj['Key'] for obj in objects['Contents']]
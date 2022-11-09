# -*- coding: utf-8 -*-

# pip install boto3 pyarrow openpyxl

import os
import gc
import sys
import pandas as pd
import argparse
from tendo import singleton
from minio_connector import MinIOConnector as minio
from minio_config import MinIOConfig

def to_parquet(df, dst_dir, dst_file_name):
    if not os.path.isdir(dst_dir):
        os.makedirs(dst_dir, exist_ok=True)
    dst_path = os.path.join(dst_dir, dst_file_name)
    df.to_parquet(dst_path, engine='pyarrow', compression='gzip', index=False)

def get_weather_byday_df(minio_key_list, bucket_name):
    byday_df = pd.DataFrame()
    for minio_key in minio_key_list:
        with minio.connect(minio_conf_main) as minio_conn:
            df_tmp = minio_conn.get_df(minio_key, bucket_name)
        df_tmp = df_tmp[['일시', '평균기온(°C)', '최저기온(°C)', '최고기온(°C)', '일강수량(mm)', '평균 풍속(m/s)', '평균 상대습도(%)']]
        df_tmp.columns = ['dateTime', 'temp', 'tempMin', 'tempMax', 'precipitation', 'windspeed', 'humidity']
        df_tmp['precipitation'][df_tmp['precipitation'].isnull()] = 0
        df_tmp['dateTime'] = pd.to_datetime(df_tmp['dateTime'])
        byday_df = pd.concat([byday_df, df_tmp])
        del df_tmp
        gc.collect()
    return byday_df
    
def get_bike_byday_df(minio_key_list, bucket_name):
    byday_df = pd.DataFrame()
    for minio_key in minio_key_list:
        with minio.connect(minio_conf_main) as minio_conn:
            df_tmp = minio_conn.get_df(minio_key, bucket_name)
        df_tmp = df_tmp[['rentalDateTime', 'rentalOfficeNum']]
        df_tmp['rentalDateTime'] = df_tmp.astype({'rentalDateTime': 'datetime64[ns]'})
        df_tmp = df_tmp.groupby([pd.Grouper(key='rentalDateTime', freq='1D'), 'rentalOfficeNum']).agg(rentalCount=('rentalOfficeNum', 'count')).reset_index()
        byday_df = pd.concat([byday_df, df_tmp])
        del df_tmp
        gc.collect()
    return byday_df

def main(bucket_name, bike_prefix, weather_prefix, dst_local_dir, dst_local_file_name, src_minio_file_path, dst_minio_file_key):

    with minio.connect(minio_conf_main) as minio_conn:
        bike_key_list = minio_conn.get_keys(bike_prefix, bucket_name)

    bike_byday_df = get_bike_byday_df(bike_key_list, bucket_name)

    with minio.connect(minio_conf_main) as minio_conn:
        weather_key_list = minio_conn.get_keys(weather_prefix, bucket_name)
    weather_byday_df = get_weather_byday_df(weather_key_list, bucket_name)

    df = pd.merge(bike_byday_df, weather_byday_df, left_on='rentalDateTime', right_on='dateTime', how='left')
    del bike_byday_df
    del weather_byday_df
    gc.collect()

    # parquet 파일 로컬에 저장
    to_parquet(df, dst_local_dir, dst_local_file_name)

    with minio.connect(minio_conf_main) as minio_conn:
        minio_conn.upload(src_minio_file_path, dst_minio_file_key, bucket_name)

    del df
    gc.collect()


if __name__ == '__main__':

    try:
        me = singleton.SingleInstance()
    except:
        sys.exit(-1)
        
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_version', type=str, default='v3')
    parser.add_argument('--bucket_name', type=str, default='bike')
    parser.add_argument('--bike_prefix', type=str, default='raw_data/parquet/')
    parser.add_argument('--weather_prefix', type=str, default='raw_weather_data/byday/csv')
    parser.add_argument('--dst_local_dir', type=str, default=None)
    parser.add_argument('--dst_local_file_name', type=str, default=None)
    parser.add_argument('--src_minio_file_path', type=str, default=None)
    parser.add_argument('--dst_minio_file_key', type=str, default=None)
    parser.add_argument('--minio_conf_name', type=str, default='main')
    
    args = parser.parse_args()
    data_version = args.data_version
    bucket_name = args.bucket_name
    bike_prefix = args.bike_prefix
    weather_prefix = args.weather_prefix
    dst_local_dir = args.dst_local_dir
    dst_local_file_name = args.dst_local_file_name
    src_minio_file_path = args.src_minio_file_path
    dst_minio_file_key = args.dst_minio_file_key

    if not dst_local_dir:
        dst_local_dir = './'
    if not dst_local_file_name:
        dst_local_file_name = f'bike_weather_{data_version}.parquet'
    if not src_minio_file_path:
        src_minio_file_path = os.path.join(dst_local_dir, dst_local_file_name)
    if not dst_minio_file_key:
        dst_minio_file_key = f"{data_version}/parquet/bike_weather_{data_version}.parquet"

    minio_conf = MinIOConfig()
    minio_conf_main = getattr(minio_conf, args.minio_conf_name)
    
    main(
        bucket_name=bucket_name,
        bike_prefix=bike_prefix,
        weather_prefix=weather_prefix,
        dst_local_dir=dst_local_dir,
        dst_local_file_name=dst_local_file_name,
        src_minio_file_path=src_minio_file_path,
        dst_minio_file_key=dst_minio_file_key
    )
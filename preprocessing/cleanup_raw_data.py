# -*- coding: utf-8 -*-

# pip install boto3 pyarrow openpyxl

import os
import gc
import pandas as pd
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool
from datetime import datetime
import argparse
from tendo import singleton
from minio_connector import MinIOConnector as minio
from minio_config import MinIOConfig

def parallelize_dataframe(df, func, n_cores=4):
    n_cores = min(len(df), n_cores)
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def to_parquet(df, dst_file_name):
    df.to_parquet(dst_file_name, engine='pyarrow', compression='gzip', index=False)

def is_datetime(date_str):
    try:
        pd_dt = pd.to_datetime(str(date_str).replace("'", "").strip())
        if pd_dt.year >= 2015 and pd_dt.year <= datetime.today().year:
            return True
        else:
            return False
    except ValueError:
        return False

def get_rental_office_name(rental_office_num, valid_year_df):
    return valid_year_df[(valid_year_df['rentalOfficeNum'] == rental_office_num)\
                   | (valid_year_df['rentalOfficeNum'] == str(rental_office_num))].loc[:, 'rentalOfficeName'].iloc[0]

def cleanup_invalid_df(df, valid_year_df):
    df['distance'] = df.apply(lambda x: float(x['usageTime']), axis=1)
    df['usageTime'] = df.apply(lambda x: float(x['returnRackNum']), axis=1)
    df['returnRackNum'] = df.apply(lambda x: str(int(x['returnOfficeName'])), axis=1)
    df['returnOfficeName'] = df.apply(lambda x: str(x['returnOfficeNum']), axis=1)
    df['returnOfficeNum'] = df.apply(lambda x: str(int(x['returnDateTime'])), axis=1) 
    df['returnDateTime'] = pd.to_datetime(df['rentalRackNum'])
    df['rentalRackNum'] = df.apply(lambda x: ''.join([n for n in x['rentalOfficeName'].split(',')[-1] if n.isdigit()]), axis=1)
    df['rentalOfficeName'] = df.apply(lambda x: get_rental_office_name(x['rentalOfficeNum'], valid_year_df), axis=1)
    return df

def convert_invalid_to_valid(year_df):
    if not 'isValid' in year_df.columns:
        year_df['isValid'] = year_df.apply(lambda x: 1 if is_datetime(x['rentalDateTime']) and is_datetime(x['returnDateTime']) else 0, axis=1)
    invalid_year_df = year_df[year_df['isValid'] != 1]
    if len(invalid_year_df) > 0:
        valid_year_df = year_df[year_df['isValid'] == 1]
        del year_df
        gc.collect()
        invalid_year_df = cleanup_invalid_df(invalid_year_df, valid_year_df)
        year_df = pd.concat([valid_year_df, invalid_year_df])
    return year_df

def cleanup_year_df(df):
    df['bikeNo'] = df['bikeNo'].apply(lambda x: str(x).replace("'", "").strip())
    df['rentalDateTime'] = df['rentalDateTime'].apply(lambda x: pd.to_datetime(str(x).replace("'", "").strip()) if is_datetime(x) else str(x).replace("'", "").strip())
    df['returnDateTime'] = df['returnDateTime'].apply(lambda x: pd.to_datetime(str(x).replace("'", "").strip()) if is_datetime(x) else str(x).replace("'", "").strip())
    df['rentalOfficeNum'] = df['rentalOfficeNum'].apply(lambda x: str(int(x)) if str(x).isdigit() else str(x).replace("'", "").strip())
    df['rentalOfficeName'] = df['rentalOfficeName'].apply(lambda x: str(x).replace("'", "").strip())
    df['rentalRackNum'] = df['rentalRackNum'].apply(lambda x: str(int(x)) if str(x).isdigit() else str(x).replace("'", "").strip())
    df['returnOfficeNum'] = df['returnOfficeNum'].apply(lambda x: str(int(x)) if str(x).isdigit() else str(x).replace("'", "").strip())
    df['returnOfficeName'] = df['returnOfficeName'].apply(lambda x: str(x).replace("'", "").strip())
    df['returnRackNum'] = df['returnRackNum'].apply(lambda x: str(int(x)) if str(x).isdigit() else str(x).replace("'", "").strip())
    df['usageTime'] = df['usageTime'].apply(lambda x: float(str(x).replace("'", "").strip()))
    df['distance'] = df['distance'].apply(lambda x: float(str(x).replace("'", "").strip()))
    df['isValid'] = df.apply(lambda x: 1 if is_datetime(x['rentalDateTime']) and is_datetime(x['returnDateTime']) else 0, axis=1)
    return df

def get_year_df(target_year, minio_key_list, bucket_name):
    year_df = pd.DataFrame()
    for minio_key in minio_key_list:
        key_year = ''.join([n for n in os.path.splitext(os.path.basename(minio_key))[0] if n.isdigit()])[:4]
        if key_year == str(target_year):
            with minio.connect(minio_conf_main) as minio_conn:
                year_df_tmp = minio_conn.get_df(minio_key, bucket_name)
            if not year_df_tmp.columns[0].strip() in ["자전거번호", "'자전거번호'"]:
                year_df_tmp = year_df_tmp.T.reset_index().T
            year_columns = ['bikeNo', 'rentalDateTime', 'rentalOfficeNum', 'rentalOfficeName', 'rentalRackNum',
                           'returnDateTime', 'returnOfficeNum', 'returnOfficeName', 'returnRackNum',
                           'usageTime', 'distance']
            year_df_tmp.columns = year_columns
            year_df_tmp = parallelize_dataframe(df=year_df_tmp, func=cleanup_year_df, n_cores=mp.cpu_count())
            year_df = pd.concat([year_df, year_df_tmp])
            del year_df_tmp
            gc.collect()
    return year_df

def main(target_year, bucket_name, prefix_list, dst_local_file_name, src_minio_file_path, dst_minio_file_key):
    
    minio_key_list = list()
    with minio.connect(minio_conf_main) as minio_conn:
        for prefix in prefix_list:
            minio_key_list.extend(minio_conn.get_keys(prefix, bucket_name))

    # minio 저장소에서 csv, xlsx 파일 로드, 기본 전처리 및 year 기준의 parquet 파일 변환
    year_df = get_year_df(target_year, minio_key_list, bucket_name)
    year_df = convert_invalid_to_valid(year_df)
    year_df = year_df.drop(['isValid'], axis=1)
    year_df = year_df.drop_duplicates(keep='first')

    # local에 parquet 파일 저장
    to_parquet(year_df, dst_local_file_name)
    
    # minio 저장소에 파일 parquet 업로드
    with minio.connect(minio_conf_main) as minio_conn:
        minio_conn.upload(src_minio_file_path, dst_minio_file_key, bucket_name)
    
    del year_df
    gc.collect()
    

if __name__ == '__main__':
    
    try:
        me = singleton.SingleInstance()
    except:
        sys.exit(-1)
        
    parser = argparse.ArgumentParser()
    parser.add_argument('--target_year', type=int, default=2021)
    parser.add_argument('--bucket_name', type=str, default='bike')
    parser.add_argument('--prefix_list', nargs='+', default=None)
    parser.add_argument('--dst_local_file_name', type=str, default=None)
    parser.add_argument('--src_minio_file_path', type=str, default=None)
    parser.add_argument('--dst_minio_file_key', type=str, default=None)
    parser.add_argument('--minio_conf_name', type=str, default='main')
    
    args = parser.parse_args()
    target_year = args.target_year
    bucket_name = args.bucket_name
    prefix_list = args.prefix_list
    dst_local_file_name = args.dst_local_file_name
    src_minio_file_path = args.src_minio_file_path
    dst_minio_file_key = args.dst_minio_file_key
    
    if not prefix_list:
        prefix_list = ['raw_data/csv/', 'raw_data/xlsx/']
    if not dst_local_file_name:
        dst_local_file_name = f'raw_bike_{target_year}.parquet'
    if not src_minio_file_path:
        src_minio_file_path = f'./raw_bike_{target_year}.parquet'
    if not dst_minio_file_key:
        dst_minio_file_key = f'raw_data/parquet/raw_bike_{target_year}.parquet'

    minio_conf = MinIOConfig()
    minio_conf_main = getattr(minio_conf, args.minio_conf_name)

    main(
        target_year=target_year,
        bucket_name=bucket_name,
        prefix_list=prefix_list,
        dst_local_file_name=dst_local_file_name,
        src_minio_file_path=src_minio_file_path,
        dst_minio_file_key=dst_minio_file_key
    )
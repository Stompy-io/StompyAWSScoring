import os
import pandas as pd
import math


def approx_bisect(lst, approx_num=5):
    if approx_num == 0:
        approx_num = 1

    res = []
    length = len(lst)
    if length >= approx_num:
        for i in range(approx_num):
            ptr = math.floor(i / approx_num * length)
            ptr_ = math.floor((i + 1) / approx_num * length)
            batch = lst[ptr: ptr_]
            res.append(batch)
    else:
        res.append(lst)

    return res


def listdir_nohidden(path):
    for f in os.listdir(path):
        if not f.startswith('.'):
            yield f


def paginate(method, **kwargs):
    ec2_client = method.__self__
    paginator = ec2_client.get_paginator(method.__name__)
    for page in paginator.paginate(**kwargs).result_key_iters():
        for item in page:
            yield item


def normalize_by_columns(df, columns: []) -> pd.DataFrame:
    for col in columns:
        df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

    return df


class ParquetTranscoder:
    __PARQUET_COLUMN_ENCODE_MAP = {
        '/': '[SLASH]',
        '*': '[ASTERISK]',
        ':': '[COLON]',
        '?': '[QUESTION]',
        '"': '[DOUBLE_QUOTE]',
        '<': '[LEFT_ANGLE_BRACKET]',
        '>': '[RIGHT_ANGLE_BRACKET]',
        '\\': '[BACKSLASH]',
        '|': '[VERTICAL_BAR]',
        ' ': '[SPACE]'
    }

    __PARQUET_COLUMN_DECODE_MAP = dict(zip(__PARQUET_COLUMN_ENCODE_MAP.values(), __PARQUET_COLUMN_ENCODE_MAP.keys()))

    @staticmethod
    def encode(value: str) -> str:
        for k in ParquetTranscoder.__PARQUET_COLUMN_ENCODE_MAP:
            value = value.replace(k, ParquetTranscoder.__PARQUET_COLUMN_ENCODE_MAP[k])
        return value

    @staticmethod
    def decode(dirname: str) -> str:
        for k in ParquetTranscoder.__PARQUET_COLUMN_DECODE_MAP:
            dirname = dirname.replace(k, ParquetTranscoder.__PARQUET_COLUMN_DECODE_MAP[k])
        return dirname

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/27 0027 15:50
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : process_station.py

import os
import re

import pandas as pd
import numpy as np

from public_toolkits import public_function

"""
    处理lazada站点
"""


# 读取单个文件数据(若为excel,则读取单个sheet)
def read_file(file_path: 'full_path', skiprows=5) -> 'pd.DataFrame':
    """
        lazada站点目前只有一个广告报表(xls表)且只有sheet
    """
    # 检测路径有效性
    public_function.detect_file(file_path)
    return pd.read_excel(file_path, skiprows=skiprows)


def strip_space(data):
    """
    站点五表都需要进行初始化
        列名去掉空格
    Args:
        data:pd.DataFrame
            站点数据
    Returns:pd.DataFrame
            初始化后的站点数据

    """
    # 1.删除列名中的空格
    data.columns = [column.strip() for column in data.columns]
    # 2.删除全部数据的空格
    data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return data


def rename_columns(data):
    """
    修改站点列名
    Args:
        report_type:str
            站点类型
        data: pd.DataFrame
            站点数据

    Returns:pd.DataFrame

    """
    # 修改广告报表列名
    # dict of list
    rename_columns_dict = {'Date': ['日期', 'Tanggal', 'Ngày'], 'Product Name': ['商品名称', 'Nama Produk', 'Tên sản phẩm'],
                           'Est. Spend': ['预估花费', 'Estimasi Pengeluaran', 'Phí dự toán'],
                           'Revenue': ['支付金额', 'Pendapatan', 'Doanh thu'], 'Orders': ['订单数', 'Pesanan', 'Đơn hàng'],
                           'Units Sold': ['支付件数', 'Produk Terjual', 'Sản phẩm'],
                           'Est ROI': ['投入产出比', 'Estimasi Tingkat Pengembalian Keuntungan',
                                       'Tỷ suất lợi nhuận ước tính']}
    for standard_col, nonstandard_col_list in rename_columns_dict.items():
        rename_dict = {nonstandard_col: standard_col for nonstandard_col in nonstandard_col_list}
        data.rename(columns=rename_dict, inplace=True)


# 将列转换为数值:整型或是浮点型(保留几位有效数据)
def trans_into_numerical(str_value, type='int', point=2, fill_na=0):
    """
    将字符串型数据转换为整型或是浮点型
    Args:


        str_value:str
            字符串转换为数值型
        type :str  int or float default int
            需要转换成的数据类型:整型或是浮点型
        point:int default 2
                若为浮点型,需要保留的浮点型小数点位数
        fill_na : numerical default 0
            空白的值用0填充
    Returns:int
        转换后的数值

    """
    if not isinstance(str_value, str):
        return str_value
    if type not in ['int', 'float']:
        raise ValueError('trans into numerical TYPE should be INT or FLOAT')
    if not isinstance(point, int):
        raise ValueError('trans into numerical POINT should be INT')
    str_value = str_value.strip()
    if pd.isna(str_value):
        return fill_na
    if str_value == '':
        return fill_na
    if type == 'int':
        return int(str_value)
    else:
        if str_value[-3] in [',', '.']:
            return round(float(re.sub('[,.]', '', str_value)) / 100, point)
        else:
            return round(float(str_value), point)


def trans_columns_type(data):
    """
    修改站点中列的数据类型
    Args:
        data: pd.DataFrame
            站点数据


    Returns:pd.DataFrame

    """
    # lazada广告报表中需要处理的列为:
    # int: Orders,Units Sold
    # float:Est. Spend,Revenue
    int_columns = ['Orders', 'Units Sold']
    for col in int_columns:
        if not isinstance(data[col].values[0], (np.int64, np.int32)):
            data[col] = data[col].apply(lambda x: trans_into_numerical(x))
    float_columns = ['Est. Spend', 'Revenue']
    for col in float_columns:
        if not isinstance(data[col].values[0], (np.float64, np.float32)):
            data[col] = data[col].apply(lambda x: trans_into_numerical(x, type='float', point=2))


def init_file_data(station_name, file_data):
    """
    初始化站点数据:
        1.删除站点中列的空格
        2.重命名
        3.修改某些列的数据类型
    :param station_name: str
        站点名
    :param file_data: pd.DataFrame
        站点数据
    :return: pd.DataFrame
    """
    if not isinstance(file_data, pd.DataFrame):
        raise ImportError(f'{station_name} data type is {type(file_data)}  not pd.DataFrame.')
    if file_data.empty:
        return
    # 1删除空格
    file_data = strip_space(file_data)
    # 2.重命名
    rename_columns(file_data)
    # 修改某些列的数据类型
    trans_columns_type(file_data)
    # 核对表头
    columns = ['Date','Product Name','Seller SKU','SKU ID','Est. Spend','Revenue','Orders','Units Sold','Est ROI']
    file_data = file_data[columns]
    return file_data

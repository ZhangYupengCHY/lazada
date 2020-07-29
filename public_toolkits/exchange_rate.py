#!/usr/bin/env python
# coding=utf-8
# author:marmot


import json
import os
from datetime import datetime, timedelta

import psutil
import requests
import pandas as pd

from public_toolkits import static_param

"""
    从接口获得汇率
"""


# 人民币

def get_exchange_from_api(transed_country='CNY'):
    """
    通过接口请求汇率各个国家对美汇率
     transed_country: str default CNY
        货币需要转换成的国家简称
    若接口不通,则返回固定
    Returns:dict
        包含更新时间键的东南亚国家站点汇率


    """
    # 通过api获取汇率
    if transed_country not in ['CNY', 'USD']:
        raise ImportError(f'转换成汇率只能是"CNY"或是“USD”')

    # 从接口请求
    url = "http://api.currencylayer.com/"
    access_token = 'cbbc201a3e5c4c53084af6acb081a387'

    # 获取实时的汇率(也可以获得历史的等:参考https://currencylayer.com/quickstart)
    live_url = url + 'live?' + 'access_key=' + f'{access_token}'

    try:
        response_connect = requests.get(live_url, timeout=4)
        status_code = response_connect.status_code
    except:
        status_code = 404
    if status_code != 200:
        print('====================================================================')
        print(f'ERROR CURRENT API.CANT CONNECT {url} ')
        print('USE BACK TO CNY CHANGE_CURRENT ')
        exchange_rate = {'ID': 0.000481, 'MY': 1.646287, 'PH': 0.142238, 'SG': 5.07489, 'TH': 0.22187, 'VN': 0.000303,
                         'updatetime': ''}
        print(f'使用的汇率为:{exchange_rate}')
        print('====================================================================')
        return exchange_rate
    response = json.loads(response_connect.text)
    response_time = datetime.fromtimestamp(response['timestamp'])
    response_strtime = datetime.strftime(response_time, '%Y-%m-%d')
    response_rate = response['quotes']
    country_current_dict = static_param.SITE_EXCHANGE
    """
    api请求的国家字典为‘USD'+国家货币缩写
    """
    if transed_country == 'CNY':
        cny_exchange_rate = response_rate['USDCNY']
        exchange_rate = {country_name: round((1 / response_rate['USD' + current_code]) * cny_exchange_rate, 6) for
                         country_name, current_code in
                         country_current_dict.items()}
    else:
        exchange_rate = {country_name: round(1 / response_rate['USD' + current_code], 6) for
                         country_name, current_code in
                         country_current_dict.items()}

    # 添加更新时间字段
    exchange_rate.update({'updatetime': response_strtime})

    return exchange_rate


def get_exchange_from_local():
    """
    从本地文件中获取汇率
    :return: pd.DataFrame
    """
    # 判断存储汇率的文件是否存在
    global exchange_rate_local_path
    exchange_rate_local_path = r"D:\lazada_static\exchange_rate.csv"
    if not os.path.exists(exchange_rate_local_path):
        return

    return pd.read_csv(exchange_rate_local_path)


def change_current(transed_country='CNY') -> dict:
    """
    由于接口获取是有时间限制的(每月250次),于是通过接口一个星期更新一次存放在本地,一个星期更新一次即可.
    首先通过本地的文件件获取汇率

    """

    # 首先从本地获取汇率
    exchange_rate_from_local = get_exchange_from_local()

    if exchange_rate_from_local is not None:
        exchange_rate_from_local_updatetime = exchange_rate_from_local['updatetime'].values[0]
        now_date = datetime.today().date()
        if exchange_rate_from_local_updatetime != '':
            exchange_rate_from_local_updatetime = datetime.strptime(exchange_rate_from_local_updatetime,'%Y-%m-%d').date()
            interval_days = (now_date - exchange_rate_from_local_updatetime).days
            if interval_days < 7:
                # 本地文件中有,且更新时间在7天以内
                return {site: rate.values[0] for site, rate in exchange_rate_from_local.iteritems()}

    # 从api获取汇率
    exchange_rate = get_exchange_from_api(transed_country=transed_country)
    # 将汇率信息存储到本地中
    exchange_rate_df = pd.DataFrame([exchange_rate])
    # 将信息存储到本地的csv中
    if not os.path.exists(os.path.dirname(exchange_rate_local_path)):
        os.mkdir(os.path.dirname(exchange_rate_local_path))
    exchange_rate_df.to_csv(exchange_rate_local_path, index=False)
    return exchange_rate



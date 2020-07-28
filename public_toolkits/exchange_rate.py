#!/usr/bin/env python
# coding=utf-8
# author:marmot


import json
from datetime import datetime, timedelta

import requests

from public_toolkits import static_param

"""
    从接口获得汇率
"""


# 人民币

def change_current(transed_country='CNY'):
    """
    通过接口请求汇率各个国家对美汇率
     transed_country: str default CNY
        货币需要转换成的国家简称

    Returns:


    """
    if transed_country not in ['CNY', 'USD']:
        raise ImportError(f'转换成汇率只能是"CNY"或是“USD”')

    # 从接口请求
    url = "http://api.currencylayer.com/"
    access_token = 'cbbc201a3e5c4c53084af6acb081a387'

    # 获取实时的汇率(也可以获得历史的等:参考https://currencylayer.com/quickstart)
    live_url = url + 'live?' + 'access_key=' + f'{access_token}'

    response_connect = requests.get(live_url)
    status_code = response_connect.status_code
    if status_code != 200:
        print('====================================================================')
        print(f'ERROR CURRENT API.CANT CONNECT {url} ')
        print('USE BACK TO CNY CHANGE_CURRENT ')
        exchange_rate = {'ID': 0.000481, 'MY': 1.646287, 'PH': 0.142238, 'SG': 5.07489, 'TH': 0.22187, 'VN': 0.000303,'updatetime':''}
        print(f'使用的汇率为:{exchange_rate}')
        print('====================================================================')
        return exchange_rate
    response = json.loads(response_connect.text)
    # 获取的时间(美国东部时间间隔14个小时)
    response_time = datetime.fromtimestamp(response['timestamp']) + timedelta(hours=14)
    response_strtime = datetime.strftime(response_time, '%Y-%m-%d %H-%M-%S')
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



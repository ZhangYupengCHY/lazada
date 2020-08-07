#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/27 0027 13:47
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : static_param.py

"""
    固定参数
"""

import os

"""
    处理的国家中英简称
"""
SITE_EN_ZH = {'ID': '印度尼西亚', 'MY': '马来西亚', 'PH': '菲律宾', 'SG': '新加坡', 'TH': '泰国', 'VN': '越南'}

"""
    国家汇率简称
"""
SITE_EXCHANGE = {'ID': 'IDR', 'MY': 'MYR', 'PH': 'PHP', 'SG': 'SGD', 'TH': 'THB', 'VN': 'VND'}

"""
    项目路径
"""
PROJECT_PATH = os.path.dirname(os.getcwd())

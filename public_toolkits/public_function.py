#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/27 0027 15:59
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : public_function.py

import os
import pandas as pd
from datetime import datetime


# 实现队列
class Queue:
    def __init__(self, queue_list=None):
        if queue_list is None:
            self.items = []
        else:
            if isinstance(queue_list, list):
                self.items = queue_list
            else:
                raise TypeError(f'error type of {queue_list},type must be list')

    def enqueue(self, item):
        self.items.append(item)

    def enqueue_list(self, item_list):
        if isinstance(item_list,list):
            self.items.extend(item_list)
        else:
            raise TypeError(f'error type of {item_list},type must be list')

    def dequeue(self):
        return self.items.pop(0)

    def dequeue_many(self, num):
        return [self.items.pop(0) for _ in range(num)]

    def empty(self):
        return self.size() == 0

    def size(self):
        return len(self.items)


def detect_file(file_path):
    """
    检测路径的有效性:
        是否是有效路径
        是否存在
    :param file_path: path
        检测路径
    :return: None
    """
    if not os.path.isfile(file_path):
        raise TypeError(f'{file_path} is not a file.')
    if not os.path.exists(file_path):
        raise FileExistsError(f'{file_path} is not exists.')


def detect_folder(folder_path):
    """
    检测路径的有效性:
        是否是有效路径
        是否存在
    :param folder_path: path
        检测路径
    :return: None
    """
    if not os.path.isdir(folder_path):
        raise TypeError(f'{folder_path} is not a file.')
    if not os.path.exists(folder_path):
        raise FileExistsError(f'{folder_path} is not exists.')


def detect_df(df):
    """
    检测df的有效性
    :param df: pd.DataFrame
    :return:
    """
    if df is None:
        raise TypeError(f'import is None.')
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f'import type must be pd.DataFrame not {type(df)}.')


def run_time(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        cost_time = end_time - start_time
        print(f'{func.__name__}花费:{cost_time}')
        return result

    return wrapper

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/27 0027 15:59
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : public_function.py

import os


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

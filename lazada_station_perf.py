#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/27 0027 13:37
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : lazada_station_perf.py

import os
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

from public_toolkits import public_function
from public_toolkits import process_station
from public_toolkits import static_param
from public_toolkits import exchange_rate
from public_toolkits import write_excel_beauty
import search_sku

"""
lazada运营站点大致表现:

    1.站点层级：
        按照站点广告数据来汇总各个站点:账号/站点/时间/花费/销售额

    2.sku层级:
        有订单sku:
            按照sku来汇总:账号/站点/日期/sku/花费/销售/订单数/支付件数
        无订单sku:
            按照sku来汇总:账号/站点/日期/sku/花费/销售/订单数/支付件数


    获取存放lazada站点全部数据的文件夹层级为:
            全部站点的文件夹：
                --站点：
                    --账号站点：
                        --广告文件
"""
THREAD_POOL = ThreadPoolExecutor(4)


def load_station_name_corresponding(path):
    """
    加载广告店铺与erp店铺关系表,以erpsku的店铺命名为标准
    :param path:
    :return:
    """
    public_function.detect_file(path)
    return pd.read_excel(path)


def get_a_station_perf(station_name, station_data, perf_keep_point=2):
    """
    得到一个站点广告的表现
    :param station_name:str
         站点名
    :param station_data:pd.DataFrame
         站点数据
    :param perf_keep_point:int default 2
         表现保存的小数点
    :return:pd.DataFrame
        站点表现
    """
    # 国家
    station_name = station_name.upper()
    account = station_name[:-3]
    site = station_name[-2:]
    if site not in static_param.SITE_EN_ZH.keys():
        raise messagebox.showinfo(f'{site} error', f'{station_name} 中{site}不是有效国家名,请检查后再次运行程序.')
    # 读取站点数据
    if station_data is None:
        return
    if station_data.empty:
        return
    # 汇总站点表现
    spend_columns_name = 'Est. Spend'
    sales_columns_name = 'Revenue'
    station_perf = station_data.groupby(['Date']).agg(
        {spend_columns_name: 'sum', sales_columns_name: 'sum'}).reset_index()
    # 将汇率转换为人民币
    station_perf[spend_columns_name] = station_perf[spend_columns_name].apply(
        lambda x: round(site_exchange_rate[site] * x, perf_keep_point))
    station_perf[sales_columns_name] = station_perf[sales_columns_name].apply(
        lambda x: round(site_exchange_rate[site] * x, perf_keep_point))
    # 添加账号站点列
    station_perf['account'] = account
    station_perf['site'] = site
    station_perf['ROI'] = [round(sales / spend, perf_keep_point) if spend > 0 else 0 for spend, sales in
                           zip(station_perf[spend_columns_name], station_perf[sales_columns_name])]
    # 调整处理列
    export_columns = ['Date', 'account', 'site', spend_columns_name, sales_columns_name, 'ROI']
    station_perf = station_perf.reindex(columns=export_columns)
    return station_perf


def add_erpsku_num_one_thread():
    if request_sku_queue.empty():
        return
    if request_sku_queue.size() >= 1000:
        query_search_sku_list = request_sku_queue.dequeue_many(1000)
    else:
        query_search_sku_list = request_sku_queue.dequeue_many(request_sku_queue.size())
    query_search_sku_str = ','.join(query_search_sku_list)
    try:
        response_result = search_sku.query_lazada_sku(query_search_sku_str, 'seller_sku')
        # 将每次请求结果存储到队列中
        if not response_result.empty:
            response_sku_queue.enqueue(response_result)
    except Exception as e:
        print(e)


def add_sellersku_num_one_thread():
    if request_erpsku_queue.empty():
        return
    if request_erpsku_queue.size() >= 1000:
        request_erpsku_list = request_erpsku_queue.dequeue_many(1000)
    else:
        request_erpsku_list = request_erpsku_queue.dequeue_many(request_erpsku_queue.size())
    request_erpsku_list = [str(erpsku) for erpsku in request_erpsku_list]
    query_search_erpsku_str = ','.join(request_erpsku_list)
    try:
        response_result = search_sku.query_lazada_sku(query_search_erpsku_str, 'erp_sku')
        # 将每次请求结果存储到队列中
        if not response_result.empty:
            response_erpsku_queue.enqueue(response_result)
    except Exception as e:
        print(e)


# 多线程请求
def thread_query():
    while 1:
        all_task = []
        for _ in range(4):
            all_task.append(THREAD_POOL.submit(add_erpsku_num_one_thread))
        for future in as_completed(all_task):
            future.result()
        if request_sku_queue.empty():
            break


# 多线程请求
def thread_query_seller_sku():
    while 1:
        all_task = []
        for _ in range(4):
            all_task.append(THREAD_POOL.submit(add_sellersku_num_one_thread))
        for future in as_completed(all_task):
            future.result()
        if request_sku_queue.empty():
            break


def add_erpsku_column(df, limit_num=1000):
    """
    报表中添加erpsku列
    用4个线程去请求
    通过接口,请求erpsku
    :param df:
    :return:
    """
    global request_sku_queue, response_sku_queue
    public_function.detect_df(df)
    # 采用4线程去请求
    # 需要查询的sku列表
    request_sku = list(set(df['Seller SKU']))
    request_sku_queue = public_function.Queue(request_sku)
    # 返回结果列表
    response_sku_queue = public_function.Queue()
    # 多线程请求
    thread_query()
    # 请求结果
    response_sku_result = response_sku_queue.items
    if not response_sku_result:
        return
    response_sku_result = pd.concat(response_sku_result)
    # seller-erpsku字典
    sellersku_erpsku_dict = {sellsku: erpsku for sellsku, erpsku in
                             zip(response_sku_result['seller_sku'], response_sku_result['sku'])}
    df['erpsku'] = [sellersku_erpsku_dict.get(seller_sku, '') for seller_sku in df['Seller SKU']]


def add_sellersku_column(df, limit_num=1000):
    """
    报表中添加sellersku列
    用4个线程去请求
    通过接口,请求sellersku
    :param df:
    :return:
    """
    global request_erpsku_queue, response_erpsku_queue
    public_function.detect_df(df)
    # 采用4线程去请求
    # 需要查询的sku列表
    request_sku = list(set(df['sku']))
    request_erpsku_queue = public_function.Queue(request_sku)
    # 返回结果列表
    response_erpsku_queue = public_function.Queue()
    # 多线程请求
    thread_query_seller_sku()
    # 请求结果
    response_erpsku = response_erpsku_queue.items
    if not response_erpsku:
        return
    response_erpsku_df = pd.concat(response_erpsku)
    # seller-erpsku字典
    response_erpsku_df['seller_name'] = response_erpsku_df['seller_name'].apply(
        lambda x: x.upper() if x is not None else None)

    response_erpsku_df = pd.merge(df, response_erpsku_df, left_on=['账号', 'sku'], right_on=['seller_name', 'sku'],
                                  how='left')
    response_erpsku_df.drop_duplicates(inplace=True)
    return response_erpsku_df


def station_perf_by_ordered_sku(station_name, station_data, perf_keep_point=2, have_ordered=True):
    """
    获取站点广告有订单sku的表现
    have_ordered:bool default True
        True:有点订单
        False:没有订单
    """
    # 国家
    station_name = station_name.upper()
    account = station_name[:-3]
    site = station_name[-2:]
    if site not in static_param.SITE_EN_ZH.keys():
        raise messagebox.showinfo(f'{site} error',
                                  f'{station_name} 中{site}不是有效国家名,请检查后再次运行程序.')
    if station_data.empty:
        return
    # 初始化站点数据
    station_data = process_station.init_file_data(station_name, station_data)
    # 汇总站点表现
    spend_columns_name = 'Est. Spend'
    sales_columns_name = 'Revenue'
    sku_columns_name = 'Seller SKU'
    order_columns_name = 'Orders'
    units_columns_name = 'Units Sold'
    # 有订单的数据
    if have_ordered:
        station_have_ordered_data = station_data[station_data['Orders'] != 0]
        ordered_sku_perf = station_have_ordered_data.groupby(['Date', sku_columns_name]).agg(
            {spend_columns_name: 'sum', sales_columns_name: 'sum', order_columns_name: 'sum',
             units_columns_name: 'sum'}).reset_index()
    else:
        station_have_ordered_data = station_data[station_data['Orders'] == 0]
        ordered_sku_perf = station_have_ordered_data.groupby(['Date', sku_columns_name]).agg(
            {spend_columns_name: 'sum', sales_columns_name: 'sum', order_columns_name: 'count'}).reset_index()
    if station_have_ordered_data.empty:
        return

    # 将汇率转换为人民币
    ordered_sku_perf[spend_columns_name] = ordered_sku_perf[spend_columns_name].apply(
        lambda x: round(site_exchange_rate[site] * x, perf_keep_point))
    ordered_sku_perf[sales_columns_name] = ordered_sku_perf[sales_columns_name].apply(
        lambda x: round(site_exchange_rate[site] * x, perf_keep_point))
    # 添加账号站点列
    ordered_sku_perf['account'] = account
    ordered_sku_perf['site'] = site
    ordered_sku_perf['ROI'] = [round(sales / spend, perf_keep_point) if spend > 0 else 0 for spend, sales in
                               zip(ordered_sku_perf[spend_columns_name], ordered_sku_perf[sales_columns_name])]
    # 将值按照sku的ROI排序
    ordered_sku_perf.sort_values(by=['Date', 'ROI', sales_columns_name], ascending=[True, False, True], inplace=True)
    # 调整处理列
    if have_ordered:
        export_columns = ['Date', 'account', 'site', sku_columns_name, spend_columns_name, sales_columns_name,
                          order_columns_name, units_columns_name, 'ROI']
    else:
        ordered_sku_perf.rename(columns={order_columns_name: '次数'}, inplace=True)
        export_columns = ['Date', 'account', 'site', sku_columns_name, '次数']
    ordered_sku_perf = ordered_sku_perf.reindex(columns=export_columns)
    return ordered_sku_perf


# 检测站点是否存放在有效的文件夹下
def file_in_right_folder(seller_sku, station) -> bool:
    """
    判断站点数据是否被正确存放在正确的站点
    :param seller_sku: str
        查询seller_sku
    :param station:str
        匹配的erpsku station
    :return:bool
    """
    erpsku_info = search_sku.query_lazada_sku(seller_sku, 'seller_sku')
    if erpsku_info.empty:
        return 'empty'
    erpsku_station_set = set([station.upper() for station in erpsku_info['seller_name'].values])
    if station.upper() not in erpsku_station_set:
        return 'wrong'
    else:
        return 'right'


def get_all_station_perf(all_stations_folder):
    """
    全部站点表现: 账号/站点/花费/销售额
    all_stations_folder: path
        全部站点数据文件夹
    :return:list
        站点总体表现pd.DataFrame,有订单的sku表现,无订单sku表现
    """
    global empty_file_stations
    # 输入类型检测:检测路径是否为有效路径
    public_function.detect_folder(all_stations_folder)
    # 得到需要处理全部站点列表
    all_stations_dir = []
    for root, dirs, files in os.walk(all_stations_folder):
        if root != all_stations_folder:
            for name in dirs:
                station_path = os.path.join(root, name)
                all_stations_dir.append(station_path)

    # 若文件夹中没有文件,则返回
    if not all_stations_dir:
        print(f'{all_stations_folder}中没有站点,请检测存放站点的文件夹层级是否正确.')
        return

    all_stations_perf = []
    all_stations_sku_have_ordered_perf = []
    all_stations_sku_no_ordered_perf = []
    # 循环每个站点,得到每个站点的表现
    # 重复文件站点
    multiple_files_stations = []
    # 错误表头站点
    error_column_name_stations = []
    # 空文件站点
    empty_file_stations = []
    # 文件存放在错误的站点
    file_in_wrong_folder = []
    for station_dir in all_stations_dir:
        station_files = os.listdir(station_dir)
        station_name = os.path.basename(station_dir).upper()
        if not station_files:
            empty_file_stations.append(station_name)
            continue
        elif len(station_files) > 1:
            multiple_files_stations.append(station_name)
            continue
        # 站点数据
        file_path = os.path.join(station_dir, station_files[0])
        try:
            station_data = process_station.read_file(file_path)
            station_data = process_station.init_file_data(station_name, station_data)
        except:
            error_column_name_stations.append(station_name)
            continue
        if station_data is None:
            continue
        if station_data.empty:
            continue

        # 判断站点是否存在在正确的文件夹下
        # lazada_account = station_name[:-3]
        # lazada_site = station_name[-2:]
        # erp_account = station_name_corresponding_dict.get(lazada_account, None)
        # if erp_account is None:
        #     continue
        # detect_seller_sku = station_data['Seller SKU'].values[0]
        # erp_station = erp_account + '-' + lazada_site
        # if file_in_right_folder(detect_seller_sku, erp_station) == 'wrong':
        #     file_in_wrong_folder.append(station_name)
        #     continue
        #     # 若第一次查询seller_sku返回值为空,则进行再次检测一个seller_sku
        # elif file_in_right_folder(detect_seller_sku, erp_station) == 'empty':
        #     if len(station_data) > 1:
        #         detect_seller_sku_second = station_data['Seller SKU'].values[1]
        #         if file_in_right_folder(detect_seller_sku_second, erp_station) == 'wrong':
        #             file_in_wrong_folder.append(station_name)
        #             continue
        #     else:
        #         file_in_wrong_folder.append(station_name)
        #         continue

        # 站点整体表现
        station_perf = get_a_station_perf(station_name, station_data)
        # 有订单sku表现
        sku_have_ordered_perf = station_perf_by_ordered_sku(station_name, station_data, have_ordered=True)
        # 没有订单sku表现
        sku_no_ordered_perf = station_perf_by_ordered_sku(station_name, station_data, have_ordered=False)
        if station_perf is not None:
            all_stations_perf.append(station_perf)
        if sku_have_ordered_perf is not None:
            all_stations_sku_have_ordered_perf.append(sku_have_ordered_perf)
        if sku_no_ordered_perf is not None:
            all_stations_sku_no_ordered_perf.append(sku_no_ordered_perf)

    # 重复文件提示
    if multiple_files_stations:
        lab3.insert('insert', '\n')
        lab3.insert('insert', '文件夹中存在多个文件:')
        lab3.insert('insert', '\n')
        for file in multiple_files_stations:
            lab3.insert('insert', file)
            lab3.insert('insert', '\n')
        lab3.update()

    # 表头错误提示
    if error_column_name_stations:
        lab3.insert('insert', '\n')
        lab3.insert('insert', '文件中表头错误:')
        lab3.insert('insert', '\n')
        for file in error_column_name_stations:
            lab3.insert('insert', file)
            lab3.insert('insert', '\n')
        lab3.update()

    # 文件存放错误位置提示
    if file_in_wrong_folder:
        lab3.insert('insert', '\n')
        lab3.insert('insert', '文件夹中文件不是该站点的文件:')
        lab3.insert('insert', '\n')
        for file in file_in_wrong_folder:
            lab3.insert('insert', file)
            lab3.insert('insert', '\n')
        lab3.update()

    # 需要重复运行提示
    if error_column_name_stations or multiple_files_stations or file_in_wrong_folder:
        lab3.insert('insert', '\n')
        lab3.insert('insert', '请修改后重新运行程序')
        lab3.update()
        time.sleep(10000)

    if all_stations_perf:
        stations_perf = pd.concat(all_stations_perf)
    if all_stations_sku_have_ordered_perf:
        sku_have_ordered_perf = pd.concat(all_stations_sku_have_ordered_perf)
    if all_stations_sku_no_ordered_perf:
        sku_no_ordered_perf = pd.concat(all_stations_sku_no_ordered_perf)
    return [stations_perf, sku_have_ordered_perf, sku_no_ordered_perf]


def process_stations_perf():
    """
    处理站点的表现主函数:
        1.广告有订单的表现
        2.加入购物车广告的表现
        3.站点有销售却广告没有销售的表现
        4.将店铺销售额排名(前10:昨日,最近7天,最近30天)
    :return: xlsx
        得到excel
    """
    global site_exchange_rate, station_name_corresponding_dict
    # 得到汇率
    site_exchange_rate = exchange_rate.rate_exchange()
    # 调整输出汇率格式
    site_exchange_rate_df = pd.DataFrame(site_exchange_rate, index=[0]).T
    site_exchange_rate_df['国家简称'] = site_exchange_rate_df.index
    site_exchange_rate_df['国家'] = [static_param.SITE_EN_ZH[account] if account != 'updatetime' else '' for account
                                   in site_exchange_rate_df['国家简称']]
    site_exchange_rate_df.rename(columns={0: '汇率'}, inplace=True)
    site_exchange_rate_df = site_exchange_rate_df.applymap(lambda x: '更新时间' if x == 'updatetime' else x)
    site_exchange_rate_df = site_exchange_rate_df.reindex(columns=['国家', '国家简称', '汇率'])

    # 输入folder_path
    # 对文件夹进行判断
    # 获取输入框中内容
    folder_path = address.get()
    folder_path = folder_path.strip('" ')
    base_path = os.path.dirname(folder_path)
    # 显示输入内容
    showtext = f'开始处理" {folder_path} "文件夹,请耐心等待.'
    lab3.insert('insert', showtext + '\n')
    # lab3.update()
    lab3.update()
    public_function.detect_folder(folder_path)

    # 导入店铺名对应关系表
    # 加载店铺销售数据
    station_name_corresponding_file_name = ['店铺名统一化.xls', '店铺名统一化.xlsx']
    station_name_corresponding_file_exist = 0
    for file in station_name_corresponding_file_name:
        station_name_corresponding_file_path = os.path.join(base_path, file)
        if os.path.exists(station_name_corresponding_file_path):
            station_name_corresponding_file_exist = 1
            break
    if station_name_corresponding_file_exist == 0:
        messagebox.showinfo('文件不存在', f'店铺对应关系表不存在,请核查{base_path}下是否有 {station_name_corresponding_file_name} 文件.')

    station_name_corresponding_data = load_station_name_corresponding(station_name_corresponding_file_path)
    station_name_corresponding_data.drop_duplicates(subset=['ERP account'], inplace=True)
    station_name_corresponding_dict = {lazada_account.upper(): erp_account.upper() for erp_account, lazada_account in
                                       zip(station_name_corresponding_data['ERP account'],
                                           station_name_corresponding_data['lazada account'])}

    station_name_reverse_corresponding_dict = {erp_account.upper(): lazada_account.upper() for
                                               erp_account, lazada_account in
                                               zip(station_name_corresponding_data['ERP account'],
                                                   station_name_corresponding_data['lazada account'])}

    # 得到全部站点的表现
    [stations_perf, sku_have_ordered_perf, sku_no_ordered_perf] = get_all_station_perf(folder_path)

    # 广告信息中添加一列erpsku列
    add_erpsku_column(sku_have_ordered_perf)
    add_erpsku_column(sku_no_ordered_perf)

    # 将站点表现中的账号名(lazada平台上的账号名)转换为erpsku上的账号名
    stations_perf['account'] = stations_perf['account'].apply(
        lambda x: station_name_corresponding_dict.get(x.upper(), f'{x}找不到对应的erp 账号'))
    sku_have_ordered_perf['account'] = sku_have_ordered_perf['account'].apply(
        lambda x: station_name_corresponding_dict.get(x.upper(), f'{x}找不到对应的erp 账号'))
    sku_no_ordered_perf['account'] = sku_no_ordered_perf['account'].apply(
        lambda x: station_name_corresponding_dict.get(x.upper(), f'{x}找不到对应的erp 账号'))
    for df in [stations_perf, sku_have_ordered_perf, sku_no_ordered_perf]:
        df['station'] = df['account'] + '-' + df['site']

    """
    添加店铺sku销售情况:
        1.店铺有广告表现:
            有订单sku部分,无订单sku表部分均中添加两列:
                1. sku广告销售额占整个店铺销售额占比,
                2. sku广告销售数量占整个店铺销售数量占比
        2.店铺无广告表现:
            新建一个表格,包含列为:
                日期、账号、站点、sku、sku销售数量、sku销售额、sku销售数量占比、sku销售额占比
    """
    # 加载店铺销售数据
    shop_sales_file_name = ['lazada总销.xls', 'lazada总销.xlsx', 'lazada总销.csv']
    shop_sale_exist = 0
    for file in shop_sales_file_name:
        shop_sales_file_path = os.path.join(base_path, file)
        if os.path.exists(shop_sales_file_path):
            shop_sale_exist = 1
            break
    if shop_sale_exist == 0:
        messagebox.showinfo('文件不存在', f'lazada总销售数据不存在,请核查{base_path}下是否有{shop_sales_file_name}文件.')

    shop_sales_file_type = os.path.splitext(shop_sales_file_path)[1]
    if shop_sales_file_type.lower() == '.csv':
        try:
            shop_sales_file_data = pd.read_csv(shop_sales_file_path, encoding='gb2312')
        except:
            shop_sales_file_data = pd.read_csv(shop_sales_file_path)
    elif shop_sales_file_type.lower() in ['.xls', '.xlsx']:
        try:
            shop_sales_file_data = pd.read_excel(shop_sales_file_path, encoding='gb2312')
        except:
            shop_sales_file_data = pd.read_csv(shop_sales_file_path)
    else:
        raise ImportError(f'{shop_sales_file_path} is not a valid file.File type Must input .csv .xls or .xlsx')
    # 初始化店铺数据
    shop_data = process_station.init_shop_file_data(shop_sales_file_data)
    # 将店铺销售额转换成人民币
    shop_data['销售额'] = [round(site_exchange_rate[site] * sale, 2) for sale, site in
                        zip(shop_data['销售额'], shop_data['site'])]
    # 店铺数据按照日期,店铺,sku来汇总
    shop_data = shop_data.groupby(['付款时间', '账号', 'sku']).agg(
        {'数量': 'sum', '销售额': 'sum', '平台': 'first', 'account': 'first', 'site': 'first', '订单号': 'first',
         '币种': 'first'}).reset_index()

    # 添加seller_sku列
    shop_data_columns = list(shop_data.columns)
    shop_data_columns.append('seller_sku')
    shop_data = add_sellersku_column(shop_data)
    shop_data = shop_data[shop_data_columns]

    # 店铺按照日期,账号,sku分类
    shop_data_sorted_by_sku = shop_data[['付款时间', '账号', 'sku', '数量', '销售额']]

    # 广告每天销售的sku的集合
    need_columns = ['Date', 'station', 'erpsku']
    ad_station_sku_info = pd.concat([sku_have_ordered_perf[need_columns], sku_no_ordered_perf[need_columns]])
    ad_station_sku_set = ad_station_sku_info.groupby(['Date', 'station']).agg(
        {'erpsku': lambda x: set(x)}).reset_index()
    # 店铺有订单,广告没有订单集合

    shop_data_merge_ad_sku_set = pd.merge(shop_data, ad_station_sku_set, left_on=['付款时间', '账号'],
                                          right_on=['Date', 'station'], how='left')
    shop_data['sku_status'] = [None if pd.isna(ad_sku_set) else 1 if shop_sku in ad_sku_set else 0 for
                               shop_sku, ad_sku_set
                               in zip(shop_data_merge_ad_sku_set['sku'], shop_data_merge_ad_sku_set['erpsku'])]
    shop_sales_ad_no_sales = shop_data[shop_data['sku_status'] != 0]

    """
        1. 有订单sku表现/无订单sku表现 表中添加两列:
            sku销售额占比以及销售数量占比
        2. 店铺无广告的sku情况
    """
    # 添加销售数量占比,销售额占比

    sku_have_ordered_perf = pd.merge(sku_have_ordered_perf, shop_data_sorted_by_sku,
                                     left_on=['Date', 'station', 'erpsku'],
                                     right_on=['付款时间', '账号', 'sku'], how='left')

    sku_have_ordered_perf['销售数量占比'] = [
         '0%' if ((shop_sku_num == 0) or (
            pd.isna(shop_sku_num))) else str(round(ad_sku_num * 100 / shop_sku_num, 2)) + '%' for ad_sku_num, shop_sku_num
        in zip(sku_have_ordered_perf['Units Sold'], sku_have_ordered_perf['数量'])]

    sku_have_ordered_perf['销售额占比'] = [
        '0%' if ((shop_sku_sale == 0) or (
            pd.isna(shop_sku_sale))) else str(round(ad_sku_sale * 100 / shop_sku_sale, 2)) + '%' for
        ad_sku_sale, shop_sku_sale
        in zip(sku_have_ordered_perf['Revenue'], sku_have_ordered_perf['销售额'])]
    sku_have_ordered_perf.rename(columns={'数量': '店铺销售数量', '销售额': '店铺销售额'}, inplace=True)

    # sku_no_ordered_perf = pd.merge(sku_no_ordered_perf, shop_data_sorted_by_sku,
    #                                left_on=['Date', 'station', 'erpsku'],
    #                                right_on=['付款时间', '账号', 'sku'], how='left')
    #
    # sku_no_ordered_perf['销售数量占比'] = [
    #     '0%' if (shop_sku_num == 0) or (
    #         pd.isna(shop_sku_num)) else str(round(ad_sku_num * 100 / shop_sku_num, 2)) + '%' for ad_sku_num, shop_sku_num
    #     in zip(sku_no_ordered_perf['次数'], sku_no_ordered_perf['数量'])]
    #
    # sku_no_ordered_perf.rename(columns={'数量': '店铺销售数量'}, inplace=True)

    del sku_have_ordered_perf['付款时间']

    """
    调整输出格式:
        1.将站点名修改为lzdaza命名规则
        2.将日期格式调整为yyyymmdd格式
        3.百分比输出格式调整
        4.调整列名位置
        5.调整列输出项
    """

    # 将站点名命名为lazada命名规则
    def trans_erp_account_into_lazada_account(df):
        df['lazada_account'] = df['account'].apply(lambda x: station_name_reverse_corresponding_dict.get(x, None))
        # 添加一列station列:
        df['lazada_station'] = df['lazada_account'] + '-' + df['site']

    # 将日期格式调整为yyyymmdd
    def adjust_date_format(df):
        columns = df.columns
        if 'Date' in columns:
            df['Date'] = df['Date'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))
        if '付款时间' in columns:
            df['付款时间'] = df['付款时间'].apply(lambda x: datetime.strftime(x, '%Y%m%d'))

    # 将店铺销售额按照前10排序
    # 计算店铺最近1天,最近7天,最近30天
    last_day = max(shop_data['付款时间'])
    seven_day_before = last_day - timedelta(days=7)
    thirty_day_before = last_day - timedelta(days=30)

    # 获取店铺前十的数据
    def get_top_10(station_name, station_df, top_num=10):
        """
        获取店铺销售额top10sku的数据

        :type station_name:
        :param df:
        :return:
        """
        public_function.detect_df(df)
        if df.empty:
            return
        # 店铺最近一天的top10
        station_last_day_top10 = station_df[station_df['付款时间'] == last_day].nlargest(top_num, '销售额').groupby(
            ['seller_sku']).agg(
            {'销售额': 'sum'}).reset_index().nlargest(top_num, '销售额')
        station_last_day_top10_sku = list(station_last_day_top10['seller_sku'])
        # 店铺最近七天的top10
        station_last_seven_day_top10 = station_df[station_df['付款时间'] >= seven_day_before].groupby(['seller_sku']).agg(
            {'销售额': 'sum'}).reset_index().nlargest(top_num, '销售额')
        station_last_seven_top10_sku = list(station_last_seven_day_top10['seller_sku'])
        # 店铺最近30天的top10
        station_last_month_day_top10 = station_df[station_df['付款时间'] >= thirty_day_before].groupby(['seller_sku']).agg(
            {'销售额': 'sum'}).reset_index().nlargest(top_num, '销售额')
        station_last_month_top10_sku = list(station_last_month_day_top10['seller_sku'])

        station_top10 = pd.DataFrame(
            [station_last_day_top10_sku, station_last_seven_top10_sku, station_last_month_top10_sku]).T
        station_top10.columns = export_columns_name[3:-1]

        account = station_name[:-3]
        site = station_name[-2:]
        station_top10['账号'] = station_name
        station_top10['account'] = account
        station_top10['site'] = site
        station_top10['sales_rank'] = range(1, len(station_top10) + 1)
        return station_top10

    # 店铺seller sku销售排名
    def seller_saler_rank(station_saler):
        """
        :param station_saler:
        :return:
        """
        public_function.detect_df(station_saler)
        if station_saler.empty:
            return
        station_saler.sort_values(by=['付款时间', '销售额'], ascending=[False, False], inplace=True)
        station_saler_group_by_date = station_saler.groupby(['付款时间'])
        all_station_saler_rank = []
        for _, one_day_station_saler_info in station_saler_group_by_date:
            one_day_rank = range(1, len(one_day_station_saler_info) + 1)
            one_day_station_saler_info['销售额排名'] = one_day_rank
            all_station_saler_rank.append(one_day_station_saler_info)
        return pd.concat(all_station_saler_rank)

    shop_sale_top10 = []
    shop_sale_rank_info = []
    shop_data_have_seller_sku = shop_data[~pd.isna(shop_data['seller_sku'])]
    station_shop_grouped_data = shop_data_have_seller_sku.groupby(['账号'])
    export_columns_name = ['账号', 'account', 'site', '当日_top10_seller_sku', '近7天_top10_seller_sku',
                           '近30天_day_top10_seller_sku', 'sales_rank']
    for station_name, station_df in station_shop_grouped_data:
        account = station_name[:-3]
        site = station_name[-2:]
        lazada_account = station_name_reverse_corresponding_dict.get(account.upper(), f'{account} error')
        station_name = lazada_account + '-' + site
        station_shop_sale_top10 = get_top_10(station_name, station_df)
        # 店铺排名
        station_df['账号'] = station_name
        station_df = seller_saler_rank(station_df)
        if not station_shop_sale_top10.empty:
            shop_sale_top10.append(station_shop_sale_top10)
        if not station_df.empty:
            shop_sale_rank_info.append(station_df)

    shop_sale_top10 = pd.concat(shop_sale_top10)
    shop_sale_top10 = shop_sale_top10[export_columns_name]

    # 店铺排名
    shop_sale_rank_info = pd.concat(shop_sale_rank_info)
    shop_sale_rank_export_columns = ['付款时间', '账号', 'seller_sku', 'sku', '销售额', '数量', '销售额排名']
    adjust_date_format(shop_sale_rank_info)
    shop_sale_rank_info = shop_sale_rank_info[shop_sale_rank_export_columns]

    export_columns_dict = {'站点表现': {'value': stations_perf,
                                    'export_columns': ['Date', 'lazada_station', 'lazada_account', 'site', 'Est. Spend',
                                                       'Revenue', 'ROI']},
                           '有订单sku表现': {'value': sku_have_ordered_perf,
                                        'export_columns': ['Date', 'lazada_station', 'lazada_account', 'site',
                                                           'Seller SKU', 'erpsku', 'Est. Spend', 'Revenue', 'Orders',
                                                           'Units Sold', 'ROI', '店铺销售数量', '店铺销售额', '销售数量占比', '销售额占比']},
                           '无订单sku表现': {'value': sku_no_ordered_perf,
                                        'export_columns': ['Date', 'lazada_station', 'lazada_account', 'site',
                                                           'Seller SKU', 'erpsku', '次数']},
                           '站点有销售广告没销售': {'value': shop_sales_ad_no_sales,
                                          'export_columns': ['付款时间', '平台', 'lazada_station', 'lazada_account', 'site',
                                                             '币种', 'seller_sku', 'sku', '数量', '销售额']}}

    for _, export_items_dict in export_columns_dict.items():
        public_function.detect_df(export_items_dict['value'])
        if not export_items_dict['value'].empty:
            trans_erp_account_into_lazada_account(export_items_dict['value'])
            adjust_date_format(export_items_dict['value'])
            export_items_dict['value'] = export_items_dict['value'].reindex(columns=export_items_dict['export_columns'])

    # 输出到同文件夹下
    datetime_now = datetime.now().strftime('%Y-%m-%d_%H-%M')
    export_basename = f'lazada站点表现_{datetime_now}.xlsx'
    export_path = os.path.join(base_path, export_basename)
    writer = write_excel_beauty.ExcelWriterBeauty(export_path)
    for sheet_name, perf in export_columns_dict.items():
        perf_data = perf['value']
        if len(perf_data) > 0:
            # perf_data.to_excel(writer, sheet_name=sheet_name, index=False)
            writer.write_excel(perf_data,sheet_name=sheet_name)
    # 输出top10 sku
    if not shop_sale_top10.empty:
        writer.write_excel(shop_sale_top10, sheet_name='seller sku top10')
    # 输出店铺销售排名
    if not shop_sale_rank_info.empty:
        writer.write_excel(shop_sale_rank_info, sheet_name='shop sales rank')
    # 文件件站点情况
    if empty_file_stations:
        empty_file_stations.sort(reverse=False)
        empty_file_stations_df = pd.DataFrame([empty_file_stations]).T
        empty_file_stations_df.columns = ['站点无文件']
        writer.write_excel(empty_file_stations_df, sheet_name='站点无文件')
    # 输出汇率
    if not site_exchange_rate_df.empty:
        writer.write_excel(site_exchange_rate_df, sheet_name='汇率')
    writer.save()
    # 输出
    showtext = f'处理完毕,结果输出在文件: {export_path} 中.请关闭此窗口.或此窗口将在3秒钟后自动关闭'
    lab3.insert('insert', '\n')
    lab3.insert('insert', '\n')
    lab3.insert('insert', showtext)
    lab3.update()
    time.sleep(3)
    win.destroy()


# 关闭窗口
def close_window():
    win.destroy()


if __name__ == '__main__':
    win = tk.Tk()
    win.title("Lazada stations perf")  # 添加标题
    ttk.Label(win, text="Folder Address:").grid(column=0, row=0)  # 添加一个标签，并将其列设置为0，行设置为0
    # Address 文本框
    address = tk.StringVar()  # StringVar是Tk库内部定义的字符串变量类型，在这里用于管理部件上面的字符；不过一般用在按钮button上。改变StringVar，按钮上的文字也随之改变。
    address = tk.StringVar()  # StringVar是Tk库内部定义的字符串变量类型，在这里用于管理部件上面的字符；不过一般用在按钮button上。改变StringVar，按钮上的文字也随之改变。
    address_entered = ttk.Entry(win, width=40,
                                textvariable=address)  # 创建一个文本框，并且将文本框中的内容绑定到上一句定义的address变量上，方便clickMe调用
    address_entered.grid(column=1, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    address_entered.focus()  # 当程序运行时,光标默认会出现在该文本框中
    # 提示信息 Text
    tip = tk.Label(win, background='seashell', foreground='red',
                   text='请输入整个站点的文件夹路径')
    tip.grid(column=1, row=2)

    # 按钮绑定事件
    action = ttk.Button(win, text="Ready? Go!",
                        command=process_stations_perf)  # 创建一个按钮, text：显示按钮上面显示的文字, command：当这个按钮被点击之后会调用command函数
    action.grid(column=2, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    # 输出框
    showtext = tk.StringVar()
    lab3 = tk.Text(win, fg='blue')
    # lab3 = tk.Label(win,textvariable = showtxt,height=10, width=50,fg='blue',bg='yellow')
    lab3.grid(row=3, column=0, columnspan=3)

    # 定义关闭按钮
    win.protocol('WM_DELETE_WINDOW', close_window)

    win.mainloop()  # 当调用mainloop()时,窗口才会显示出来

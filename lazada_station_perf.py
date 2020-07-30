#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/27 0027 13:37
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : lazada_station_perf.py

import os
from datetime import datetime
import time

import pandas as pd
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

from public_toolkits import public_function
from public_toolkits import process_station
from public_toolkits import static_param
from public_toolkits import exchange_rate

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


def get_a_station_perf(station_name, station_data, perf_keep_point=2):
    """
    得到一个站点的表现
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


def station_perf_by_ordered_sku(station_name, station_data, perf_keep_point=2, have_ordered=True):
    """
    获取站点有订单sku的表现
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


def get_all_station_perf(all_stations_folder):
    """
    全部站点表现: 账号/站点/花费/销售额
    all_stations_folder: path
        全部站点数据文件夹
    :return:list
        站点总体表现pd.DataFrame,有订单的sku表现,无订单sku表现
    """
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
    for station_dir in all_stations_dir:
        station_files = os.listdir(station_dir)
        station_name = os.path.basename(station_dir)
        if not station_files:
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
    # 输出重复文件
    if multiple_files_stations:
        lab3.insert('insert', '\n')
        lab3.insert('insert', '重复文件如下:')
        lab3.insert('insert', '\n')
        for file in multiple_files_stations:
            lab3.insert('insert', file)
            lab3.insert('insert', '\n')
        lab3.update()

    if error_column_name_stations:
        lab3.insert('insert', '\n')
        lab3.insert('insert', '表头内容有问题的文件如下:')
        lab3.insert('insert', '\n')
        for file in error_column_name_stations:
            lab3.insert('insert', file)
            lab3.insert('insert', '\n')
        lab3.update()
    if error_column_name_stations or multiple_files_stations:
        lab3.insert('insert', '\n')
        showtext = '请修改后重新运行程序.'
        lab3.insert('insert', showtext)
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
    处理站点的表现主函数
    :return:
    """
    global site_exchange_rate
    # 得到汇率
    site_exchange_rate = exchange_rate.change_current()
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
    folder_path = address.get()
    folder_path = folder_path.strip('" ')
    # 显示输入内容
    showtext = f'开始处理" {folder_path} "文件夹,请耐心等待.'
    lab3.insert('insert', showtext + '\n')
    # lab3.update()
    lab3.update()
    public_function.detect_folder(folder_path)
    [stations_perf, sku_have_ordered_perf, sku_no_ordered_perf] = get_all_station_perf(folder_path)
    perf_type_dict = {'站点表现': stations_perf, '有订单sku表现': sku_have_ordered_perf, '无订单sku表现': sku_no_ordered_perf}
    # 输出到同文件夹下
    export_dirname = os.path.dirname(folder_path)
    export_basename = 'lazada站点表现.xlsx'
    export_path = os.path.join(export_dirname, export_basename)
    writer = pd.ExcelWriter(export_path)
    for sheet_name, perf in perf_type_dict.items():
        if len(perf) > 0:
            perf.to_excel(writer, sheet_name=sheet_name, index=False)
    site_exchange_rate_df.to_excel(writer, sheet_name='汇率', index=False)
    writer.save()
    # 输出
    showtext = f'处理完毕,结果输出在文件: {export_path} 中.请关闭此窗口.或此窗口将在3秒钟后自动关闭'
    lab3.insert('insert', '\n')
    lab3.insert('insert', '\n')
    lab3.insert('insert', showtext)
    lab3.update()
    time.sleep(3)
    win.destroy()


if __name__ == '__main__':
    win = tk.Tk()
    win.title("Lazada stations perf")  # 添加标题
    ttk.Label(win, text="Folder Address:").grid(column=0, row=0)  # 添加一个标签，并将其列设置为0，行设置为0
    # Address 文本框
    address = tk.StringVar()  # StringVar是Tk库内部定义的字符串变量类型，在这里用于管理部件上面的字符；不过一般用在按钮button上。改变StringVar，按钮上的文字也随之改变。
    address_entered = ttk.Entry(win, width=40,
                                textvariable=address)  # 创建一个文本框，并且将文本框中的内容绑定到上一句定义的address变量上，方便clickMe调用
    address_entered.grid(column=1, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    address_entered.focus()  # 当程序运行时,光标默认会出现在该文本框中
    # 提示信息 Text
    tip = tk.Label(win, background='seashell', foreground='red',
                   text='请输入整个站点的文件夹路径')
    tip.grid(column=1, row=2)
    # 按钮
    action = ttk.Button(win, text="Ready? Go!",
                        command=process_stations_perf)  # 创建一个按钮, text：显示按钮上面显示的文字, command：当这个按钮被点击之后会调用command函数
    action.grid(column=2, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    # 输出框
    showtext = tk.StringVar()
    lab3 = tk.Text(win, fg='blue')
    # lab3 = tk.Label(win,textvariable = showtxt,height=10, width=50,fg='blue',bg='yellow')
    lab3.grid(row=3, column=0, columnspan=3)
    win.mainloop()  # 当调用mainloop()时,窗口才会显示出来

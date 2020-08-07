#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/8/6 0006 14:43
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : write_excel_beauty.py

"""
    将df好看的写入到xlsx中
"""
import os

import pandas as pd
import xlwt



class ExcelWriterBeauty(object):
    """
        调整df输出到单元格格式:
            单元格大小(宽,高)
            单元格背景颜色
            单元格字体大小,字体,字体颜色,是否是斜体,是否有下划线,是否加粗
    """

    def __init__(self, save_path):
        """
            初始化保存的文件路径
        :param save_path: dir
            文件保存路径
        """
        if not os.path.exists(os.path.dirname(save_path)):
            raise FileNotFoundError(f'{save_path} is not exist.')
        self._save_path = save_path
        self._book = xlwt.Workbook(encoding='utf-8')


    def write_excel(self, df: pd.DataFrame, sheet_name='Sheet1', min_cell_width=12, max_cell_width=100,
                        head_height=3, content_height=2,
                        summary_row=False,
                        head_options={'font_color': [255, 255, 255], 'font-family': 'Microsoft YaHei',
                                      'font-size': 13, 'font-bord': True, 'font-underline': False,
                                      'font-italic': False, 'background-color': [0, 51, 102]},
                        content_options={'font_color': [0, 0, 0], 'font-family': 'Microsoft YaHei',
                                         'font-size': 13, 'font-bord': False, 'font-underline': False,
                                         'font-italic': False, 'background-color-odd': [245, 248, 252],
                                         'background-color-even': [225, 232, 240]}):
        """
            将df按照格式路径xlsx的工作表中
            将工作表格式分为三个部分:标题行、数据行、汇总行(如果存在)
            格式项:
                    单元格背景颜色,单元格边框,单元格高度,单元格宽度,字体,字体颜色,字体加粗,字体大小,字体上下对齐方式,字体左右对齐方式
            待开发的格式项:
                边框:边框大小,颜色,样式等
        :parma head_options: dict
            工作表表头选项:
                font_color:list [r,g,b]
                    字体颜色
                font-family: str
                    字体
                font-size:int
                    字体大小
                font-bord：bool
                    字体是否加粗
                font-underline:bool
                    字体是否拥有下划线
                font-italic:bool
                    是否是斜体
                background-color: list [r,g,b]
                    背景颜色
        :parma content_option:dict
            same as head_options params
        :param summary_row: bool default False
            数据结尾是否拥有总结行 
        :param head_height: int default 3
            表头单元格高度
        :param content_height: int default 2
            内容单元格高度
        :param min_cell_width: int defalut 12
            最小单元格宽度
        :param max_cell_width: int defalut 100
            最大单元格宽度
        :param df: pd.DataFrame
        :param sheet_name:str
            保存的工作表名

        :param save_path:write into path
            保存写入的路径
        :return: None
        """

        # 对df进行检测
        if df is None:
            raise TypeError(f'import is None.')
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'import type must be pd.DataFrame not {type(df)}.')
        if df.empty:
            return

        # 对输入参数进行检测:
        for int_value in [min_cell_width, max_cell_width, head_height, content_height]:
            if not isinstance(int_value, int):
                raise TypeError(f'cell width or height must be int.import {int_value} is not int.')

        for dict_value in [head_options, content_options]:
            if not isinstance(dict_value, dict):
                raise TypeError(f'cell options must be dict.import {dict_value} is not dict.')

        # 检测rgb格式
        for rgb_list in [head_options['font_color'], head_options['background-color'], content_options['font_color'],
                         content_options['background-color-odd'], content_options['background-color-even']]:
            if not isinstance(rgb_list,list):
                raise TypeError(f'Cell font-color or background-color must be RGB list.import {rgb_list} not list.')
            if len(rgb_list)!=3:
                raise ValueError(f'RGB must element is 3,but you rgb is {len(rgb_list)}.')
            for value in rgb_list:
                if value <0 or value >255:
                    raise ValueError(f'RGB must 0~255,but you rgb value is {value}.')


        # 初始化工作表
        sheet = self._book.add_sheet(sheet_name, cell_overwrite_ok=True)  # cell_overwrite_ok代表可以重复处理一个单元格

        # 个性化定制颜色
        """
           Colour indexes 0 to 7 represent 8 fixed built-in colours: black, white, red, green, blue, yellow, magenta, and cyan.

            The remaining colours in the palette (8 to 63 in Excel 5.0 and later) can be changed by the user.
        

            Indexes 8 to 15 correspond to changeable parallels of the 8 fixed colours – for example, index 7 is forever cyan; index 15 starts off being cyan but can be changed by the user. 
            
            xlwt中有64中颜色,但是只有8中颜色是系统固定不变的(代号为:0x1~0x8),其他56中颜色是可以自定义的
            
        """
        self._book.set_colour_RGB(0x10, head_options['font_color'][0], head_options['font_color'][1],
                                  head_options['font_color'][2])
        self._book.set_colour_RGB(0x11, content_options['font_color'][0], content_options['font_color'][1],
                                  content_options['font_color'][2])
        self._book.set_colour_RGB(0x21, head_options['background-color'][0], head_options['background-color'][1],
                                  head_options['background-color'][2])
        self._book.set_colour_RGB(0x22, content_options['background-color-odd'][0],
                                  content_options['background-color-odd'][1],
                                  content_options['background-color-odd'][2])
        self._book.set_colour_RGB(0x23, content_options['background-color-even'][0],
                                  content_options['background-color-even'][1],
                                  content_options['background-color-even'][2])

        # 1.初始化字体
        font_head = xlwt.Font()
        font_content = xlwt.Font()
        # 设置字体颜色,字体大小,字体是否加粗,以及是否有下划线、斜体
        font_head.name = head_options['font-family']
        font_content.name = content_options['font-family']
        font_head.colour_index = 0x10
        font_content.colour_index = 0x11
        font_head.height = head_options['font-size'] * 20  # 字体大小,20为衡量单位,为字号
        font_content.height = content_options['font-size'] * 20  # 字体大小,20为衡量单位,为字号
        font_head.bold = head_options['font-bord']
        font_content.bold = content_options['font-bord']
        font_head.underline = head_options['font-underline']
        font_content.underline = content_options['font-underline']
        font_head.italic = head_options['font-italic']
        font_content.italic = content_options['font-italic']
        # 设置字体颜色,字体大小,字体是否加粗,以及是否有下划线、斜体

        # 2.初始化字体的对齐方式
        alignment_head = xlwt.Alignment()
        alignment_content = xlwt.Alignment()
        # 左右对齐和上下对齐的方式
        # 0x01(左端对齐)、0x02(水平方向上居中对齐)、0x03(右端对齐)
        alignment_head.horz = 0x01
        alignment_content.horz = 0x02
        # 0x00(上端对齐)、 0x01(垂直方向上居中对齐)、0x02(底端对齐)
        alignment_head.vert = 0x01
        alignment_content.vert = 0x01
        # 设置自动换行
        alignment_head.wrap = 1
        alignment_content.wrap = 1

        # 3.设置边框
        borders_head = xlwt.Borders()
        borders_content = xlwt.Borders()

        # 细实线:1，小粗实线:2，细虚线:3，中细虚线:4，大粗实线:5，双线:6，细点虚线:7
        # 大粗虚线:8，细点划线:9，粗点划线:10，细双点划线:11，粗双点划线:12，斜点划线:13
        # 不设置则没有边框

        # 4.设置背景颜色
        pattern_head = xlwt.Pattern()
        pattern_content_odd = xlwt.Pattern()
        pattern_content_even = xlwt.Pattern()
        # 设置背景颜色的模式
        pattern_head.pattern = pattern_content_odd.pattern = pattern_content_even.pattern = xlwt.Pattern.SOLID_PATTERN
        # 背景颜色
        pattern_head.pattern_fore_colour = 0x21
        pattern_content_odd.pattern_fore_colour = 0x22
        pattern_content_even.pattern_fore_colour = 0x23

        # 5.设置列宽，一个中文等于两个英文等于两个字符，11为字符数，256为衡量单位
        def get_col_width(col_list, extent_len=2, min_cell_width=min_cell_width,
                          max_cell_width=max_cell_width) -> int:
            """
                excel中文占用两个字符,英文和数字占用一个字符
                判断最大值是否为异常值:
                    如果不是异常值,则用最大值excel的宽度的1.2倍为列宽
                    如果是异常值,则用Q3+1.5IQR的1.2倍作为excel的列宽

            :param col_list:
            :param extent_len:int
                 自适应扩展宽度倍数
            :return: int
                 计算出写入到excel中的宽度
            """

            # 计算字符串长度
            def calc_str_width(calc_str):
                # 计算字符串的长度:中文等为2个字符,数字和英文为1个字符
                if not isinstance(calc_str, str):
                    if pd.isna(calc_str):
                        calc_str = ' '
                    else:
                        calc_str = str(calc_str)
                ori_str_len = len(calc_str)
                chinses_num = 0
                for i in calc_str:
                    # 中文检测:中文符号区
                    if '\u4e00' <= i <= '\u9fff':
                        chinses_num += 1
                str_len = ori_str_len + chinses_num
                return str_len

            if len(col_list) == 0:
                return 1
            # 计算List中每个占用字符长度
            list_element_len = [calc_str_width(str) for str in col_list]
            list_element_len_sorted = sorted(list_element_len)
            list_len = len(col_list)
            # 计算list的最大长度和上四分位数以及四分位间距
            list_element_max_len = list_element_len_sorted[-1]
            # 上四分位数
            q3_element_max_len = list_element_len_sorted[int(0.75 * list_len)]
            # 下四分位数
            q1_element_max_len = list_element_len_sorted[int(0.25 * list_len)]
            # 1.5倍四分位间距
            # 有效上线
            limit_max_len = q3_element_max_len + 1.5 * (q3_element_max_len - q1_element_max_len)
            cell_len = min(limit_max_len, list_element_max_len)
            return min(max_cell_width, max(int(extent_len * cell_len), min_cell_width))

        i = 0
        for _, col_value in df.iteritems():
            col_len = get_col_width(col_value, min_cell_width=min_cell_width,max_cell_width=max_cell_width)
            sheet.col(i).width = col_len * 256
            i += 1

        # 6.设置行高
        # 首行行高
        # first you should tell xlwt row height and default font height do not match
        sheet.row(0).height_mismatch = True
        sheet.row(0).height = head_height * 256
        # 设置其他行
        for i in range(1, len(df) + 1):
            sheet.row(i).height_mismatch = True
            sheet.row(i).height = content_height * 256
        if summary_row:
            sheet.row(len(df)).height_mismatch = True
            sheet.row(len(df)).height = head_height * 256

        # 初始化样式
        # 列名行
        style_head = xlwt.XFStyle()
        # 字体
        style_head.font = font_head
        # 背景颜色
        style_head.pattern = pattern_head
        # 对齐方式
        style_head.alignment = alignment_head
        # 边框
        style_head.borders = borders_head

        # 数据行
        style_content_odd = xlwt.XFStyle()
        style_content_even = xlwt.XFStyle()
        style_content_odd.font = style_content_even.font = font_content
        style_content_odd.pattern = pattern_content_odd
        style_content_even.pattern = pattern_content_even
        style_content_odd.alignment = style_content_even.alignment = alignment_content
        style_content_odd.borders = style_content_even.borders = borders_content

        def row_writer(row_value, row_index, sheet, style):
            for i, value in enumerate(row_value):
                sheet.write(row_index, i, value, style)

        # 写列标题
        column = df.columns
        columns_index = 0
        row_writer(column, columns_index, sheet, style_head)
        # 写内容
        df.reset_index(drop=True,inplace=True)
        for row, row_value in df.iterrows():
            if (row % 2) == 0:
                row_writer(row_value, row + 1, sheet, style_content_odd)
            else:
                row_writer(row_value, row + 1, sheet, style_content_even)

        if summary_row:
            row_writer(list(df.tail(1).values[0]), len(df), sheet, style_head)

    def save(self):
        self._book.save(self._save_path)


if __name__ == '__main__':
    a = ExcelWriterBeauty(save_path)
    a.write_excel(df, summary_row=True)
    a.write_excel(df, sheet_name='123', summary_row=False)
    a.save()


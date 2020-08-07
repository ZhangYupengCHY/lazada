#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/30 0030 18:21
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : search_sku.py


"""
    通过sku查询erpsku,店铺名个店铺id
"""
import json
import pandas as pd

import requests


def query_lazada_sku(sku,sku_type:['seller_sku','erp_sku']):
    """
    通过接口获取接口数据:

    查询方式：
        接口文档地址:
            http://erppub.yibainetwork.com/services/lazada/lazadaforeign/getlistingmesg
        接口参数:
            参数名	        必选	    类型	    说明
            seller sku	    是	    string	Seller SKU(多个sku用逗号连接)
            sku             是	    string	erp SKU(多个sku用逗号连接)
    :return:df
            接口返回数据
    """
    if not isinstance(sku,str):
        raise ImportError(f'{sku} must be string.')
    if sku_type not in ['seller_sku','erp_sku']:
        raise ImportError('import sku type must one of "seller_sku" or "erp_sku"')
    request_url = r"http://erppub.yibainetwork.com/services/lazada/lazadaforeign/getlistingmesg"
    if sku_type == 'seller_sku':
        params = {
            "seller_sku": sku,
        }
    else:
        params = {
            "sku": sku,
        }
    response = requests.get(request_url, params)
    if response.status_code!=200:
        raise ConnectionError(f'{request_url} status code is {response.status_code}.')
    response = json.loads(response.content)
    if response['message'] == '查询失败':
        print(f'{sku}无查询结果,请核查是否为有效的lazada sellersku.')
        return
    response_df = pd.DataFrame(response['content'])
    return response_df


if __name__ == '__main__':
    sellersku = 'JUMD2Q3O2N9K9S'
    erpsku = query_lazada_sku(sellersku,'seller_sku')
    print(erpsku)
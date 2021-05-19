import pandas as pd
import numpy as np
import os
import requests
import datetime
import re
from ozon_seller_client import OzonSellerClient
from transform_data import extract_details, transform_postings, save_to_csv_by_month
from date_boundaries import month_start_and_end_dates, last_three_month_start_and_end_dates, get_month_year_unique_list

ozon_api_key = r'30b688f6-90a0-4930-8fad-3fb542d99609'
ozon_client_id = r'31905'
# data_path = r'\\192.168.244.9\ShfS9\marketing\Dannye_dlya_analitiki\DataComDep\Ozon'
data_path = r'C:\Users\frpst\\'

client = OzonSellerClient(api_key=ozon_api_key, client_id=ozon_client_id)

#current_date = datetime.datetime.now()
current_date = datetime.datetime(2021, 4, 1, 0, 0, 0)
start_date, end_date = month_start_and_end_dates(current_date)

posting_request_body = {'dir': 'asc',
                        'filter': {
                            'since': start_date,
                            'to': end_date
                                 },
                        'limit': "100",
                        'offset': "0",
                        "translit": True,
                        "with": {
                            "analytics_data": True,
                            "financial_data": True
                                }
                        }

# Получаем данные об отправлениях со склада Ozon
# ozon_postings = client.get_ozon_postings(posting_request_body, offset_increment=50)
#
# if not ozon_postings.empty:
#     ozon_postings = transform_postings(ozon_postings)
#     save_to_csv_by_month(ozon_postings, 'created_at', data_path + r'postings\ozon_data_fbo_' + ozon_client_id )
#
#
# # Получаем данные об отправлениях с собственного склада
# self_postings = client.get_self_postings(posting_request_body, offset_increment=50)
#
# if not self_postings.empty:
#     self_postings = transform_postings(self_postings)
#     save_to_csv_by_month(self_postings, 'created_at', data_path + r'postings\ozon_data_fbs_' + ozon_client_id )


# Получаем данные по остаткам

stocks_request_body = {
  "page": 1,
}

data_stocks = client.get_stocks(stocks_request_body, page_size=1000)
data_stocks = data_stocks.explode('stocks').reset_index()
data_stocks = extract_details(data_stocks, 'stocks', ['type', 'present', 'reserved'])

data_stocks = data_stocks[['product_id','offer_id','type','present','reserved','request_scopes']]
data_stocks.to_csv(data_path+r'\stocks\ozon_stocks'+ ozon_client_id + '.csv', sep='\t', encoding='utf-8')

# Идентификаторы товаров
product_list_scopes = r'https://api-seller.ozon.ru/v1/product/list'

# r = requests.post(product_list_scopes,
#                   json={'page_size':1000},
#                   headers=headers)

request_body = {}

product_id_list = client.get_product_list(request_body)

# product_id_list = extract_details(product_id_list, 'items', ['product_id','offer_id'])

product_id_list.to_csv(data_path+'\\directories\product_id_list.csv', sep='\t', encoding='utf-8')
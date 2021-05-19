import pandas as pd
import numpy as np
import os
import requests
import datetime
import re
from ozon_seller_client import OzonSellerClient

def get_ozon_offset_data(scopes, params, headers):
    df = pd.DataFrame()
    offset = 0
    result_len = 1

    json_params = params.copy()

    while result_len > 0:
        print(offset, ' rows loaded')
        r = requests.post(scopes, json=json_params, headers=headers)
        df = df.append(pd.DataFrame(r.json()['result']))

        result_len = len(r.json()['result'])
        offset += 50
        json_params['offset'] = str(offset)

    request_scopes = re.findall(r'\/v.?\/(.*)', scopes)[0]
    df['request_scopes'] = request_scopes

    return df


def extract_details(df, extract_column, params):
    if not df.empty:
        df_details = df.reset_index()

        for i in params:
            df_details[i] = df_details[extract_column].apply(lambda x: get_dict_element(x, i))

        return df_details

    return df


def get_products_data(scopes, params, headers, page_size=100):
    df = pd.DataFrame()
    json_params = params.copy()
    json_params['page_size'] = page_size
    result_len = json_params['page_size']
    page = 1

    while result_len == page_size:
        r = requests.post(scopes, json=json_params, headers=headers)
        df = df.append(pd.DataFrame(r.json()['result']['items']))

        result_len = len(r.json()['result']['items'])
        page += 1
        json_params['page'] = str(page)

    request_scopes = re.findall('\/v.?\/(.*)', scopes)[0]
    df['request_scopes'] = request_scopes

    return df


def get_ozon_paged_data(scopes, params, headers, page_size=100):
    df = pd.DataFrame()
    json_params = params.copy()
    json_params['page_size'] = page_size
    result_len = json_params['page_size']
    page = 1

    while result_len == page_size:
        r = requests.post(scopes, json=json_params, headers=headers)
        df = df.append(pd.DataFrame(r.json()['result']))

        result_len = len(r.json()['result'])
        page += 1
        json_params['page'] = str(page)

    request_scopes = re.findall('\/v.?\/(.*)', scopes)[0]
    df['request_scopes'] = request_scopes

    return df


def get_dict_element(dictionary, element):
    try:
        return dictionary.get(element)
    except:
        return np.nan


def write_each_month_to_csv(path, data, date_column='date', prefix='data'):
    dates = data[date_column].unique()
    for date in dates:
        filename = prefix + '_' + str(date) + '.csv'
        data[data[date_column] == date].to_csv(os.path.join(path, filename), sep='\t')



ozon_api_key = r'30b688f6-90a0-4930-8fad-3fb542d99609'
ozon_client_id = r'31905'
data_path = r'\\192.168.244.9\ShfS9\marketing\Dannye_dlya_analitiki\DataComDep\Ozon'

client = OzonSellerClient(api_key=ozon_api_key, client_id=ozon_client_id)

# определяем текушую дату
current_date = datetime.datetime.now()

# определяем даты начала текущего и следующего месяца
start_date = current_date.replace(day=1, hour=0, minute=0, second=0).strftime('%Y-%m-%dT%H:%M:%SZ')
if current_date.month == 12:
    end_date = current_date.replace(day=1, hour=0, minute=0, second=0, month=1, year=current_date.year + 1).strftime(
        '%Y-%m-%dT%H:%M:%SZ')
else:
    end_date = current_date.replace(day=1, month=current_date.month + 1, hour=0, minute=0, second=0).strftime(
        '%Y-%m-%dT%H:%M:%SZ')

ozon_stock_shipments_scopes = r'http://api-seller.ozon.ru/v2/posting/fbo/list'
self_stock_shipments_scopes = r'http://api-seller.ozon.ru/v2/posting/fbs/list'

headers = {
    'host': 'api-seller.ozon.ru',
    'client-Id': ozon_client_id,
    'api-Key': ozon_api_key,
    'Content-Type': 'application/json'}

request_body = {'dir': 'asc',
                'filter': {
                    'since': start_date,
                    'to': end_date
                },
                'limit': "50",
                'offset': "0",
                "translit": True,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
                }

# Получаем данные об отправлениях со склада Ozon
shipments_ozon = get_ozon_offset_data(ozon_stock_shipments_scopes,
                                      request_body,
                                      headers)

if not shipments_ozon.empty:
    shipments_ozon_details = shipments_ozon.explode('products')
    shipments_ozon_details = extract_details(shipments_ozon_details, 'products',
                                             ['sku', 'offer_id', 'name', 'quantity', 'price'])
    shipments_ozon_details = extract_details(shipments_ozon_details, 'analytics_data',
                                             ['region', 'city', 'delivery_type', 'payment_type_group_name',
                                              'warehouse_id', 'warehouse_name'])
    shipments_ozon_details.to_csv(data_path+r'\postings\ozon_data_fbo_' + current_date.strftime('%Y-%b') + '.csv',
                                  sep='\t',
                                  encoding='utf-8')

# Получаем данные об отправлениях с собственного склада
self_stock_shipments = get_ozon_offset_data(self_stock_shipments_scopes,
                                            request_body,
                                            headers)

if not self_stock_shipments.empty:
    self_stock_shipments_details = self_stock_shipments.explode('products')
    self_stock_shipments_details = extract_details(self_stock_shipments_details, 'products',
                                                   ['sku', 'offer_id', 'name', 'quantity', 'price'])
    self_stock_shipments_details = extract_details(self_stock_shipments_details, 'analytics_data',
                                                   ['region', 'city', 'delivery_type', 'payment_type_group_name',
                                                    'warehouse_id', 'warehouse_name'])
    self_stock_shipments_details.to_csv(data_path+'\\postings\ozon_data_fbs_' + current_date.strftime('%Y-%b') + '.csv', sep='\t',
                                        encoding='utf-8')

# Получаем данные по остаткам
stocks_url_scopes = r'https://api-seller.ozon.ru/v2/product/info/stocks'

stocks_request_body = {
  "page": 1,
}

data_stocks = get_products_data(stocks_url_scopes,stocks_request_body, headers=headers, page_size=1000)
data_stocks = data_stocks.explode('stocks').reset_index()
data_stocks['stock_type'] = data_stocks['stocks'].apply(lambda x: get_dict_element(x, 'type'))
data_stocks['present'] = data_stocks['stocks'].apply(lambda x: get_dict_element(x, 'present'))
data_stocks['reserved'] = data_stocks['stocks'].apply(lambda x: get_dict_element(x, 'reserved'))

data_stocks = data_stocks[['product_id','offer_id','stock_type','present','reserved','request_scopes']]
data_stocks.to_csv(data_path+r'\stocks\ozon_stocks.csv', sep='\t', encoding='utf-8')


# Идентификаторы товаров
product_list_scopes = r'https://api-seller.ozon.ru/v1/product/list'

# r = requests.post(product_list_scopes,
#                   json={'page_size':1000},
#                   headers=headers)

request_body = {'page_size': 1000}

product_id_list = get_products_data(product_list_scopes,
                                    request_body,
                                    headers,
                                    page_size=1000)

# product_id_list = extract_details(product_id_list, 'items', ['product_id','offer_id'])

product_id_list.to_csv(data_path+'\\directories\product_id_list' + ozon_client_id +'.csv', sep='\t', encoding='utf-8')


# Информация о товарах
product_list_info_scopes = r'https://api-seller.ozon.ru/v2/product/info/list'

request_body = {'product_id': [str(x) for x in list(product_id_list.product_id.unique())]}

product_list_info = get_products_data(product_list_info_scopes,
                                      request_body,
                                      headers=headers,
                                      page_size=1000)

product_list_info

product_list_info = extract_details(product_list_info, 'stocks', ['coming', 'present', 'reserved'])
product_list_info = extract_details(product_list_info, 'visibility_details',
                                    ['has_price', 'has_stock', 'active_product'])

product_list_info.to_csv(data_path+'\\directories\product_list_info.csv', sep='\t', encoding='utf-8')


# Достаем все транзакции за последние 3 месяца
month_delta = -1
result = current_date.month + month_delta
if result < 0:
    transactions_start_date = datetime.datetime(current_date.year - 1, 12 + result, 1).strftime('%Y-%m-%dT%H:%M:%SZ')
elif result == 0:
    transactions_start_date = datetime.datetime(current_date.year - 1, 12, 1).strftime('%Y-%m-%dT%H:%M:%SZ')
else:
    transactions_start_date = datetime.datetime(current_date.year, result, 1).strftime('%Y-%m-%dT%H:%M:%SZ')

# Информация о транзакциях
transactions_scopes = r'https://api-seller.ozon.ru/v2/finance/transaction/list'

request_body = {
    "filter": {
        "date": {
            "from": transactions_start_date,
            "to": end_date,
        },
        "transaction_type": "all"
    },
    "page": 1,
    "page_size": 1000
}

# r = requests.post(transactions_scopes, json=request_body, headers=headers)
ozon_transactions_data = get_ozon_paged_data(transactions_scopes,
                                             request_body,
                                             headers,
                                             page_size=1000)

ozon_transactions_data['period'] = ozon_transactions_data['orderDate'].astype('datetime64[ns]').dt.to_period('M')

write_each_month_to_csv(data_path+r'\transactions\\', ozon_transactions_data, date_column='period', prefix='transactions_')
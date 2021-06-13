import requests
import re
import pandas as pd
import os
from response import Response


ozon_stock_shipments_scopes = r'http://api-seller.ozon.ru/v2/posting/fbo/list'
self_stock_shipments_scopes = r'http://api-seller.ozon.ru/v2/posting/fbs/list'

class OzonSellerClient:
    def __init__(self, api_key, client_id, host= r'api-seller.ozon.ru'):
        self.api_key = api_key
        self.client_id = client_id
        self.host = host
        self.headers = {
                        'host': host,
                        'client-Id': client_id,
                        'api-Key': api_key,
                        'Content-Type': 'application/json'}

    def get_products(self, params={}, page_size=100):
        method = r'/v1/product/list'
        scopes = r'http://' + self.host + method
        return self.get_product_data(scopes, params, page_size)

    def get_products_info(self, params={}, page_size=100) -> pd.DataFrame:
        method = r'/v2/product/info/list'
        scopes = r'http://' + self.host + method
        return self.get_product_data(scopes, params, page_size)

    def get_ozon_postings(self, start_date, end_date, offset_increment=50):
        method = r'/v2/posting/fbo/list'
        scopes = r'http://' + self.host + method
        params = {'dir': 'asc',
                   'filter': {
                       'since': start_date,
                       'to': end_date
                        },
                   'limit': offset_increment,
                   'offset': "0",
                   "translit": True,
                   "with": {
                       "analytics_data": True,
                       "financial_data": True
                        }
                   }

        return self.get_ozon_offset_data(scopes, params, offset_increment)

    def get_self_postings(self, start_date, end_date, offset_increment=50):
        method = r'/v2/posting/fbs/list'
        scopes = r'http://' + self.host + method
        params = {'dir': 'asc',
                  'filter': {
                      'since': start_date,
                      'to': end_date
                            },
                  'limit': offset_increment,
                  'offset': "0",
                  "translit": True,
                  "with": {
                      "analytics_data": True,
                      "financial_data": True
                          }
                  }

        return self.get_ozon_offset_data(scopes, params, offset_increment)

    def get_stocks(self, params={'page': 1}, page_size=100):
        method = r'/v2/product/info/stocks'
        scopes = r'http://' + self.host + method
        return self.get_product_data(scopes, params, page_size)

    def get_transactions(self, start_date, end_date, page_size=500):
        method = r'/v2/finance/transaction/list'
        scopes = r'http://' + self.host + method
        params = {
            "filter": {
                "date": {
                    "from": start_date,
                    "to": end_date,
                },
                "transaction_type": "all"
            },
            "page": 1
        }

        return self.get_ozon_paged_data(scopes, params, page_size)

    def get_ozon_offset_data(self, scopes, params, offset_increment=50):
        params = params.copy()
        dataframe = pd.DataFrame()
        offset = 0
        result_len = 1

        while result_len > 0:
            print('[Log]: ', offset, ' rows loaded')
            raw_response = requests.post(scopes, json=params, headers=self.headers)
            response = Response(raw_response, body_key='result')
            data = response.get_results()

            if data is None:
                print('[Log]: Empty server response. Retrying.')
                continue

            dataframe = dataframe.append(pd.DataFrame(data))

            result_len = len(data)
            offset += offset_increment
            params['offset'] = str(offset)

        request_scopes = re.findall(r'\/v.?\/(.*)', scopes)[0]
        dataframe['request_scopes'] = request_scopes

        return dataframe

    def get_product_data(self, scopes, params, page_size=100):
        df = pd.DataFrame()
        params = params.copy()
        params['page_size'] = page_size
        result_len = params['page_size']
        page = 1

        while result_len == page_size:
            raw_response = requests.post(scopes, json=params, headers=self.headers)
            r = Response(raw_response, body_key='result')
            data = r.get_results()['items']

            if data is None:
                print('[Log]: Empty server response. Retrying.')
                continue

            df = df.append(pd.DataFrame(data))

            result_len = len(r.get_results()['items'])
            page += 1
            params['page'] = str(page)

        request_scopes = re.findall('\/v.?\/(.*)', scopes)[0]
        df['request_scopes'] = request_scopes

        return df

    def get_ozon_paged_data(self, scopes, params, page_size=100):
        df = pd.DataFrame()
        params = params.copy()
        params['page_size'] = page_size
        result_len = params['page_size']
        page = 1

        while result_len == page_size:
            print('[Log]: ', page_size*page, ' rows loaded')
            raw_response = requests.post(scopes, json=params, headers=self.headers)
            r = Response(raw_response, body_key='result')
            df = df.append(pd.DataFrame(r.get_results()))

            result_len = len(r.get_results())
            page += 1
            params['page'] = str(page)

        request_scopes = re.findall('\/v.?\/(.*)', scopes)[0]
        df['request_scopes'] = request_scopes
        return df

    def write_each_month_to_csv(self, path, data, date_column='date', prefix='data'):
        dates = data[date_column].unique()
        for date in dates:
            filename = prefix + '_' + str(date) + '.csv'
            data[data[date_column] == date].to_csv(os.path.join(path, filename), sep='\t')




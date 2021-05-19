import requests
import re
import pandas as pd
import numpy as np
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

    def get_ozon_postings(self, params, offset_increment=50):
        method = r'/v2/posting/fbo/list'
        scopes = r'http://' + self.host + method
        return self.get_ozon_offset_data(scopes, params, offset_increment)

    def get_self_postings(self, params, offset_increment=50):
        method = r'/v2/posting/fbs/list'
        scopes = r'http://' + self.host + method
        return self.get_ozon_offset_data(scopes, params, offset_increment)

    def get_ozon_offset_data(self, scopes, params, offset_increment=50):

        params = params.copy()
        dataframe = pd.DataFrame()
        offset = 0
        result_len = 1

        while result_len > 0:
            print(offset, ' rows loaded')
            raw_response = requests.post(scopes, json=params, headers=self.headers)
            response = Response(raw_response, body_key='result')
            data = response.get_results()

            if data is None:
                continue

            dataframe = dataframe.append(pd.DataFrame(data))

            result_len = len(data)
            offset += offset_increment
            params['offset'] = str(offset)

        request_scopes = re.findall(r'\/v.?\/(.*)', scopes)[0]
        dataframe['request_scopes'] = request_scopes
        dataframe['client_id'] = self.client_id

        return dataframe

    def get_product_list(self, params, page_size=100):
        method = r'/v1/product/list'
        scopes = r'http://' + self.host + method

        df = pd.DataFrame()
        params = params.copy()
        params['page_size'] = page_size
        result_len = params['page_size']
        page = 1

        while result_len == page_size:
            raw_response = requests.post(scopes, json=params, headers=self.headers)
            r = Response(raw_response, body_key='result')
            df = df.append(pd.DataFrame(r.get_results()['items']))

            result_len = len(r.get_results()['items'])
            page += 1
            params['page'] = str(page)

        request_scopes = re.findall('\/v.?\/(.*)', scopes)[0]
        df['request_scopes'] = request_scopes

        return df

    def get_stocks(self, params, page_size=100):
        method = r'/v2/product/info/stocks'
        scopes = r'http://' + self.host + method

        df = pd.DataFrame()
        params = params.copy()
        params['page_size'] = page_size
        result_len = params['page_size']
        page = 1

        while result_len == page_size:
            raw_response = requests.post(scopes, json=params, headers=self.headers)
            r = Response(raw_response, body_key='result')
            df = df.append(pd.DataFrame(r.get_results()['items']))

            result_len = len(r.get_results()['items'])
            page += 1
            params['page'] = str(page)

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



    def write_each_month_to_csv(path, data, date_column='date', prefix='data'):
        dates = data[date_column].unique()
        for date in dates:
            filename = prefix + '_' + str(date) + '.csv'
            data[data[date_column] == date].to_csv(os.path.join(path, filename), sep='\t')
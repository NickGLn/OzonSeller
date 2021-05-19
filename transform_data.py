import numpy as np
from date_boundaries import get_month_year_unique_list


def extract_details(df, parse_column, params):
    if not df.empty:
        df_details = df.reset_index()

        for i in params:
            df_details[i] = df_details[parse_column].apply(lambda x: get_dict_element(x, i))

        return df_details

    return df


def get_dict_element(dictionary, element) -> object:
    try:
        return dictionary.get(element)
    except KeyError:
        return np.nan


def transform_postings(postings: object) -> object:
    if not postings.empty:
        postings = postings.explode('products')
        postings = extract_details(postings,
                                        'products',
                                        ['sku', 'offer_id', 'name', 'quantity', 'price'])

        postings = extract_details(postings,
                                        'analytics_data',
                                        ['region', 'city', 'delivery_type', 'payment_type_group_name', 'warehouse_id',
                                         'warehouse_name'])

        return postings


def save_to_csv_by_month(dataframe, date_column, data_path):
    months = get_month_year_unique_list(dataframe[date_column])
    for month in months:
        dataframe[
            dataframe['created_at'].astype('datetime64[ns]').apply(lambda x: x.strftime('%Y-%b') == month)] \
            .to_csv(data_path +
                    month +
                    '.csv',
                    sep='\t',
                    encoding='utf-8')

def add_clientid_column(dataframe, client_id):
    dataframe['client_id']= client_id
import datetime
import config
from ozon_seller_client import OzonSellerClient
from transform_data import extract_details, transform_postings, save_to_csv_by_month, add_constant_column, add_clientid_column
from date_boundaries import month_start_and_end_dates, last_three_month_start_and_end_dates


data_path = config.path
# data_path = r'C:\Users\frpst\\'

for client_id, api_key in config.credentials.items():
    print('Получаем данные для кабинета: ', client_id)
    client = OzonSellerClient(api_key=api_key, client_id=client_id)

    current_date = datetime.datetime.now()
    start_date, end_date = last_three_month_start_and_end_dates(current_date, month_delta=-3)

    # Отправления со склада Ozon
    print('[Log]: Получаем данные об отправлениях со склада Ozon')
    ozon_postings = client.get_ozon_postings(start_date, end_date, offset_increment=50)

    if not ozon_postings.empty:
        ozon_postings = transform_postings(ozon_postings)
        ozon_postings = add_clientid_column(ozon_postings, client_id)
        save_to_csv_by_month(ozon_postings, 'created_at', data_path + r'\postings\ozon_data_fbo_' +  client_id +'_' )

    # Отправления со своего склада
    print('[Log]: Получаем данные об отправлениях с собственного склада')
    self_postings = client.get_self_postings(start_date, end_date, offset_increment=50)

    if not self_postings.empty:
        self_postings = transform_postings(self_postings)
        self_postings = add_clientid_column(self_postings, client_id)
        save_to_csv_by_month(self_postings, 'created_at', data_path + r'\postings\ozon_data_fbs_' + client_id +'_' )

    # Информация о транзакциях
    print('[Log]: Получаем транзакции')
    transactions = client.get_transactions(start_date, end_date)

    if not transactions.empty:
        transactions['period'] = transactions['orderDate'].astype('datetime64[ns]').dt.to_period('M')
        transactions = add_clientid_column(transactions, client_id)
        save_to_csv_by_month(transactions, 'orderDate', data_path + r'\transactions\transactions_' + client_id + '_')

    # Данные по остаткам
    print('[Log]: Получаем данные по остаткам')
    stocks = client.get_stocks(page_size=1000)
    stocks = stocks.explode('stocks').reset_index()
    stocks = extract_details(stocks, 'stocks', ['type', 'present', 'reserved'])
    stocks = stocks[['product_id', 'offer_id', 'type', 'present', 'reserved', 'request_scopes']]
    stocks = add_clientid_column(stocks, client_id)
    stocks.to_csv(data_path + r'\stocks\ozon_stocks_' + client_id + '_.csv', sep='\t', encoding='utf-8')

    # Идентификаторы товаров
    print('[Log]: Получаем идентификаторы товаров')
    product_list_scopes = r'https://api-seller.ozon.ru/v1/product/list'
    product_ids = client.get_products()
    product_ids = add_clientid_column(product_ids, client_id)
    product_ids.to_csv(data_path + r'\directories\product_id_list_' + client_id + '_.csv', sep='\t', encoding='utf-8')

    # Информация о товарах
    print('[Log]: Получаем информацию о товарах')
    product_id_dict = {'product_id': [str(x) for x in list(product_ids.product_id.unique())]}

    products_info = client.get_products_info(params=product_id_dict)
    products_info = extract_details(products_info, 'stocks', ['coming', 'present', 'reserved'])
    products_info = extract_details(products_info, 'visibility_details', ['has_price', 'has_stock', 'active_product'])
    products_info = add_clientid_column(products_info, client_id)
    products_info.to_csv(data_path + r'\directories\product_list_info_' + client_id + '_.csv', sep='\t', encoding='utf-8')
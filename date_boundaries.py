import datetime

def month_start_and_end_dates(initial_date):
    # определяем даты начала текущего и следующего месяца
    start_date = initial_date.replace(day=1,
                                      hour=0,
                                      minute=0,
                                      second=0).strftime('%Y-%m-%dT%H:%M:%SZ')
    if initial_date.month == 12:
        end_date = initial_date.replace(day=1,
                                        hour=0,
                                        minute=0,
                                        second=0,
                                        month=1,
                                        year=initial_date.year + 1).strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        end_date = initial_date.replace(day=1,
                                        month=initial_date.month + 1,
                                        hour=0,
                                        minute=0,
                                        second=0).strftime('%Y-%m-%dT%H:%M:%SZ')

    return start_date, end_date


def last_three_month_start_and_end_dates(initial_date, month_delta):
    # определяем даты начала месяца за три месяца до текущего и и дату начала следующего месяца

    result = initial_date.month + month_delta
    if result < 0:
        start_date = datetime.datetime(initial_date.year - 1, 12 + result, 1).strftime('%Y-%m-%dT%H:%M:%SZ')
    elif result == 0:
        start_date = datetime.datetime(initial_date.year - 1, 12, 1).strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        start_date = datetime.datetime(initial_date.year, result, 1).strftime('%Y-%m-%dT%H:%M:%SZ')

    if initial_date.month == 12:
        end_date = initial_date.replace(day=1,
                                        hour=0,
                                        minute=0,
                                        second=0,
                                        month=1,
                                        year=initial_date.year + 1).strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        end_date = initial_date.replace(day=1,
                                        month=initial_date.month + 1,
                                        hour=0,
                                        minute=0,
                                        second=0).strftime('%Y-%m-%dT%H:%M:%SZ')

    return start_date, end_date


def get_month_year_unique_list(date_series):
    return list(date_series.astype('datetime64[ns]').apply(lambda x: x.strftime('%Y-%b')).unique())

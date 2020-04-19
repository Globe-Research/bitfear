import calendar
import glob
import numpy as np
import openapi_client as dbitApi
import os
import pandas as pd

from datetime import datetime

api = dbitApi.MarketDataApi()

def format_datetime_to_expiry(date):
    if os.name == 'nt':
        return datetime.strftime(date, '%#d%b%y').upper()
    else:
        return datetime.strftime(date, '%-d%b%y').upper()

def get_near_next_terms(now):
    c = calendar.Calendar(firstweekday=calendar.MONDAY)
    
    this_month_cal = c.monthdatescalendar(now.year, now.month)
    this_fridays = [datetime(day.year, day.month, day.day, 8, 0, 0) 
                    for week in this_month_cal for day in week 
                    if day.weekday() == calendar.FRIDAY and day.month == now.month 
                    and datetime(day.year, day.month, day.day, 8, 0, 0) >= now]
    
    next_year = now.year if now.month < 12 else now.year + 1
    next_month = now.month + 1 if now.month < 12 else 1
    
    next_month_cal = c.monthdatescalendar(next_year, next_month)
    next_fridays = [datetime(day.year, day.month, day.day, 8, 0, 0) 
                    for week in next_month_cal for day in week 
                    if day.weekday() == calendar.FRIDAY and day.month == next_month 
                    and datetime(day.year, day.month, day.day, 8, 0, 0) >= now]
    
    fridays = this_fridays + next_fridays
    
    near_term, next_term = fridays[0], fridays[1]
        
    return (format_datetime_to_expiry(near_term), format_datetime_to_expiry(next_term), near_term, next_term)

def get_index(currency='BTC'):
    try:
        index_result = api.public_get_index_get(currency)['result'][currency]
        return index_result
    except dbitApi.exceptions.ApiException as e:
        print(e)
        #logger.exception('Exception when calling MarketDataApi->public_get_instruments_get!')
        exit()

def get_instruments_with_expiry(expiry, currency='BTC', kind='option', expired='false'):
    try:
        instrument_result = api.public_get_instruments_get(currency, kind=kind, expired=expired)['result']
        return [instrument['instrument_name'] for instrument in instrument_result if expiry in instrument['instrument_name']]
    except dbitApi.exceptions.ApiException as e:
        print(e)
        #logger.exception('Exception when calling MarketDataApi->public_get_instruments_get!')
        exit()

def get_ticker(instrument):
    try:
        instrument_result = api.public_ticker_get(instrument)['result']
        return instrument_result
    except dbitApi.exceptions.ApiException as e:
        print(e)
        #logger.exception('Exception when calling MarketDataApi->public_get_instruments_get!')
        exit()

def get_bids_asks(near_list, next_list):
    near_calls = dict()
    near_puts = dict()
    next_calls = dict()
    next_puts = dict()

    for instrument in near_list:
        data = get_ticker(instrument)
        best_bid, best_ask = data['best_bid_price'], data['best_ask_price']
        strike, cp = int(instrument.split('-')[2]), instrument.split('-')[3]

        if cp == 'C':
            near_calls[strike] = {'best_bid': best_bid, 'best_ask': best_ask}
        elif cp == 'P':
            near_puts[strike] = {'best_bid': best_bid, 'best_ask': best_ask}
        else:
            print(f'Error {instrument}')

    for instrument in next_list:
        data = get_ticker(instrument)
        best_bid, best_ask = data['best_bid_price'], data['best_ask_price']
        strike, cp = int(instrument.split('-')[2]), instrument.split('-')[3]

        if cp == 'C':
            next_calls[strike] = {'best_bid': best_bid, 'best_ask': best_ask}
        elif cp == 'P':
            next_puts[strike] = {'best_bid': best_bid, 'best_ask': best_ask}
        else:
            print(f'Error {instrument}')

    near_calls_df = pd.DataFrame.from_dict(near_calls, orient='index').sort_index().replace(0, np.nan)
    near_puts_df = pd.DataFrame.from_dict(near_puts, orient='index').sort_index().replace(0, np.nan)
    next_calls_df = pd.DataFrame.from_dict(next_calls, orient='index').sort_index().replace(0, np.nan)
    next_puts_df = pd.DataFrame.from_dict(next_puts, orient='index').sort_index().replace(0, np.nan)

    return near_calls_df, near_puts_df, next_calls_df, next_puts_df

def calculate_indices(time, near_datetime, next_datetime, const_mature_days, R, near_calls_df, near_puts_df, next_calls_df, next_puts_df):
    # Compute strikes with min call/put price difference
    near_prices = pd.DataFrame(index=near_calls_df.index)
    near_prices['call_price'] = (near_calls_df['best_bid'] + near_calls_df['best_ask']) / 2
    near_prices['put_price'] = (near_puts_df['best_bid'] + near_puts_df['best_ask']) / 2
    near_prices['abs_diff'] = abs(near_prices['call_price'] - near_prices['put_price'])

    min_near_strike = near_prices['abs_diff'].idxmin()
    min_near_diff = near_prices.loc[min_near_strike].abs_diff

    next_prices = pd.DataFrame(index=next_calls_df.index)
    next_prices['call_price'] = (next_calls_df['best_bid'] + next_calls_df['best_ask']) / 2
    next_prices['put_price'] = (next_puts_df['best_bid'] + next_puts_df['best_ask']) / 2
    next_prices['abs_diff'] = abs(next_prices['call_price'] - next_prices['put_price'])

    min_next_strike = next_prices['abs_diff'].idxmin()
    min_next_diff = next_prices.loc[min_next_strike].abs_diff

    const_mature_days = 7
    R = 0

    n1 = (near_datetime - time).total_seconds() / 60
    n2 = (next_datetime - time).total_seconds() / 60
    nY = 525600
    n = const_mature_days * 24 * 60

    t1 = n1/nY
    t2 = n2/nY

    # Compute forward prices and at-the-money strikes
    f1 = min_near_strike + np.e**(R*t1) * min_near_diff
    k0_1 = max([strike for strike in near_prices.index if strike <= min_near_strike])

    f2 = min_next_strike + np.e**(R*t2) * min_next_diff
    k0_2 = max([strike for strike in next_prices.index if strike <= min_next_strike])

    near_otm_puts_df = near_puts_df.loc[:k0_1][:-1]
    near_otm_calls_df = near_calls_df.loc[k0_1:][1:]
    next_otm_puts_df = next_puts_df.loc[:k0_2][:-1]
    next_otm_calls_df = next_calls_df.loc[k0_2:][1:]

    near_otm_puts_df = near_otm_puts_df.sort_index(ascending=False)
    near_otm_puts_df = near_otm_puts_df.assign(zero_bid=lambda df: (df['best_bid'] == 0).astype(int))
    near_otm_puts_df['zero_bid_cumsum'] = near_otm_puts_df['zero_bid'].cumsum()
    near_otm_puts_df = near_otm_puts_df[(near_otm_puts_df['zero_bid_cumsum'] <= 2) & (near_otm_puts_df['best_bid'] > 0)]
    
    near_otm_calls_df = near_otm_calls_df.assign(zero_bid=lambda df: (df['best_bid'] == 0).astype(int))
    near_otm_calls_df['zero_bid_cumsum'] = near_otm_calls_df['zero_bid'].cumsum()
    near_otm_calls_df = near_otm_calls_df[(near_otm_calls_df['zero_bid_cumsum'] <= 2) & (near_otm_calls_df['best_bid'] > 0)]

    next_otm_puts_df = next_otm_puts_df.sort_index(ascending=False)
    next_otm_puts_df = next_otm_puts_df.assign(zero_bid=lambda df: (df['best_bid'] == 0).astype(int))
    next_otm_puts_df['zero_bid_cumsum'] = next_otm_puts_df['zero_bid'].cumsum()
    next_otm_puts_df = next_otm_puts_df[(next_otm_puts_df['zero_bid_cumsum'] <= 2) & (next_otm_puts_df['best_bid'] > 0)]

    next_otm_calls_df = next_otm_calls_df.assign(zero_bid=lambda df: (df['best_bid'] == 0).astype(int))
    next_otm_calls_df['zero_bid_cumsum'] = next_otm_calls_df['zero_bid'].cumsum()
    next_otm_calls_df = next_otm_calls_df[(next_otm_calls_df['zero_bid_cumsum'] <= 2) & (next_otm_calls_df['best_bid'] > 0)]

    near_calc_strikes_df = pd.DataFrame(index=near_prices.index)
    near_calc_strikes_df['price'] = (near_otm_puts_df['best_bid'] + near_otm_puts_df['best_ask']) / 2
    near_calc_strikes_df['price'] = near_calc_strikes_df.price.combine_first((near_otm_calls_df['best_bid'] + near_otm_calls_df['best_ask']) / 2)
    near_calc_strikes_df.at[k0_1] = (near_prices.loc[k0_1].call_price + near_prices.loc[k0_1].put_price) / 2
    near_calc_strikes_df = near_calc_strikes_df.dropna()

    next_calc_strikes_df = pd.DataFrame(index=next_prices.index)
    next_calc_strikes_df['price'] = (next_otm_puts_df['best_bid'] + next_otm_puts_df['best_ask']) / 2
    next_calc_strikes_df['price'] = next_calc_strikes_df.price.combine_first((next_otm_calls_df['best_bid'] + next_otm_calls_df['best_ask']) / 2)
    next_calc_strikes_df.at[k0_2] = (next_prices.loc[k0_2].call_price + next_prices.loc[k0_2].put_price) / 2
    next_calc_strikes_df = next_calc_strikes_df.dropna()

    near_sum = 0
    for i in range(len(near_calc_strikes_df)):
        row = near_calc_strikes_df.iloc[i]
        if i == 0:
            deltaKi = near_calc_strikes_df.iloc[i+1].name - row.name
        elif i == len(near_calc_strikes_df) - 1:
            deltaKi = row.name - near_calc_strikes_df.iloc[i-1].name
        else:
            deltaKi = (near_calc_strikes_df.iloc[i+1].name + near_calc_strikes_df.iloc[i-1].name) / 2

        near_sum += deltaKi/(row.name ** 2) * np.e**(R*t1) * row.price
        
    next_sum = 0
    for i in range(len(next_calc_strikes_df)):
        row = next_calc_strikes_df.iloc[i]
        if i == 0:
            deltaKi = next_calc_strikes_df.iloc[i+1].name - row.name
        elif i == len(next_calc_strikes_df) - 1:
            deltaKi = row.name - next_calc_strikes_df.iloc[i-1].name
        else:
            deltaKi = (next_calc_strikes_df.iloc[i+1].name + next_calc_strikes_df.iloc[i-1].name) / 2
        
        next_sum += deltaKi/(row.name ** 2) * np.e**(R*t2) * row.price
        
    sigma1 = ((2/t1) * near_sum) - (1/t1)*((f1/k0_1 - 1)**2)
    sigma2 = ((2/t2) * next_sum) - (1/t2)*((f2/k0_2 - 1)**2)

    #print(near_calc_strikes_df)

    VXBT = 100 * np.sqrt(((t1*sigma1**2)*((n2-n)/(n2-n1)) + (t2*sigma2**2)*((n-n1)/(n2-n1)))*(nY/n))

    omega = ((n2-nY)/(n2-n1))*n
    sigma1_a = sigma1 * (f1**-2)
    sigma2_a = sigma2 * (f2**-2)

    GVXBT = np.sqrt(omega*t1*sigma1 + (1-omega)*t2*sigma2)
    AVXBT = np.sqrt(omega*t1*sigma1_a + (1-omega)*t2*sigma2_a)
    
    return VXBT, GVXBT, AVXBT

def get_indices(live=True, time=None, near_next_cp_data=None):
    if live:
        now = datetime.now()
        near_expiry, next_expiry, near_datetime, next_datetime = get_near_next_terms(now)
        near_instruments = get_instruments_with_expiry(near_expiry)
        next_instruments = get_instruments_with_expiry(next_expiry)

        near_calls_df, near_puts_df, next_calls_df, next_puts_df = get_bids_asks(near_instruments, next_instruments)
        
        VXBT, GVXBT, AVXBT = calculate_indices(now, near_datetime, next_datetime, 7, 0, near_calls_df, near_puts_df, next_calls_df, next_puts_df)

    return VXBT, GVXBT, AVXBT


if __name__ == '__main__':
    print(get_indices())
import glob
import json
import os
import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt

from vxbt_calc import vxbt_calc
#from datetime import datetime

capi_data_path = '/path/to/coinapi_csvs'

start_c = pd.to_datetime('2019-05-01 00:00:00')
end_c = pd.to_datetime('2020-05-01 00:00:00')

instrument_start_end = dict()
capi_orderbook_data = dict()

capi_indices_df = pd.DataFrame(columns=['timestamp', 'vxbt', 'gvxbt', 'avxbt'])
results = {}

def read_orderbook_data(csv_paths, expiry, coinapi=False, data_dict=dict()):
    if expiry not in data_dict:
        data_dict[expiry] = dict()
    else:
        # Already read
        return data_dict
    
    near_next_csv = list()
    for path in csv_paths:
        near_next_csv += glob.glob(path + f'BTC-{expiry}-*-*.csv')

    #if len(near_next_csv) == 0:
    #    raise ValueError(f'{expiry} data unavailable!')
        
    print(f'Reading {expiry} data from disk...')

    for file_path in near_next_csv:
        instrument = os.path.basename(file_path).split('-')
        exp, strike, cp = instrument[1], int(instrument[2]), instrument[3].split('.')[0]  

        if strike not in data_dict[exp]:
            data_dict[exp][strike] = dict()
        try:    
            df = pd.read_csv(file_path).filter(['timestamp', 'best_bid_price', 'best_ask_price'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
            df = df.set_index('timestamp').drop_duplicates()
            if coinapi:
                df.loc[instrument_start_end[exp]['start']] = [np.nan, np.nan]
                df.loc[instrument_start_end[exp]['end']] = [np.nan, np.nan]
                df = df.sort_index()
                df_resampled = df.resample('5min').ffill().fillna(method='bfill')#s.dropna()
            else:
                df_resampled = df.resample('5min').last().dropna()
        except Exception as e:
            print('ERROR', file_path, e)
            
        data_dict[exp][strike][cp] = df_resampled
        
    return data_dict

def build_dataframes(time, near_expiry, next_expiry, data_dict):
    near_calls = dict()
    near_puts = dict()
    next_calls = dict()
    next_puts = dict()

    index_price = index_df[' Price'].loc[time]
    
    for strike in data_dict[near_expiry]:
        try:
            near_calls[strike] = data_dict[near_expiry][strike]['C'].loc[time].astype(float) * index_price
        except KeyError:
            pass

        try:
            near_puts[strike] = data_dict[near_expiry][strike]['P'].loc[time].astype(float) * index_price
        except KeyError:
            pass
    
    for strike in data_dict[next_expiry]:
        try:
            next_calls[strike] = data_dict[next_expiry][strike]['C'].loc[time].astype(float) * index_price
        except KeyError:
            pass

        try:
            next_puts[strike] = data_dict[next_expiry][strike]['P'].loc[time].astype(float) * index_price
        except KeyError:
            pass
        
    near_calls_df = pd.DataFrame.from_dict(near_calls, orient='index').sort_index().replace(0, np.nan).rename(columns={'best_bid_price': 'best_bid', 'best_ask_price': 'best_ask'})
    near_puts_df = pd.DataFrame.from_dict(near_puts, orient='index').sort_index().replace(0, np.nan).rename(columns={'best_bid_price': 'best_bid', 'best_ask_price': 'best_ask'})
    next_calls_df = pd.DataFrame.from_dict(next_calls, orient='index').sort_index().replace(0, np.nan).rename(columns={'best_bid_price': 'best_bid', 'best_ask_price': 'best_ask'})
    next_puts_df = pd.DataFrame.from_dict(next_puts, orient='index').sort_index().replace(0, np.nan).rename(columns={'best_bid_price': 'best_bid', 'best_ask_price': 'best_ask'})
    
    return near_calls_df, near_puts_df, next_calls_df, next_puts_df

index_df = pd.read_csv('/path/to/deribit_btc_usd_index_19-05-01_20-05-31_5min.csv')
index_df['Date and Time'] = pd.to_datetime(index_df['Date and Time'])
index_df = index_df.set_index('Date and Time')

now = pd.to_datetime('2019-05-01 00:00:00')
end = pd.to_datetime('2020-05-01 00:00:00')

while now <= end:
    for expiry in vxbt_calc.get_near_next_terms(now)[:2]:
        if expiry in instrument_start_end:
            instrument_start_end[expiry]['end'] = now + datetime.timedelta(minutes=15)
        else:
            instrument_start_end[expiry] = {'start': now - datetime.timedelta(hours=1, minutes=15), 'end': ''}
            
    now += datetime.timedelta(hours=1)

now_c = start_c

while now_c <= end_c:
    near_expiry, next_expiry, near_datetime, next_datetime = vxbt_calc.get_near_next_terms(now_c)
    
    capi_orderbook_data = read_orderbook_data([capi_data_path], near_expiry, coinapi=True, data_dict=capi_orderbook_data)
    capi_orderbook_data = read_orderbook_data([capi_data_path], next_expiry, coinapi=True, data_dict=capi_orderbook_data)
    
    if not capi_orderbook_data[near_expiry] or not capi_orderbook_data[next_expiry]:
        print(f'WARNING: Insufficient data at {now_c}')
        now_c += datetime.timedelta(minutes=5)
        continue

    try:
        print(f'At {str(now_c)}', end='\r')
        dfs = build_dataframes(now_c, near_expiry, next_expiry, capi_orderbook_data)
        VXBT, GVXBT, AVXBT = vxbt_calc.get_indices(live=False, time=now_c, dfs=dfs)
            
        #capi_indices_df = capi_indices_df.append({'timestamp': now_c, 'vxbt': VXBT, 'gvxbt': GVXBT, 'avxbt': AVXBT}, ignore_index=True)
        results[str(now_c)] = [VXBT, GVXBT, AVXBT]
    except IndexError as e:
        print(f'WARNING: Insufficient data at {now_c}, {repr(e)}')
    '''except Exception as e:
        print(f'WARNING: Unhandled error at {now_c}')
        print(repr(e))
    '''
    
    now_c += datetime.timedelta(minutes=5)

with open('/path/to/output.json', 'w') as f:
    f.write(json.dumps(results))

df = pd.DataFrame.from_dict(results, orient='index')
df.plot()
plt.show()
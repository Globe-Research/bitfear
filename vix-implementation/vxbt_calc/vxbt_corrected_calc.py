import calendar
import glob
import numpy as np
import os
import pandas as pd
import asyncio

from time import time
import logging
from datetime import datetime, timedelta, timezone, date


def filter_otm_options(input_df):
    input_df = input_df.assign(zero_bid=lambda df: (
        df['best_bid'].isna()).astype(int))
    input_df_to_include = []
    for i in range(input_df.shape[0] - 1):
        if input_df.iloc[i:i+2, 2].sum() == 2:
            break
        elif input_df.iloc[i, 2] != 1:
            input_df_to_include.append(i)
    return input_df.iloc[input_df_to_include, :2]


def calculate_indices(time, near_datetime, next_datetime, const_mature_days, near_rate, next_rate, near_calls_df, near_puts_df, next_calls_df, next_puts_df):
    # Compute strikes with min call/put price difference
    near_calls_df = near_calls_df.replace(0, np.nan)
    near_puts_df = near_puts_df.replace(0, np.nan)
    next_calls_df = next_calls_df.replace(0, np.nan)
    next_puts_df = next_puts_df.replace(0, np.nan)

    near_prices = pd.DataFrame(index=near_calls_df.index)
    near_prices['call_price'] = (
        near_calls_df['best_bid'] + near_calls_df['best_ask']) / 2
    near_prices['put_price'] = (
        near_puts_df['best_bid'] + near_puts_df['best_ask']) / 2
    near_prices['abs_diff'] = abs(
        near_prices['call_price'] - near_prices['put_price'])
    near_prices['diff'] = near_prices['call_price'] - \
        near_prices['put_price']

    min_near_strike = near_prices['abs_diff'].idxmin()
    min_near_diff = near_prices.loc[min_near_strike]['diff']

    next_prices = pd.DataFrame(index=next_calls_df.index)
    next_prices['call_price'] = (
        next_calls_df['best_bid'] + next_calls_df['best_ask']) / 2
    next_prices['put_price'] = (
        next_puts_df['best_bid'] + next_puts_df['best_ask']) / 2
    next_prices['abs_diff'] = abs(
        next_prices['call_price'] - next_prices['put_price'])
    next_prices['diff'] = next_prices['call_price'] - \
        next_prices['put_price']

    min_next_strike = next_prices['abs_diff'].idxmin()
    min_next_diff = next_prices.loc[min_next_strike]['diff']

    n1 = (near_datetime - time).total_seconds() / 60
    n2 = (next_datetime - time).total_seconds() / 60
    nY = 525600
    n = const_mature_days * 24 * 60

    t1 = n1/nY
    t2 = n2/nY

    # Compute forward prices and at-the-money strikes
    f1 = min_near_strike + (np.e**(near_rate*t1)) * min_near_diff
    k0_1 = max(
        [strike for strike in near_prices.index if strike <= f1])

    f2 = min_next_strike + (np.e**(next_rate*t2)) * min_next_diff
    k0_2 = max(
        [strike for strike in next_prices.index if strike <= f2])
    near_otm_puts_df = filter_otm_options(
        near_puts_df.loc[:k0_1].iloc[:-1].sort_index(ascending=False))
    near_otm_calls_df = filter_otm_options(
        near_calls_df.loc[k0_1:].iloc[1:])
    next_otm_puts_df = filter_otm_options(
        next_puts_df.loc[:k0_2].iloc[:-1].sort_index(ascending=False))
    next_otm_calls_df = filter_otm_options(
        next_calls_df.loc[k0_2:].iloc[1:])

    near_calc_strikes_df = pd.DataFrame(index=near_prices.index)
    near_calc_strikes_df['price'] = (
        near_otm_puts_df['best_bid'] + near_otm_puts_df['best_ask']) / 2
    near_calc_strikes_df['price'] = near_calc_strikes_df.price.combine_first(
        (near_otm_calls_df['best_bid'] + near_otm_calls_df['best_ask']) / 2)
    near_calc_strikes_df.at[k0_1] = (
        near_prices.loc[k0_1].call_price + near_prices.loc[k0_1].put_price) / 2
    near_calc_strikes_df = near_calc_strikes_df.dropna()

    next_calc_strikes_df = pd.DataFrame(index=next_prices.index)
    next_calc_strikes_df['price'] = (
        next_otm_puts_df['best_bid'] + next_otm_puts_df['best_ask']) / 2
    next_calc_strikes_df['price'] = next_calc_strikes_df.price.combine_first(
        (next_otm_calls_df['best_bid'] + next_otm_calls_df['best_ask']) / 2)
    next_calc_strikes_df.at[k0_2] = (
        next_prices.loc[k0_2].call_price + next_prices.loc[k0_2].put_price) / 2
    next_calc_strikes_df = next_calc_strikes_df.dropna()

    near_calc_strikes_df['delta_k'] = 0
    near_calc_strikes_df['contribution'] = 0
    for i in range(len(near_calc_strikes_df)):
        row = near_calc_strikes_df.iloc[i]
        if i == 0:
            delta_k = near_calc_strikes_df.iloc[i+1].name - row.name
        elif i == len(near_calc_strikes_df) - 1:
            delta_k = row.name - near_calc_strikes_df.iloc[i-1].name
        else:
            delta_k = (
                near_calc_strikes_df.iloc[i+1].name - near_calc_strikes_df.iloc[i-1].name) / 2
        near_calc_strikes_df.iloc[i, 1] = delta_k
        near_calc_strikes_df.iloc[i, 2] = delta_k/(row.name ** 2) * \
            np.e**(near_rate*t1) * row.price

    next_calc_strikes_df['delta_k'] = 0
    next_calc_strikes_df['contribution'] = 0
    for i in range(len(next_calc_strikes_df)):
        row = next_calc_strikes_df.iloc[i]
        if i == 0:
            delta_k = next_calc_strikes_df.iloc[i+1].name - row.name
        elif i == len(next_calc_strikes_df) - 1:
            delta_k = row.name - next_calc_strikes_df.iloc[i-1].name
        else:
            delta_k = (
                next_calc_strikes_df.iloc[i+1].name - next_calc_strikes_df.iloc[i-1].name) / 2
        next_calc_strikes_df.iloc[i, 1] = delta_k
        next_calc_strikes_df.iloc[i, 2] = delta_k/(row.name ** 2) * \
            np.e**(next_rate*t1) * row.price

    sigma1 = (
        (2/t1) * near_calc_strikes_df['contribution'].sum()) - (1/t1)*((f1/k0_1 - 1)**2)
    sigma2 = (
        (2/t2) * next_calc_strikes_df['contribution'].sum()) - (1/t2)*((f2/k0_2 - 1)**2)

    VIX = 100 * np.sqrt(((t1*sigma1)*((n2-n)/(n2-n1)) +
                         (t2*sigma2)*((n-n1)/(n2-n1)))*(nY/n))
    if np.isnan(VIX):
        raise ValueError("VIX is not a number : {}".format(VIX))
    return VIX


def test_verify_fixed_vix_from_CBOE_white_paper():
    maturity = 30
    near_rate = 0.000305
    next_rate = 0.000286

    current = datetime(2020, 6, 1, 9, 46)
    near_exp_date = datetime(2020, 6, 26, 8, 30)
    next_exp_date = datetime(2020, 7, 3, 15, 0)

    datetime(2020, 6, 1, 9, 46) + timedelta(minutes=35924)

    near_data = pd.read_csv(
        './test_data/cboe_near_term.csv', engine='python')
    next_data = pd.read_csv(
        './test_data/cboe_next_term.csv', engine='python')

    near_calls_df = near_data.set_index('strike')[['calls_bid', 'calls_ask']].rename(
        {'calls_bid': 'best_bid', 'calls_ask': 'best_ask'}, axis=1).astype('float')
    near_puts_df = near_data.set_index('strike')[['puts_bid', 'puts_ask']].rename(
        {'puts_bid': 'best_bid', 'puts_ask': 'best_ask'}, axis=1).astype('float')

    next_calls_df = next_data.set_index('strike')[['calls_bid', 'calls_ask']].rename(
        {'calls_bid': 'best_bid', 'calls_ask': 'best_ask'}, axis=1).astype('float')
    next_puts_df = next_data.set_index('strike')[['puts_bid', 'puts_ask']].rename(
        {'puts_bid': 'best_bid', 'puts_ask': 'best_ask'}, axis=1).astype('float')

    VIX = calculate_indices(time=current,
                            near_datetime=near_exp_date,
                            next_datetime=next_exp_date,
                            const_mature_days=maturity,
                            near_rate=near_rate,
                            next_rate=next_rate,
                            near_calls_df=near_calls_df,
                            near_puts_df=near_puts_df,
                            next_calls_df=next_calls_df,
                            next_puts_df=next_puts_df)
    if round(VIX, 2) == 13.69:
        print('Passed: ')
        print('{} == {}'.format(round(VIX, 2), 13.69))
    else:
        print('Test Failed:')
        print('[ERORR] {} == {}'.format(round(VIX, 2), 13.69))

    assert round(VIX, 2) == 13.69


test_verify_fixed_vix_from_CBOE_white_paper()

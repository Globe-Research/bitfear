import pandas as pd
import numpy as np
import math
from openpyxl import load_workbook

# Dictionary of expiry dates hard coded as historical expiry dates are not readily available
expdct = {'10APR20': 1586505600000,
          '17APR20': 1587110400000,
          '24APR20': 1587715200000
          }


# Arbitrary start time
rando_start = 1586433719209

path = ("path to formatted data using groupbyexp")
data_destination = ("data destination")


columnsToDrop = ['underlying_price', 'timestamp', 'state', 'settlement_price', 'open_interest',	'min_price',
                 'max_price',	'mark_price',	'mark_iv', 'last_price', 'interest_rate',	'instrument_name',
                 'index_price',	'change_id',	'bids',	'bid_iv', 'best_bid_amount',	'best_ask_amount',
                 'asks',	'ask_iv',	'24h_high',	'24h_low', '24h_vol',	'theta',	'delta',	'rho',
                 'gamma',	'vega']



def getSigma(df, timeStamp, optionExpDate, columnsToDrop):

    # get the time to expiration in minutes
    expTime = expdct[optionExpDate]
    N = (expTime - timeStamp)/(1000 * 60)
    T = N/525600

    # formatting
    CP = df['instrument_name'].str
    df['CP'] = CP[-1:]
    df['strike'] = CP.extract('([0-9][0-9][0-9]+)').astype(int)
    df = df.drop(columns=columnsToDrop)
    df = df.sort_values(['CP', 'strike']).reset_index()
    df['mid'] = (df['best_bid_price'] + df['best_ask_price']) / 2
    dfTemp = df.copy()

    # calculating F and K
    dfTemp.set_index(['CP', 'strike'], inplace=True)
    dfTemp = dfTemp[dfTemp['best_bid_price'] > 0]['mid'].unstack('CP')
    dfTemp['diff'] = np.absolute(np.array(dfTemp['C']) - np.array((dfTemp['P'])))
    # Might potentially get IndexError on the next line. I think its when there is no minimum. ToDo
    strike = dfTemp.index[np.where(dfTemp['diff'] == np.amin(dfTemp['diff']))[0][0]]
    # Have to check if this multiplier is needed
    eRT = math.exp(N * 0.001)
    F = strike + (eRT * np.amin(dfTemp['diff']))
    dfTemp = dfTemp[dfTemp.index < F]
    K = dfTemp.index[dfTemp.shape[0] - 1]

    # selecting out of money option
    P = df[df['CP'] == 'P']
    strike_index = int(np.where((P['strike'] == K) == True)[0])
    oomPut = (P['best_bid_price'] != P['best_bid_price']).tolist()
    putCutoff = 0
    for i in range(strike_index):
        if(oomPut[i] == oomPut[i+1] and oomPut[i] == True):
            putCutoff = i+1
            continue
    P = P.iloc[putCutoff+1:]
    keep = np.array(P['strike'] > K-1) + np.array(P['best_bid_price'] != 0)
    P = P[keep].reset_index()
    C = df[df['CP'] == 'C']
    oomCall = (C['best_bid_price'] != C['best_bid_price']).tolist()
    callCutOff = C.shape[0]
    for i in range((len(oomCall)-1),strike_index,-1):
        if(oomCall[i] == oomCall[i-1] and oomPut[i] == True):
            callCutOff = i-1
            continue
    C = C.iloc[:callCutOff]
    keep = np.array(C['strike'] < K) + np.array(C['best_bid_price'] != 0)
    C = C[keep].reset_index()
    P_put = int(np.where((P['strike'] == K) == True)[0])
    # TypeError: only size-1 arrays can be converted to Python scalars. Not sure why ToDo
    C_call = int(np.where((C['strike'] == K) == True)[0])
    mid = P['mid'][:P_put].tolist() + [(P['mid'][P_put] + C['mid'][C_call])/2] + C['mid'][C_call+1:].tolist()
    df_mid = pd.merge(P, C, on='strike', how='inner')

    # step 2 formula part
    strike = df_mid['strike'].tolist()
    sum = 0
    for i in range(len(strike)):
        if i == 0:
            delta_strike = strike[i+1] - strike[i]
        elif i == len(strike)-1:
            delta_strike = strike[i] - strike[i-1]
        else:
            delta_strike = (strike[i-1] + strike[i+1])/2
        sum += (delta_strike) * eRT * mid[i]

    sigma = (2 * sum - ((F/K) - 1)**2) / (T * (F**2))

    return N, sigma


def calculateVix(N1, sum1, N2, sum2):
    try:
        intermediate = ((N1 * sum1 * ((N2 - 10080)/(N2 - N1))) + (N2 * sum2 * ((10080 - N1)/(N2 - N1)))) * (1/10080)
        return 100 * math.sqrt(intermediate)
    except ZeroDivisionError:
        return 0


def closest(timestamp, path, nextDate, i):
    #Finding the timestamp that is closest to the 15 minute interval we use
    smallestDiff = math.inf
    while True:
        t = int(mean(list(pd.read_csv(path + "//" + str(nextDate) + "_" + str(i) + ".csv")['timestamp'])))
        diff = abs(timestamp - t)
        if diff > smallestDiff:
            return i - 1
        else:
            smallestDiff = diff
            i += 1


def mean(lst):
    # Find the mean of a list. Needed to create my own implementation as there is a stray string in one of the options
    # that needs to be handled
    acc = 0
    lgth = len(lst)
    for num in lst:
        try:
            acc += int(num)
        except ValueError:
            lgth -= 1
    return acc/len(lst)


dates = list(expdct.keys())

dateIndex = 0
counter1 = closest(rando_start, path, dates[0],0)
counter2 = closest(rando_start, path, dates[1],0)

time = rando_start

lst = []
timelst = []

while True:
    try:
        option1 = path + "//" + str(dates[dateIndex]) + "_" + str(counter1) + ".csv"
        option2 = path + "//" + str(dates[dateIndex+1]) + "_" + str(counter2) + ".csv"
        df1 = pd.read_csv(option1).replace(0, np.nan)
        N1, sum1 = getSigma(df1, time, dates[dateIndex], columnsToDrop)
        df2 = pd.read_csv(option2).replace(0, np.nan)
        N2, sum2 = getSigma(df2, time, dates[dateIndex+1], columnsToDrop)
        y = calculateVix(N1, sum1, N2, sum2)
    # IndexError from line 48, TypeError from line 79
    except (IndexError, TypeError):
        y = np.nan
    lst += [y]
    timelst += [time]
    time += 900000
    try:
        counter1 = closest(time, path, dates[dateIndex], counter1+1)
        counter2 = closest(time, path, dates[dateIndex+1], counter2+1)
    except FileNotFoundError:
        # FileNotFoundError is thrown when an option expires and we overshoot the index range.
        # Thus we change the expiry dates
        counter1 = counter2
        option1 = option2
        if dateIndex + 2 >= len(dates):
            break
        else:
            counter2 = closest(time, path, dates[dateIndex+2], 0)
            dateIndex += 1


# writing as xlsx file
data = pd.DataFrame({'timeStamp':timelst, 'Vix': lst})

book = load_workbook(data_destination)
writer = pd.ExcelWriter(data_destination, engine="openpyxl", mode="a")
writer.book = book
writer.sheets = dict(("Sheet1", ws) for ws in book.worksheets)
pd.DataFrame(data).to_excel(writer)

writer.save()
writer.close()
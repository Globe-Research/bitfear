import pandas as pd
import datetime
import numpy as np
import os
import re
import calendar
import math

path = ("path to data that has been reformatted using my code")


columnsToDrop = ['underlying_price', 'timestamp', 'state', 'settlement_price', 'open_interest',	'min_price',
                 'max_price',	'mark_price',	'mark_iv', 'last_price', 'interest_rate',	'instrument_name',
                 'index_price',	'change_id',	'bids',	'bid_iv', 'best_bid_amount',	'best_ask_amount',
                 'asks',	'ask_iv',	'24h_high',	'24h_low', '24h_vol',	'theta',	'delta',	'rho',
                 'gamma',	'vega']

# U.S Treasury yield curve rates
rates = {1 : 1.55, 2 : 1.57, 3 : 1.57, 6 : 1.57, 12 : 1.48}

def expiry(d1, d2, tmrw, exp, rates):
    # check to see if weekly or standard expiration
    if calendar.Calendar(0).monthdatescalendar(exp.year, exp.month)[3][4].day == exp.day:
        settlement = 510
    else:
        settlement = 900
    d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
    exp = datetime.datetime.strptime(str(exp), "%Y-%m-%d")
    d2 = datetime.datetime.strptime(d2, "%Y-%m-%d %H:%M:%S")
    tmrw = datetime.datetime.strptime(tmrw, "%Y-%m-%d %H:%M:%S")

    # trying to calculate the rates. Will implement a better version soon
    index = exp.month - d1.month
    if index == 0:
        index = 1
    elif index > 3 and index < 6:
        index = 3
    elif index > 6 and index < 12:
        index = 6
    else:
        index = 12

    timeToExp = ((d2 - tmrw).seconds/60 + (exp - d1).days * 24 * 60 + settlement)
    return timeToExp, timeToExp * rates[index]/(1000 * 565400)


def getSigma(df, fileName, columnsToDrop):

    # get the time to expiration
    timeNowWithSecond = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timeNow = datetime.datetime.now().strftime("%Y-%m-%d")
    timeTmrw = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    exp = datetime.datetime.strptime((re.split("_",fileName))[0],'%d%b%y').date()
    N, expTime = expiry(timeNow, timeNowWithSecond, timeTmrw, exp, rates)
    T = N/565400

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
    strike = dfTemp.index[np.where(dfTemp['diff'] == np.amin(dfTemp['diff']))[0][0]]
    eRT = math.exp(expTime)
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
        sum += (delta_strike/(strike[i]**2)) * eRT * mid[i]

    sigma = (1 / T) * (2 * sum - ((F/K) - 1)**2)

    return N, sigma

def calculateVix(N1, sum1, N2, sum2):
    # calculating vix
    T1 = N1/565400
    T2 = N2/565400
    intermediate = ((T1 * sum1 * ((N2 - 43200)/(N2 - N1))) + (T2 * sum2 * ((43200 - N1)/(N2 - N1)))) * (565400/43200)
    return 100 * math.sqrt(intermediate)


test1 = os.listdir(path)[10000]
df1 = pd.read_csv(path + "//" + test1).replace(0, np.nan)
N1, sum1 = getSigma(df1, test1,columnsToDrop)
test2 = os.listdir(path)[15000]
df2 = pd.read_csv(path + "//" + test2).replace(0, np.nan)
N2, sum2 = getSigma(df2, test2,columnsToDrop)
y = calculateVix(N1, sum1, N2, sum2)
print("The vix calculation is " + str(y))
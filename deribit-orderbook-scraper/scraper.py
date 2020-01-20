import csv
import json
import os.path
#import pandas as pd
import openapi_client as dbitApi

from time import sleep

#### PARAMETERS ####
# How often the scraper should retreive the databook for each instrument. Example: setting to 60 means every 60 seconds
resolution = 60

# Filter instruments to scrape
currency = 'BTC'
kind = 'option'
expired = 'false'

# Output destinations
order_data_folder = './deribit-orderbook-scraper/scraped_data'

api = dbitApi.MarketDataApi()

# Get instrument selection
try:
    instrument_result = api.public_get_instruments_get(currency, kind=kind, expired=expired)['result']
except ApiException as e:
    print("Exception when calling MarketDataApi->public_get_instruments_get: {}}\n".format(e))
    exit()

instruments = [instrument['instrument_name'] for instrument in instrument_result]

def scrape(instrument):
    try:
        orderbook_result = api.public_get_order_book_get(instrument)['result']
    except ApiException as e:
        print("Exception when calling MarketDataApi->public_get_instruments_get: {}}\n".format(e))
        return
    
    ''' TO DO: dump JSON to preserve non flat fields (e.g. ask/bid prices, greeks, stats) without manipulation

    with open(order_data_folder + '/{}.json'.format(instrument), 'r+') as f:
        content = json.load(f)
        if not content:
            # Is empty so initialise
            json.dump([], f)
        content = json.load(f)
        content.append(orderbook_result)
        json.dump(content, f)

    '''

    # Flatten all fields and append to CSV

    # Stats
    try:
        orderbook_result['24h_high'] = orderbook_result['stats']['high']
        orderbook_result['24h_low'] = orderbook_result['stats']['low']
        orderbook_result['24h_vol'] = orderbook_result['stats']['volume']
        del orderbook_result['stats']
    except KeyError as e:
        print('{}: {}'.format(instrument, e))

    # Greeks
    try:
        for k, v in orderbook_result['greeks'].items():
            orderbook_result[k] = v
        del orderbook_result['greeks']
    except KeyError as e:
        print('{}: {}'.format(instrument, e))

    # Asks and bids
    orderbook_result['asks'] = str(orderbook_result['asks'])
    orderbook_result['bids'] = str(orderbook_result['bids'])

    filename = order_data_folder + '/{}.csv'.format(instrument)
    csv_exists = os.path.isfile(filename)

    try:
        with open(filename, 'a+', newline='') as f:
            headers = ['underlying_price', 'underlying_index', 'timestamp', 'state', 'settlement_price', 'open_interest', 'min_price', 'max_price', 'mark_price', 'mark_iv', 'last_price', 'interest_rate', 'instrument_name', 'index_price', 'change_id', 'bids', 'bid_iv', 'best_bid_price', 'best_bid_amount', 'best_ask_price', 'best_ask_amount', 'asks', 'ask_iv', '24h_high', '24h_low', '24h_vol', 'theta', 'delta', 'rho', 'gamma', 'vega']
            writer = csv.DictWriter(f, fieldnames=headers)

            if not csv_exists:
                writer.writeheader()

            writer.writerow(orderbook_result)
            print("Successfully updated {}".format(filename))

    except PermissionError:
        # File unwritable for some temporary reason (e.g. been opened by another process) so skip
        print("Skipping {}: file unavailable")

if __name__ == '__main__':
    try:
        while True:
            for instrument in instruments:
                scrape(instrument)
            sleep(resolution)
    except KeyboardInterrupt:
        # Exit gracefully
        exit()

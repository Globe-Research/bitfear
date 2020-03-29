import csv
import json
import logging
import logging.handlers
import os.path
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
order_data_folder = '~/cloud-mounts/deribit_scraped_data'
output_log_path = 'deribit_scraper.log'

#### ---------- ####

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.handlers.RotatingFileHandler(output_log_path, maxBytes=8000000, backupCount=5)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

api = dbitApi.MarketDataApi()

def get_instruments(currency, kind, expired):
    try:
        instrument_result = api.public_get_instruments_get(currency, kind=kind, expired=expired)['result']
        return [instrument['instrument_name'] for instrument in instrument_result]
    except dbitApi.exceptions.ApiException as e:
        logger.exception('Exception when calling MarketDataApi->public_get_instruments_get!')
        exit()

def scrape_orderbook(instrument):
    try:
        orderbook_result = api.public_get_order_book_get(instrument)['result']
    except dbitApi.exceptions.ApiException as e:
        logger.exception('Exception when calling MarketDataApi->public_get_instruments_get!')
        return False

    # Flatten all fields and append to CSV

    # Stats
    try:
        orderbook_result['24h_high'] = orderbook_result['stats']['high']
        orderbook_result['24h_low'] = orderbook_result['stats']['low']
        orderbook_result['24h_vol'] = orderbook_result['stats']['volume']
        orderbook_result['24h_change'] = orderbook_result['stats']['price_change']
        del orderbook_result['stats']
    except KeyError as e:
        logger.exception('{}: Error parsing stats'.format(instrument))

    # Greeks
    try:
        for k, v in orderbook_result['greeks'].items():
            orderbook_result[k] = v
        del orderbook_result['greeks']
    except KeyError as e:
        logger.exception('{}: Error parsing greeks'.format(instrument))

    # Asks and bids
    orderbook_result['asks'] = str(orderbook_result['asks'])
    orderbook_result['bids'] = str(orderbook_result['bids'])

    filename = order_data_folder + '/{}.csv'.format(instrument)
    csv_exists = os.path.isfile(filename)

    try:
        with open(filename, 'a+', newline='') as f:
            headers = ['timestamp', 'instrument_name', 'state', 'index_price', 'underlying_index', 'underlying_price', 'settlement_price', 'estimated_delivery_price', 'delivery_price', 'open_interest', 'bids', 'bid_iv', 'best_bid_price', 'best_bid_amount', 'asks', 'ask_iv', 'best_ask_price', 'best_ask_amount', 'min_price', 'max_price', 'mark_price', 'mark_iv', 'last_price', 'interest_rate', 'change_id', '24h_high', '24h_low', '24h_vol', '24h_change', 'theta', 'delta', 'rho', 'gamma', 'vega']
            writer = csv.DictWriter(f, fieldnames=headers)

            if not csv_exists:
                writer.writeheader()

            writer.writerow(orderbook_result)
            logger.info('Successfully updated {}'.format(filename))

    except PermissionError:
        logger.exception('Skipping {}: file unavailable')

    return orderbook_result['state'] == 'closed'

if __name__ == '__main__':
    logger.info('===================================================')
    logger.info('BitFEAR Deribit Orderbook Scraper')
    logger.info('Params: resolution={} currency={} kind={} expired={}'.format(resolution, currency, kind, expired))

    instruments = get_instruments(currency, kind, expired)
    logger.info('Got {} instruments'.format(len(instruments)))

    while True:
        for instrument in instruments:
            try:
                expired = scrape_orderbook(instrument)
                if expired:
                    instruments.remove(instrument)
            except KeyboardInterrupt:
                logger.info("Exiting...")
                exit()
            except Exception as e:
                logger.exception('Unhandled exception for {}!'.format(instrument))
                logger.info('Skipping {}...'.format(instrument))
                continue 

        logger.info('Updates complete. Waiting {} seconds...'.format(resolution))

        try:
            sleep(resolution)
        except KeyboardInterrupt:
            logger.info("Exiting...")
            exit()

import asyncio
import websockets
import json
import pandas as pd
from openpyxl import load_workbook
import time
import numpy as np

id = 1

BitFearData = pd.DataFrame(columns=["strike", "instrument_name", "bid_price", "ask_price", "open_interest",
                                    "24_hour_volume", "index_price", "direction", "amount"])

data_destination = (r"C:\Users\ggoh\Documents\bitfearData.xlsx")


async def call_api(msg, dataframe, identifier):
    # To get different parameters such as bid_price, a new message needs to be sent to the derebit api
    # The data will come back in a JSON format
    # currently the way to get the data back from the JSON block is hard coded. Will try to change this in future implementations
    # the messages being sent to the api are also hard coded but im not sure if there is much we can do about it
    async with websockets.connect('wss://testapp.deribit.com/ws/api/v2') as websocket:
        await websocket.send(msg)
        while websocket.open:
            response = await websocket.recv()
            json_response = json.loads(response)
            # getting the types of options
            if identifier == "get_instruments":
                # dictionary of option and individual strike prices for each option
                instrument_names = {instrumentNames['instrument_name']: instrumentNames['strike'] for instrumentNames in json_response['result']}
                return instrument_names
            # getting the bid price, ask price and open interest
            if identifier == "get_book_summary_by_instrument":
                result = json_response['result'][0]
                dataframe["instrument_name"] = [result["instrument_name"]]
                dataframe["bid_price"] = [result["bid_price"]]
                dataframe["ask_price"] = [result["ask_price"]]
                dataframe["open_interest"] = [result["open_interest"]]
                return dataframe
            # getting index price, direction and amount
            if identifier == "trades_by_instrument":
                try:
                    # checks to see if trades take place during a certain timeframe
                    trades = json_response['result']['trades'][0]
                    dataframe["index_price"] = trades["index_price"]
                    dataframe["direction"] = trades["direction"]
                    dataframe["amount"] = trades["amount"]
                except IndexError:
                    # if no trades there will be an index error
                    dataframe["index_price"] = None
                    dataframe["direction"] = None
                    dataframe["amount"] = None
                return dataframe
            return None

# this for loop will be modified later on depending on the amount of data we need
# It is just to test with a smaller number of iterations first
for i in range(1):
    time.sleep(1)
    # creating data for bitFearData row by row
    BitFearRowData = pd.DataFrame()

    # getting the names of current Bitcoin options
    msg_get_instruments = \
        {
            "jsonrpc": "2.0",
            "id": id,
            "method": "public/get_instruments",
            "params": {
                "currency": "BTC",
                "kind": "option",
                "expired": 'false'
            }
        }

    instruments = asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg_get_instruments), None, "get_instruments"))

    for options in instruments:

        msg_get_book_summary_by_instrument = \
            {
                "jsonrpc": "2.0",
                "id": id,
                "method": "public/get_book_summary_by_instrument",
                "params": {
                    "instrument_name": options
                }
            }
        BitFearRowData["strike"] = instruments[options]
        call_api_summary_by_instruments = call_api(json.dumps(msg_get_book_summary_by_instrument), BitFearRowData, "get_book_summary_by_instrument")
        BitFearRowData = asyncio.get_event_loop().run_until_complete(call_api_summary_by_instruments)

        msg_last_trades_by_instrument = \
            {
                "jsonrpc": "2.0",
                "id": id,
                "method": "public/get_last_trades_by_instrument",
                "params": {
                    "instrument_name": options,
                    "count": 1
                }
            }

        call_api_trades_by_instrument = call_api(json.dumps(msg_last_trades_by_instrument), BitFearRowData, "trades_by_instrument")
        BitFearRowData = asyncio.get_event_loop().run_until_complete(call_api_trades_by_instrument)

        msg_ticker = \
            {
                "jsonrpc": "2.0",
                "id": id,
                "method": "public/ticker",
                "params": {
                    "instrument_name": "instrument"
                }
            }

        call_api_trades_by_instrument = call_api(json.dumps(msg_last_trades_by_instrument), BitFearRowData, "trades_by_instrument")
        BitFearRowData = asyncio.get_event_loop().run_until_complete(call_api_trades_by_instrument)

        # creates data row by row and appends them onto the larger dataframe
        BitFearData = BitFearData.append(BitFearRowData)

# optional, just for me to see data is in order
print(BitFearData)

# prints the data onto an excel worksheet, can be changed to csv in the future
book = load_workbook(data_destination)
writer = pd.ExcelWriter(data_destination, engine="openpyxl", mode="a")
writer.book = book
writer.sheets = dict(("Sheet1", ws) for ws in book.worksheets)
pd.DataFrame(BitFearData).to_excel(writer)

writer.save()
writer.close()

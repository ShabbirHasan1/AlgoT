# local imports
from candle_engine import SpotCandleEngine, OptionsCandleEngine
from strategy import SpotBrain, OptionsBrain, StopLossBrain
from data_engine import SymphonyFintechAPI
from logging_config import CustomLogger
import constants
from datetime import datetime
# library imports
import asyncio
import json
import sys

DF_15MIN = None
DF_OPTIONS_DATA = None
SPOT_DF = None
# symphony_api = SymphonyFintechAPI()
# symphony_api.get_headers()

async def get_options_data():
    return DF_OPTIONS_DATA

async def main(instrument_id,equity,strike,wings,base_qty,freeze_qty,series):
    # initialize the symphony api
    symphony_api = SymphonyFintechAPI(series=series)
    symphony_api.get_headers() # loggin in to the symphony api
    
    # initialize the SpotCandleEngine, OptionsCandleEngine
    spot_candle_engine = SpotCandleEngine(symphony_api=symphony_api)
    options_candle_engine = OptionsCandleEngine(symphony_api=symphony_api,equity=equity, series=series)
    print('8')
    response_df = await spot_candle_engine.fetch_ohlc_once(exchangeInstrumentID=int(instrument_id), lookbackDays=constants.DF_15MIN_BACK_DAYS) # getting spot data once
    print('9')
    await spot_candle_engine.update_dataframe_15min_ohlc(response_df)
    print('10')
    latest_candle = await spot_candle_engine.get_latest_15min_candle()
    print('11') 
    close_price = latest_candle['Close']
    print('12')
    # get the options instruments
    strike=int(strike)
    options_instruments = await options_candle_engine.get_options_instruments(spot_close_price=close_price,strike=strike,symbol=equity)
    
    print('13')
    # initialize the SpotBrain
    spot_brain = SpotBrain(equity=equity)

    # initialize the OptionsBrain
    options_brain = OptionsBrain(equity=equity,strike=strike,wings=wings)
    await options_brain.populate_map_strike_instrument(options_candle_engine.map_strike_instrument)

    # initialze the stopLossBrain
    stop_loss_brain = StopLossBrain(symphony_api=symphony_api,equity=equity)


    
    exchangeInstrumentID = int(instrument_id)
    # Start the candle engine and run SpotBrain concurrently
    await asyncio.gather(
        spot_candle_engine.run(exchangeInstrumentID),
        #options_candle_engine.run(),
        spot_brain_loop(spot_brain, spot_candle_engine),
        options_brain_loop(options_brain, options_candle_engine),
        stop_loss_brain_loop(stop_loss_brain)
    )

async def spot_brain_loop(spot_brain, spot_candle_engine):
    global DF_15MIN, SPOT_DF
    while True:
        # Get the dataframe from SpotCandleEngine
        DF_15MIN = await spot_candle_engine.get_df_15min()
        await spot_brain.populate_dataframe(DF_15MIN)
        await spot_brain.run()
        SPOT_DF = await spot_brain.get_df()
        await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)

async def options_brain_loop(options_brain, options_candle_engine):
    global DF_OPTIONS_DATA, SPOT_DF
    while True:
        # Get the dataframe from OptionsCandleEngine
        # DF_OPTIONS_DATA = await options_candle_engine.get_df_options_data()
        await options_brain.populate_dataframe(SPOT_DF)
        await options_brain.run()
        DF_OPTIONS_DATA = await options_brain.get_df()
        # position = await DF_OPTIONS_DATA.iloc[-1]['position']
        # with open(f'daily_jsons/{equity}.json', 'r') as f:
        #     data = await json.load(f)
        # data['Position'] = position

        # with open(f'daily_jsons/{equity}.json', 'r') as file:
        #     json.dump(data, file, indent=4)

        # options_df = await options_brain.get_df()
        await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)

async def stop_loss_brain_loop(stop_loss_brain):
    while True:
        await stop_loss_brain.run()
        await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)


def get_equity_data(equity):
    with open(constants.EQUITY_DATA_PATH, 'r') as f:
        data = json.load(f)
        return data[equity]


if __name__ == "__main__":
    try:
        equity = sys.argv[1].upper()
        equity_data = get_equity_data(equity)
        print(equity)
        print(equity_data)
        instrument_id = equity_data['exchangeInstrumentID']
        strike = equity_data['Strike']
        wings = equity_data['Wings']
        base_qty = equity_data['base_qty']
        freeze_qty = equity_data['freeze_qty']
        series = equity_data['series']
        if equity == 'NIFTY':
            today=datetime.today().weekday()
            print(f"{today= }"*10)
            print(today)
            if today==0:
                wings = 400
            elif today==1:
                wings = 300
            elif today==2:
                wings = 200
            elif today==3:
                wings = 100
            elif today==4:
                wings = 400
            elif today==5:
                wings = 400
            elif today==6:
                wings = 400
            
            print(f"{wings= }"*10)
        asyncio.run(main(instrument_id=instrument_id,equity=equity,strike=strike,wings=wings,freeze_qty=freeze_qty,base_qty=base_qty,series=series), debug=True)
    except Exception as e:
        print(e)
        logger = CustomLogger('main.py')
        logger.log_message('error', f"Error in main.py: {e}")

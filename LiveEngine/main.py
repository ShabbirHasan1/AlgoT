# local imports
from candle_engine import SpotCandleEngine, OptionsCandleEngine
from strategy import SpotBrain, OptionsBrain, StopLossBrain
from data_engine import SymphonyFintechAPI
from logging_config import CustomLogger
import constants

# library imports
import asyncio

DF_15MIN = None
DF_OPTIONS_DATA = None
SPOT_DF = None

async def get_options_data():
    return DF_OPTIONS_DATA


async def main():
    # initialize the symphony api
    symphony_api = SymphonyFintechAPI()
    symphony_api.login() # loggin in to the symphony api

    # initialize the SpotCandleEngine, OptionsCandleEngine
    spot_candle_engine = SpotCandleEngine(symphony_api=symphony_api)
    options_candle_engine = OptionsCandleEngine(symphony_api=symphony_api)

    response_df = await spot_candle_engine.fetch_ohlc_once(exchangeInstrumentID=26000, lookbackDays=constants.DF_15MIN_BACK_DAYS) # getting spot data once
    await spot_candle_engine.update_dataframe_15min_ohlc(response_df)
    latest_candle = await spot_candle_engine.get_latest_15min_candle()
    close_price = latest_candle['Close']

    # get the options instruments
    options_instruments = await options_candle_engine.get_options_instruments(spot_close_price=close_price)

    # initialize the SpotBrain
    spot_brain = SpotBrain()

    # initialize the OptionsBrain
    options_brain = OptionsBrain()
    await options_brain.populate_map_strike_instrument(options_candle_engine.map_strike_instrument)

    # initialze the stopLossBrain
    stop_loss_brain = StopLossBrain(symphony_api=symphony_api)


    

    # Start the candle engine and run SpotBrain concurrently
    await asyncio.gather(
        spot_candle_engine.run(),
        # options_candle_engine.run(),
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
        # options_df = await options_brain.get_df()
        await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)

async def stop_loss_brain_loop(stop_loss_brain):
    while True:
        await stop_loss_brain.run()
        await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)




if __name__ == "__main__":
    try:
        asyncio.run(main(), debug=True)
    except Exception as e:
        print(e)
        logger = CustomLogger('main.py')
        logger.log_message('error', f"Error in main.py: {e}")

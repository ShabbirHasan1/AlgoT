import asyncio
import pandas as pd
from datetime import timedelta
from data_engine import SymphonyFintechAPI
import constants
from logging_config import CustomLogger
from datetime import datetime, timezone
from io import StringIO


class SpotCandleEngine:
    def __init__(self, symphony_api):
        self.logger = CustomLogger('spotCandleEngine')
        self.df_1sec_daily = pd.DataFrame(columns=['Datetime', 'Open', 'High', 'Low', 'Close'])
        self.df_15min = pd.DataFrame(columns=['Datetime', 'Open', 'High', 'Low', 'Close'])
        self.symphony_api = symphony_api
        self.formatted_quote = None
        
        self.spot_instruments = [{
        'exchangeSegment': 2,
        'exchangeInstrumentID': 14732,
        }]

    async def fetch_quotes(self, instruments, message_code):
        try:
            while True:
                response = symphony_api.get_quote(instruments, message_code=message_code)
                self.formatted_quote = symphony_api.format_spot_quote(response)
                await self.update_dataframe_1sec_daily(self.formatted_quote)
                await self.insert_quote_to_database(self.formatted_quote)
                await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)
        except Exception as e:
            self.logger.log_message('error', f"Failed to fetch quotes: {e}")

    async def update_dataframe_1sec_daily(self, quote):
        try:
            # Convert JSON keys to DataFrame columns
            new_row = {
                'Datetime': pd.to_datetime(quote['time'], format='%Y-%m-%d %H:%M:%S'),
                'Open': quote['open_price'],
                'High': quote['high_price'],
                'Low': quote['low_price'],
                'Close': quote['close_price']
            }
            # Add the new row to the DataFrame
            self.df_1sec_daily = pd.concat([self.df_1sec_daily, pd.DataFrame([new_row], columns=self.df_1sec_daily.columns)], ignore_index=True)
        except Exception as e:
            print(e)
            self.logger.log_message('error', f"Failed to update dataframe: {e}")


        try:
            if self.df_1sec_daily.empty: # If the dataframe is already populated, return
                async with SQLDatabase() as db:
                    await db.connect()
                    rows = await db.read_data('daily_nifty_spot', length=21000)
                    # Convert the rows to a dataframe
                    temp_df = pd.DataFrame(rows, columns=['Datetime', 'Open', 'High', 'Low', 'Close'])
                    # Combine the fetched dataframe with the existing one
                    self.df_1sec_daily = pd.concat([self.df_1sec_daily, temp_df], ignore_index=True)
                    await db.disconnect()
                    self.logger.log_message('info', "Populated dataframe with last 30 rows from the database")
        except Exception as e:
            self.logger.log_message('error', f"Failed to populate 1-second dataframe: {e}")

    async def populate_dataframe_1sec_daily(self):
        try:
            if self.df_1sec_daily.empty: # If the dataframe is already populated, return
                async with SQLDatabase() as db:
                    await db.connect()
                    rows = await db.read_data('daily_nifty_spot', length=300)
                    # Convert the rows to a dataframe
                    temp_df = pd.DataFrame(rows, columns=['Datetime', 'Open', 'High', 'Low', 'Close'])
                    # Combine the fetched dataframe with the existing one
                    self.df_1sec_daily = pd.concat([self.df_1sec_daily, temp_df], ignore_index=True)
                    await db.disconnect()
                    self.logger.log_message('info', "Populated dataframe with last 30 rows from the database")
        except Exception as e:
            self.logger.log_message('error', f"Failed to populate 1-second dataframe: {e}")

    async def populate_dataframe_15min(self):
        try:
            if self.df_15min.empty: # If the dataframe is already populated, return
                async with SQLDatabase() as db:
                    await db.connect()
                    rows = await db.read_data('nifty_spot_15min', length=30)
                    # Convert the rows to a dataframe
                    temp_df = pd.DataFrame(rows, columns=['Datetime', 'Open', 'High', 'Low', 'Close'])
                    # Combine the fetched dataframe with the existing one
                    self.df_15min = pd.concat([self.df_15min, temp_df], ignore_index=True)
                    await db.disconnect()
                    self.logger.log_message('info', "Populated 15-minute dataframe with last 30 rows from the database")
        except Exception as e:
            self.logger.log_message('error', f"Failed to populate 15-minute dataframe: {e}")

    async def update_15min_candles(self):
        try:
            while True:
            # Convert 'Datetime' column to datetime objects
                self.df_1sec_daily['Datetime'] = pd.to_datetime(self.df_1sec_daily['Datetime'])
                
                # Find the starting time for the first 15-minute interval
                start_time_first_interval = self.df_1sec_daily['Datetime'].iloc[0].replace(second=0)
                start_time_first_interval += timedelta(minutes=(15 - start_time_first_interval.minute % 15) % 15)
                
                # Group the data into 15-minute intervals
                self.df_1sec_daily['Interval'] = ((self.df_1sec_daily['Datetime'] - start_time_first_interval) // timedelta(minutes=15)).astype(int)
                result_df = self.df_1sec_daily.groupby('Interval').agg({
                    'High': 'max',
                    'Low': 'min',
                    'Open': 'first',
                    'Close': 'last'
                })
                
                # Reset the index and calculate the datetime for each interval
                result_df.reset_index(inplace=True)
                result_df['Datetime'] = start_time_first_interval + result_df['Interval'] * timedelta(minutes=15)
                
                # Drop the 'Interval' column
                result_df.drop(columns='Interval', inplace=True)
                
                # Update df_15min with the calculated 15-minute candles
                self.df_15min = result_df[['Datetime', 'Open', 'High', 'Low', 'Close']]
                await asyncio.sleep(constants.UPDATE_15MIN_DF_INTERVAL)
        except Exception as e:
            self.logger.log_message('error', f"Failed to update 15-minute candles: {e}")

    async def insert_quote_to_database(self, quote):
        try:
            async with SQLDatabase() as db:
                await db.connect()
                await db.insert_data('daily_nifty_spot', quote)
                await db.disconnect()
        except Exception as e:
            self.logger.log_message('error', f"Failed to insert data to database: {e}")

    async def get_df_15min(self):
        return self.df_15min
    
    async def fetch_ohlc_once(self, exchangeSegment='NSECM', exchangeInstrumentID=26000, compressionValue=constants.compression_value_15_min, lookbackDays=constants.DF_15MIN_BACK_DAYS, drop_last=True, subtract_time=True):
        try:
            

            current_datetime = datetime.now()
            start_time = (datetime.now().replace(hour=9, minute=15, second=0) - timedelta(days=lookbackDays)).strftime("%b %d %Y %H%M%S")
            end_time = (datetime.now().replace(hour=16, minute=00, second=0)).strftime("%b %d %Y %H%M%S")
            print('5')
            response = self.symphony_api.get_ohlc(exchangeSegment=exchangeSegment, exchangeInstrumentID=exchangeInstrumentID, startTime=start_time, endTime=end_time, compressionValue=compressionValue)
            
            
            formatted_df = self.symphony_api.format_ohlc_spot(response, drop_last=drop_last, subtract_time=subtract_time)
            
            return formatted_df
        except Exception as e:
            self.logger.log_message('error', f"Failed to fetch ohlc once: {e}")

    async def fetch_spot_ohlc(self, exchangeInstrumentID):
        try:
            while True:
                current_datetime = datetime.now()
                start_time = (datetime.now().replace(hour=9, minute=15, second=0) - timedelta(days=constants.DF_15MIN_BACK_DAYS)).strftime("%b %d %Y %H%M%S")
                end_time = (datetime.now().replace(hour=16, minute=00, second=0)).strftime("%b %d %Y %H%M%S")
                response = symphony_api.get_ohlc(exchangeSegment='NSECM', exchangeInstrumentID=exchangeInstrumentID, startTime=start_time, endTime=end_time, compressionValue=constants.compression_value_15_min)
                formatted_df = symphony_api.format_ohlc_spot(response)
                await self.update_dataframe_15min_ohlc(formatted_df)
                await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)
        except Exception as e:
            self.logger.log_message('error', f"Failed to fetch spot ohlc: {e}")


    async def update_dataframe_15min_ohlc(self, df):
        self.df_15min = df

    # to get the latest 15min candle
    async def get_latest_15min_candle(self):
        try:
            return self.df_15min.iloc[-1]
        except Exception as e:
            self.logger.log_message('error', f"Failed to get latest 15min candle: {e}")


    async def run(self,exchangeInstrumentID):
        try:
            # Start fetching and inserting quotes asynchronously
            print(exchangeInstrumentID)
            await asyncio.gather(
                # self.fetch_quotes(self.spot_instruments, message_code=constants.SPOT_1MIN_MESSAGE_CODE),
                # self.update_15min_candles()
                self.fetch_spot_ohlc(exchangeInstrumentID=exchangeInstrumentID)
            )
        except Exception as e:
            self.logger.log_message('error', f"Failed to run the engine: {e}")


class OptionsCandleEngine:
    def __init__(self, symphony_api,equity, series, compression_value=900):
        self.logger = CustomLogger('OptionsCandleEngine')
        self.df_options_data = pd.DataFrame(columns=['Datetime', 'ExchangeInstrumentID', 'Close'])
        self.symphony_api = symphony_api
        self.df_formatted_quote = None
        self.map_strike_instrument = None
        self.compression_value = compression_value
        self.options_instruments = None
        self.equity = equity
        self.series = series
    async def fetch_options_ohlc(self, exchangeInstrumentID, start_time, end_time):
        try:
                response = self.symphony_api.get_ohlc(exchangeSegment='NSEFO', exchangeInstrumentID=exchangeInstrumentID, startTime=start_time, endTime=end_time, compressionValue=self.compression_value)
                return self.symphony_api.format_ohlc_options(response)
        except Exception as e:
            self.logger.log_message('error', f"Failed to fetch options ohlc for instrument {exchangeInstrumentID}: {e}")

    
    async def form_options_df(self, instruments):
        try:
            while True:
                final_options_df = pd.DataFrame(columns=['Datetime', 'ExchangeInstrumentID', 'Close'])
                current_datetime = datetime.now()
                start_time = (datetime.now().replace(hour=9, minute=15, second=0)).strftime("%b %d %Y %H%M%S")
                end_time = (datetime.now().replace(hour=16, minute=00, second=0)).strftime("%b %d %Y %H%M%S")
                for instrument in instruments:
                    response = await self.symphony_api.get_ohlc(exchangeSegment='NSEFO', exchangeInstrumentID=instrument['exchangeInstrumentID'], startTime=start_time, endTime=end_time, compressionValue=900)                 
                    # formatted_df = self.symphony_api.format_ohlc_options(response)
                    # final_options_df = pd.concat([final_options_df, formatted_df], ignore_index=True)
                    # await self.update_dataframe_options(final_options_df)
                await asyncio.sleep(constants.FETCH_INTERVAL_SECONDS)
        except Exception as e:
            self.logger.log_message('error', f"Failed to form options df: {e}")

    
    async def update_dataframe(self, df):
        try:
            self.df_options_data = df
        except Exception as e:
            self.logger.log_message('error', f"Failed to update dataframe: {e}")

    async def _form_instrument_strike_map(self, df, strike_prices):
        try:
            self.map_strike_instrument = {(strike, option_type): df.loc[(df['Strike'] == strike) & (df['OptionType'] == option_type), 'ExchangeInstrumentID'].iloc[0] for strike in strike_prices for option_type in ['CE', 'PE'] if (strike, option_type) in zip(df['Strike'], df['OptionType'])}


        except Exception as e:
            self.logger.log_message('error', f"Failed to form instrument strike map: {e}")

    async def get_options_instruments(self, spot_close_price,symbol,strike,exchangeSegment=['NSEFO']):
        try:
            # rounding off the close price to the nearest 50
            
            
            spot_close_price_rounded = round(spot_close_price/strike)*strike
            
            # make a list of 20 strike prices above and below the spot close price at interval of 50
            strike_prices = [spot_close_price_rounded + i*strike for i in range(-20, 20)]
            
            
            # get closest expiry date
            # exchangeSegment: int, series: str, symbol: str
            symphony_api = SymphonyFintechAPI(series=self.series)
            symphony_api.get_headers()
            
            closest_expiry_date = symphony_api.get_closest_expiry_date(exchangeSegment=2, series=self.series, symbol=symbol)
            
            df_formatted_masters = symphony_api.get_formatted_masters(exchangeSegment=exchangeSegment,symbol=symbol)

            # keep only where the ExpiryDate is equal to the closest expiry date
            df_formatted_masters = df_formatted_masters[df_formatted_masters['ExpiryDate'] == closest_expiry_date]
            
            # check strike data type and convert to int if necessary
            if df_formatted_masters['Strike'].dtype != 'int64':
                df_formatted_masters['Strike'] = df_formatted_masters['Strike'].astype(int)

            filtered_df = df_formatted_masters[df_formatted_masters['Strike'].isin(strike_prices)]
            
            filtered_df.reset_index(drop=True, inplace=True)

            filtered_df.to_csv(f'equities/{self.equity}_instruments.csv')
            await self._form_instrument_strike_map(filtered_df, strike_prices)
            # Construct list of dictionaries in the required format
            instruments = []
            
            for _, row in filtered_df.iterrows():
                instruments.append({
                    'exchangeSegment': int(row['InstrumentType']),  # Assuming the exchange segment is always 1
                    'exchangeInstrumentID': int(row['ExchangeInstrumentID'])
                })

            await self.update_options_instruments(instruments)
            return instruments
        except Exception as e:
            self.logger.log_message('error', f"Failed to get options instruments: {e}")

    async def get_df_options_data(self):
        return self.df_options_data

    # function to update options_instruments
    async def update_options_instruments(self, instruments):
        self.logger.log_message('info', f"Updated options instruments, {instruments}")
        self.options_instruments = instruments

    async def run(self):
        try:
            # Start fetching and inserting quotes asynchronously
            await self.form_options_df(self.options_instruments)
            # await asyncio.gather(
            #     self.fetch_quotes(instruments=options_instruments, message_code=1501)
            # )
        except Exception as e:
            self.logger.log_message('error', f"Failed to run the engine: {e}")


if __name__ == "__main__":
    # spot_candle_engine = SpotCandleEngine()
    # asyncio.run(spot_candle_engine.run(), debug=True)

    symphony_api = SymphonyFintechAPI() 
    symphony_api.get_headers()
    
    options_candle_engine = OptionsCandleEngine(symphony_api=symphony_api)
    asyncio.run(options_candle_engine.run(), debug=True)    
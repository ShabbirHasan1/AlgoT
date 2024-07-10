# local imports
import constants
from logging_config import CustomLogger  

# library imports
import requests
import time
import json
#import asyncpg
import constants
import pandas as pd
import numpy as np
import asyncio
import talib
from datetime import datetime, timezone, time
from candle_engine import SpotCandleEngine

class SpotBrain:
    def __init__(self,equity):
        self.logger = CustomLogger('spotBrain')
        self.df = pd.DataFrame(columns=['Datetime', 'Open', 'High', 'Low', 'Close'])
        self.window = constants.WINDOW
        self.fast_factor = constants.FAST_FACTOR
        self.slow_factor = constants.SLOW_FACTOR
        self.equity = equity
    async def populate_dataframe(self, df):
        try:
            del self.df
            self.df = df
            self.df['Close'] = self.df['Close'].astype(float)
            self.df['High'] = self.df['High'].astype(float)
            self.df['Low'] = self.df['Low'].astype(float)
            self.df['Open'] = self.df['Open'].astype(float)
            # reset the index of the dataframe
            # df.reset_index(drop=True, inplace=True)
        except Exception as e:
            self.logger.log_message('error', f"Failed to populate dataframe: {e}")

    
    def _calculate_ama(self):
        try:
            close_prices = self.df['Close']
            volatility = close_prices.pct_change().rolling(window=self.window, min_periods=self.window).std()
            fast_ema = close_prices.ewm(span=self.fast_factor * self.window, adjust=False).mean()
            slow_ema = close_prices.ewm(span=self.slow_factor * self.window, adjust=False).mean()
            ama = (fast_ema + volatility * (close_prices - slow_ema)).ewm(span=self.window, adjust=False).mean()
            self.df['AMA'] = ama
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate AMA: {e}")

    def _calculate_rsi(self):
        try:
            self.df['RSI'] = talib.RSI(self.df['Close'], timeperiod=self.window)
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate RSI: {e}")


    def _calculate_atr(self):
        try:
            self.df['high-low'] = self.df['High'] - self.df['Low']
            self.df['high-close_prev'] = abs(self.df['High'] - self.df['Close'].shift(1))
            self.df['low-close_prev'] = abs(self.df['Low'] - self.df['Close'].shift(1))
            self.df['true_range'] = self.df[['high-low', 'high-close_prev', 'low-close_prev']].max(axis=1)

            # Calculate ATR
            self.df['ATR'] = self.df['true_range'].rolling(window=self.window, min_periods=1).mean()

            # Drop intermediate columns
            self.df.drop(['high-low', 'high-close_prev', 'low-close_prev', 'true_range'], axis=1, inplace=True)
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate ATR: {e}")

    def _calculate_piviot_points(self, column_name=None):
        try:
            data = self.df.copy()
            length = self.window
            if column_name == 'High':
                pivot_points = pd.DataFrame(index=data.index, columns=[column_name, f'IsPivot{column_name.capitalize()}'])

                for i in range(length, len(data) - length):
                    # Check if the current point is a pivot high
                    is_pivot_point = all(data[column_name][i] > data[column_name][i - length:i]) and all(data[column_name][i] > data[column_name][i + 1:i + length + 1])
                    # is_pivot_point = all(data[column_name][i] > data[column_name][i - length:i]) 

                    # Store the values in the DataFrame
                    pivot_points.at[data.index[i], column_name] = data[column_name][i] if is_pivot_point else None

                    pivot_points.at[data.index[i], f'IsPivot{column_name.capitalize()}'] = is_pivot_point
            
            elif column_name == 'Low':
                pivot_points = pd.DataFrame(index=data.index, columns=[column_name, f'IsPivot{column_name.capitalize()}'])
                for i in range(length, len(data) - length):
                    # Check if the current point is a pivot low
                    is_pivot_point = all(data[column_name][i] < data[column_name][i - length:i]) and all(data[column_name][i] < data[column_name][i + 1:i + length + 1])
                    # is_pivot_point = all(data[column_name][i] < data[column_name][i - length:i])

                    # Store the values in the DataFrame
                    pivot_points.at[data.index[i], column_name] = data[column_name][i] if is_pivot_point else None

                    pivot_points.at[data.index[i], f'IsPivot{column_name.capitalize()}'] = is_pivot_point
            del data
            return pivot_points
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate Pivot Points: {e}")
            
    def _assign_piviot_values(self):
        try:
            for i in range(1, len(self.df)):
                if self.df.at[self.df.index[i], 'PH']:
                    self.df.at[self.df.index[i], 'PH_val'] = self.df.at[self.df.index[i], 'High']
                else:
                    self.df.at[self.df.index[i], 'PH_val'] = self.df.at[self.df.index[i - 1], 'PH_val']
                if self.df.at[self.df.index[i], 'PL']:
                    self.df.at[self.df.index[i], 'PL_val'] = self.df.at[self.df.index[i], 'Low']
                else:
                    self.df.at[self.df.index[i], 'PL_val'] = self.df.at[self.df.index[i - 1], 'PL_val']
        except Exception as e:
            self.logger.log_message('error', f"Failed to assign Pivot Values: {e}")

    def _calculate_slope(self):
        try:
            self.df['Slope'] = self.df['ATR']/self.window
            
            # Initialize the first values for slope_ph and slope_pl
            # self.df.at[self.df.index[0], 'slope_ph'] = self.df.at[self.df.index[0], 'Slope']
            # self.df.at[self.df.index[0], 'slope_pl'] = self.df.at[self.df.index[0], 'Slope']
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate Slope: {e}")

    def _update_slope_pivots(self):
        try:
            for i in range(1, len(self.df)):
                if self.df.at[self.df.index[i], 'PH']:
                    self.df.at[self.df.index[i], 'slope_ph'] = self.df.at[self.df.index[i], 'Slope']
                else:
                    self.df.at[self.df.index[i], 'slope_ph'] = self.df.at[self.df.index[i - 1], 'slope_ph']
                if self.df.at[self.df.index[i], 'PL']:
                    self.df.at[self.df.index[i], 'slope_pl'] = self.df.at[self.df.index[i], 'Slope']
                else:
                    self.df.at[self.df.index[i], 'slope_pl'] = self.df.at[self.df.index[i - 1], 'slope_pl']
        except Exception as e:
            self.logger.log_message('error', f"Failed to update Slope Pivots: {e}")

    def _calcualte_upper_lower_band(self):
        try:
            self.df['upper'] = 0.0
            self.df['lower'] = 0.0

            for i in range(1, len(self.df)):
                if self.df.at[self.df.index[i], 'PH'] == True:
                    self.df.at[self.df.index[i], 'upper'] = self.df.at[self.df.index[i], 'High']
                else:
                    self.df.at[self.df.index[i], 'upper'] = (self.df.at[self.df.index[i - 1], 'upper'] - self.df.at[self.df.index[i], 'slope_ph'])


            for i in range(1, len(self.df)):
                if self.df.at[self.df.index[i], 'PL'] == True:
                    self.df.at[self.df.index[i], 'lower'] = self.df.at[self.df.index[i], 'Low']
                else:
                    self.df.at[self.df.index[i], 'lower'] = (self.df.at[self.df.index[i - 1], 'lower'] + self.df.at[self.df.index[i], 'slope_pl'])


        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate Upper and Lower Bands: {e}")

    def _calculate_upos_dnos(self):
        try:
            self.df['upos'] = 0
            self.df['dnos'] = 0

            # Calculate upos
            for i in range(1, len(self.df)):
                if self.df.at[self.df.index[i], 'PH'] != True or self.df.at[self.df.index[i], 'PL'] =='':
                    upper_limit = self.df.at[self.df.index[i - 1], 'upper']
                    if self.df.at[self.df.index[i], 'Close'] > upper_limit:
                        self.df.at[self.df.index[i], 'upos'] = 1

            # Calculate dnos
            for i in range(1, len(self.df)):
                if self.df.at[self.df.index[i], 'PL'] != True or self.df.at[self.df.index[i], 'PH'] == '':
                    lower_limit = self.df.at[self.df.index[i - 1], 'lower']
                    if self.df.at[self.df.index[i], 'Close'] < lower_limit:
                        self.df.at[self.df.index[i], 'dnos'] = 1
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate Upos and Dnos: {e}")
            
    def _calculate_signals(self):
        try:
            self.df['signal'] = 'Hold'
            buy_condition = (self.df['upos'] > self.df['upos'].shift(1)) & (self.df['signal'] != 'Buy')
            sell_condition = (self.df['dnos'] > self.df['dnos'].shift(1)) & (self.df['signal'] != 'Sell')

            self.df.loc[buy_condition, 'signal'] = 'Buy'
            self.df.loc[sell_condition, 'signal'] = 'Sell'
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate Signals: {e}")

    async def get_df(self):
        return self.df

    async def run(self):
        try:
            self._calculate_ama()
            self._calculate_rsi()
            self._calculate_atr()
            self.df['PH'] = self._calculate_piviot_points(column_name='High')['IsPivotHigh']
            self.df['PL'] = self._calculate_piviot_points(column_name='Low')['IsPivotLow']
            self._assign_piviot_values()
            self._calculate_slope()
            self._update_slope_pivots()
            self._calcualte_upper_lower_band()
            self._calculate_upos_dnos()
            self._calculate_signals()

            # self.df.to_csv('spot_brain.csv')
        except Exception as e:
            self.logger.log_message('error', f"Failed to calculate Pivot Points: {e}")


class OptionsBrain:
    def __init__(self,equity, strike, wings):
        self.equity = equity
        self.strike = strike
        self.wings = wings
        self.logger = CustomLogger('optionsBrain')
        # create empty df with no columns
        self.options_df = pd.DataFrame()
        self.map_strike_instrument = None
        

    
    async def populate_dataframe(self, df):
        try:
            del self.options_df
            self.options_df = df
        except Exception as e:
            self.logger.log_message('error', f"Failed to populate dataframe: {e}")
    
    async def populate_map_strike_instrument(self, map):
        try:
            self.map_strike_instrument = map
            
            print(self.map_strike_instrument)

        except Exception as e:
            
            self.logger.log_message('error', f"Failed to populate map_strike_instrument: {e}")

    async def get_df(self):
        return self.options_df
    
    def _round_to_nearest_50(self, number):
        return round(number / self.strike) * self.strike
    
    def _get_daily_diff(self, current_datetime):
        day_of_week = current_datetime.weekday()
        # Define a dictionary to map each day to its corresponding difference
        day_diff_mapping = {
            "Monday": self.wings,
            "Tuesday": self.wings,
            "Wednesday": self.wings,
            "Thursday": self.wings,
            "Friday": self.wings,
            "Saturday": self.wings,
            "Sunday": self.wings
        }
        # Return the difference based on the day of the week
        return day_diff_mapping[datetime.strftime(current_datetime, '%A')]

        
    def get_close_price_dict(self, atmSP, wingCall, wingPut, current_datetime_str_short, expiry):
        pass

    async def mark_trades(self):
        try:
            df = self.options_df
            position = "squareoff"

            # set the initial capital and the current capital
            capital = 100000

            first_run = True

            for i, row in enumerate(df.itertuples()):
                # timer_start = tm.time()
                # Check if this a new day and time is 09:30:00+05:30

                current_datetime_str = str(row.Datetime)
                current_datetime_str_short = current_datetime_str[:-6]

                has_traded_today = False
                has_squareoff_today = False

                # check if the time is 09:30:00+05:30 or it is the first run
                if '09:30:00' in current_datetime_str or first_run:
                    # Calculate the ATM strike price
                    atmSP = self._round_to_nearest_50(row.Close)
                    wingPut = atmSP - self._get_daily_diff(row.Datetime)
                    wingCall = atmSP + self._get_daily_diff(row.Datetime)
                    
                    # # marking the postion as 0 updating tickers_dict
                    # close_price_dict = get_close_price_dict(atmSP, wingCall, wingPut, current_datetime_str_short, row.closest_expiry)

                    # df.loc[i, ['atmSP', 'wingCall', 'wingPut', 'legPriceOrignal1', 'legPriceOrignal2', 'legPriceOrignal3', 'legPriceOrignal4']] = [atmSP, wingCall, wingPut, close_price_dict['atmSPCall'], close_price_dict['atmSPPut'], close_price_dict['wingCallPrice'], close_price_dict['wingPutPrice']]
                    if first_run:
                        # Reset values to 0 for the specified columns at row i
                        # df.loc[i, columns_to_reset] = 0
                        df.at[i, 'position'] = 'hold'
                        position = 'hold'
                        df.at[i, 'balance'] = capital
                        first_run = False
                    else:
                        df.at[i, 'position'] = 'beginx'
                        position = 'hold'
                        df.loc[i, ['atmSP', 'wingCall', 'wingPut'] ] = [atmSP, wingCall, wingPut]
                        # calculate_m2m_new(i, start_day=True, caller='beginx 9:30')
                    # sell atmSP call -> api call # sell atmSp put -> api call # buy wingPut -> api call # buy wingCall -> api call
                    
                
                # if time is 15:15:00+05:30 then squareoff all positions
                elif row.Datetime.time() == time(15, 00):
                    # buy atmSp call -> api call # buy atmSp put -> api call # sell wingPut -> api call # sell wingCall -> api call
                    capital = df.at[i-1, 'balance']
                    df.at[i, 'balance'] = df.at[i-1, 'balance']
                    df.at[i, 'position'] = position = 'squareoff'
                    df.loc[i, ['atmSP', 'wingCall', 'wingPut'] ] = [atmSP, wingCall, wingPut]
                    # store the values in the dataframe
                    # close_price_dict = get_close_price_dict(atmSP, wingCall, wingPut, current_datetime_str_short, row.closest_expiry)  
                    # df.loc[i, ['atmSP', 'wingCall', 'wingPut', 'legPriceOrignal1', 'legPriceOrignal2', 'legPriceOrignal3', 'legPriceOrignal4']] = [atmSP, wingCall, wingPut, close_price_dict['atmSPCall'], close_price_dict['atmSPPut'], close_price_dict['wingCallPrice'], close_price_dict['wingPutPrice']]         
                    # rolling_dict = calculate_m2m_new(i, caller='15:15')
                
                elif row.Datetime.time() == time(15, 30):
                    # store the values in the dataframe
                    df.loc[i, ['atmSP', 'wingCall', 'wingPut'] ] = [atmSP, wingCall, wingPut]
                    df.at[df.index[i], 'position'] = 'hold'
                    df.at[i, 'balance'] = df.at[i-1, 'balance']

                    position = 'hold'
                    
                elif row.Datetime.time() == time(9, 15):
                    # this is for time between 15:15:00+05:30 and 09:30:00+05:30
                    # store the values in the dataframe

                    df.loc[i, ['atmSP', 'wingCall', 'wingPut'] ] = [atmSP, wingCall, wingPut]
                    df.at[df.index[i], 'position'] = 'hold'
                    df.at[i, 'balance'] = df.at[i-1, 'balance']
                    position = 'hold'
                
                elif df.at[i-1, 'position'] == 'squareoff' and row.Datetime.time() > time(9, 30) and row.Datetime.time() < time(15, 15):
                    # if previous was squareoff then take position this time
                    atmSP = self._round_to_nearest_50(row.Close)
                    wingPut = atmSP - self._get_daily_diff(row.Datetime)
                    wingCall = atmSP + self._get_daily_diff(row.Datetime)
                    # close_price_dict = get_close_price_dict(atmSP, wingCall, wingPut, current_datetime_str_short, row.closest_expiry)  
                    df.at[i, 'position'] = position = 'beginx'
                    df.loc[i, ['atmSP', 'wingCall', 'wingPut']] = [atmSP, wingCall, wingPut]
                    # calculate_m2m_new(i, start_day=True, caller='beginx')
                
                else:
                    # close_price_dict = get_close_price_dict(atmSP, wingCall, wingPut, current_datetime_str_short, row.closest_expiry) 
                    if position == 'hold':
                        if row.signal == 'Buy':
                            if row.RSI < 70 and row.AMA < row.Close:
                                # buy atmSp call -> api call # Sell atmSp put -> api call # buy wingCall -> api call # buy wingPut -> api call
                                df.at[i, 'position'] = position = 'buy'
                                has_traded_today = True
                        
                        elif row.signal == 'Sell':
                            if row.RSI > 30 and row.AMA > row.Close:
                                # sell atmSp call -> api call # buy atmSp put -> api call # Buy wingPut -> api call # Buy wingCall -> api call
                                df.at[i, 'position'] = position = 'sell'
                                has_traded_today = True

                    elif position == 'buy':
                        if row.signal == 'Hold':
                            if row.RSI > 70:
                                # buy atmSp put -> api call x 2 # sell wingPut -> api call x 2 # sell wingCall -> api call x 2
                                df.at[i, 'position'] = position = 'squareoff'
                                has_traded_today = True
                        
                        elif row.signal == 'Sell':          
                            # buy atmSp put -> api call # sell wingPut -> api call # sell wingCall -> api call
                            df.at[i, 'position'] = position =  'squareoff'
                            has_traded_today = True

                    elif position == 'sell':
                        if row.signal == 'Hold':
                            if row.RSI < 30:              
                                # buy atmSp call -> api call # sell wingPut -> api call # sell wingCall -> api call  
                                df.at[i, 'position'] = position = 'squareoff'
                                has_traded_today = True

                                
                        elif row.signal == 'Buy':
                            df.at[df.index[i], 'position'] = position = 'squareoff'
                            has_traded_today = True
                            

                    df.at[i, 'position'] = position
                    # df.loc[i, ['atmSP', 'wingCall', 'wingPut', 'legPriceOrignal1', 'legPriceOrignal2', 'legPriceOrignal3', 'legPriceOrignal4']] = [atmSP, wingCall, wingPut, close_price_dict['atmSPCall'], close_price_dict['atmSPPut'], close_price_dict['wingCallPrice'], close_price_dict['wingPutPrice']]
                    df.loc[i, ['atmSP', 'wingCall', 'wingPut'] ] = [atmSP, wingCall, wingPut]

                    if not has_traded_today:
                        df.at[i, 'position'] = df.at[i-1, 'position']

                    
                    # called everytime
                    # calculate_m2m_new(i, caller='daily')

                    # if df.at[i, 'totalPL'] < -3300:
                    #     # squareoff all positions
                    #     # df.loc[i, columns_to_reset] = 0
                    #     df.at[i, 'position'] = position = 'squareoff'
                    #     # calculate_m2m_new(i, caller='stoploss')
                    #     # df.at[i, 'totalPL'] = -3300
                    #     # df.at[i, 'balance'] = df.at[i-1, 'balance'] - 3300
                        

                    
                
                # timer_end = tm.time()
                # logging.info(f"Time taken for this iteration: {timer_end - timer_start} seconds")


                # df.at[df.index[i], 'position'] = position
            
            self.options_df = df
            # df.to_csv('options_brain.csv')
        except Exception as e:
            self.logger.log_message('error', f"Failed to mark trades: {e}")

    async def put_instruments(self):
        try:
            # get matching strike for the given atmSP, wingCall, wingPut and store in df
            df = self.options_df
            # Map prices to instrument IDs and create new columns
            df['atmce'] = df['atmSP'].map(lambda x: self.map_strike_instrument.get((x, 'CE'), None))
            df['atmpe'] = df['atmSP'].map(lambda x: self.map_strike_instrument.get((x, 'PE'), None))
            df['wce'] = df['wingCall'].map(lambda x: self.map_strike_instrument.get((x, 'CE'), None))
            df['wpe'] = df['wingPut'].map(lambda x: self.map_strike_instrument.get((x, 'PE'), None))
            
            self.options_df = df
            df.to_csv(f'equities/{self.equity}_options_brain.csv')
        except Exception as e:
            self.logger.log_message('error', f"Failed to put instruments: {e}")



    
    
    async def run(self):
        try:
            await self.mark_trades()
            await self.put_instruments()
        except Exception as e:
            self.logger.log_message('error', f"Failed to run OptionsBrain: {e}")



class StopLossBrain:
    def __init__(self, symphony_api, equity):
        self.logger = CustomLogger('stoplossBrain')
        # create empty df with no columns
        self.options_df = pd.DataFrame()
        self.spot_candle_engine = SpotCandleEngine(symphony_api=symphony_api)
        self.leg_prices = 0
        self.equity = equity
        self.instrument_ids = []

    async def populate_dataframe(self):
        self.options_df = pd.read_csv(f'equities/{self.equity}_options_brain.csv')
        self.options_df = self.options_df.iloc[-1]
        self.instrument_ids = [self.options_df['atmce'], self.options_df['atmpe'], self.options_df['wce'], self.options_df['wpe']]
    
    async def get_current_price(self):
        try:
            # Get current prices for all instruments in one go
            prices = {
                'legPriceOg0': await self.spot_candle_engine.fetch_ohlc_once(exchangeInstrumentID=int(self.options_df['atmce']), exchangeSegment='NSEFO', compressionValue=constants.compression_value_1_min, lookbackDays=0, drop_last=False, subtract_time=False),
                'legPriceOg1': await self.spot_candle_engine.fetch_ohlc_once(exchangeInstrumentID=int(self.options_df['atmpe']), exchangeSegment='NSEFO', compressionValue=constants.compression_value_1_min, lookbackDays=0, drop_last=False, subtract_time=False),
                'legPriceOg2': await self.spot_candle_engine.fetch_ohlc_once(exchangeInstrumentID=int(self.options_df['wce']), exchangeSegment='NSEFO', compressionValue=constants.compression_value_1_min, lookbackDays=0, drop_last=False, subtract_time=False),
                'legPriceOg3': await self.spot_candle_engine.fetch_ohlc_once(exchangeInstrumentID=int(self.options_df['wpe']), exchangeSegment='NSEFO', compressionValue=constants.compression_value_1_min, lookbackDays=0, drop_last=False, subtract_time=False),
            }
            self.leg_prices = {key: value.iloc[-1]['Close'] for key, value in prices.items()}
            self.instrument_ids = [self.options_df['atmce'], self.options_df['atmpe'], self.options_df['wce'], self.options_df['wpe']]


        except Exception as e:
            self.logger.log_message('error', f"Failed to get current price: {e}")



    async def write_to_csv(self):
            try:
                # Writing leg prices to DataFrame
                df = pd.DataFrame(self.leg_prices, index=[0])
                
                # Read the existing trade sheet
                trade_sheet = pd.read_csv(f'Tradesheets/{self.equity}_trade_sheet.csv')
                trade_sheet = trade_sheet.copy()  # Ensure working with a copy


                
                
                # Update the CMP column in the trade_sheet
                for i in range(4):
                    instrument_id = self.instrument_ids[i]
                    leg_price = df.at[0, f'legPriceOg{i}']
                    
                    if instrument_id in trade_sheet['InstrumentID'].values:
                        # Update the CMP for rows where InstrumentID matches
                        trade_sheet.loc[trade_sheet['InstrumentID'] == instrument_id, 'CMP'] = leg_price
                    else:
                        # Create a new row with default values and the specific InstrumentID and CMP
                        new_row = pd.DataFrame([{col: 0 for col in trade_sheet.columns}])
                        new_row['InstrumentID'] = instrument_id
                        new_row['CMP'] = leg_price
                        trade_sheet = pd.concat([trade_sheet, new_row], ignore_index=True)
                    

                # Write the updated trade sheet back to the CSV
                trade_sheet.to_csv(f'Tradesheets/{self.equity}_trade_sheet.csv', index=False)
            except Exception as e:
                self.logger.log_message('error', f"Failed to write to csv: {e}")



    async def run(self):
        try:
            await self.populate_dataframe()
            await self.get_current_price()
            await self.write_to_csv()
        except Exception as e:
            self.logger.log_message('error', f"Failed to run StopLossBrain from run: {e}")


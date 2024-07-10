import constants
from logging_config import CustomLogger, TelegramBot
import time
import pandas as pd
import requests
import json
from datetime import datetime
clientID = "PRO227"

class SymphonyInteractiveAPI:
    def __init__(self, base_qty,equity,freeze_qty, offset=0):
        self.__api_key = constants.INTERACTIVE_API_KEY
        self.__api_secret = constants.INTERACTIVE_API_SECRET
        self.__source = constants.INTERACTIVE_SOURCE
        self.__root_url = constants.INTERACTIVE_ROOT_URL
        self.__token = None
        self.__multiplier = 0
        self.__base_quantity = base_qty
        self.__freeze_quantity = freeze_qty
        self.__equity = equity
        self.__quantity = 0
        self.offset = offset
        self.logger = CustomLogger(filename="SymphonyInteractiveAPI")
        self.trade_sheet = pd.DataFrame(columns=['TradingSymbol', 'InstrumentID', 'CMP', 'LegPosition', 'BuyAveragePrice', 'BuyQuantity', 'SellAveragePrice', 'SellQuantity', 'NetQuantity', 'UnrealisedM2M', 'RealisedM2M', 'M2M'], index=None)
        self.telegramBot = TelegramBot()
        self.endpoints = {
            "login": ("/interactive/user/session", "POST"),
            "logout": ("/interactive/user/session", "DELETE"),
            "place_order": ("/interactive/orders", "POST"),
            "modify_order": ("/interactive/orders", "PUT"),
            "order_history": ("/interactive/orders", "GET"),
            "balance": (f"/interactive/user/balance?clientID={clientID}", "GET"),
            "positions": ("/interactive/portfolio/dealerpositions?dayOrNet=DayWise", "GET"),
            "tradebook": ("/interactive/orders/dealertradebook", "GET"),
        }

        self.stoploss_time = time.localtime()
        self.headers = {"Content-Type": "application/json"}
        self.logger = CustomLogger("dataEngine")
        self.live_leg_data = {
            "leg0":{"BUYPRICE": 0.0, "SELLPRICE": 0.0, "BUYQTY": 0, "SELLQTY": 0, "LASTPOS": ""},
            "leg1":{"BUYPRICE": 0.0, "SELLPRICE": 0.0, "BUYQTY": 0, "SELLQTY": 0, "LASTPOS": ""},
            "leg2":{"BUYPRICE": 0.0, "SELLPRICE": 0.0, "BUYQTY": 0, "SELLQTY": 0, "LASTPOS": ""},
            "leg3":{"BUYPRICE": 0.0, "SELLPRICE": 0.0, "BUYQTY": 0, "SELLQTY": 0, "LASTPOS": ""}
        }
        self.daily_json_path = f'daily_jsons/{self.__equity}.json'

    def update_headers(self, token):
        if token is None:
            self.headers.pop("Authorization", None)
        else:
            self.headers["Authorization"] = token

    def get_headers(self):
        # read headers from json file
        with open('headers.json', 'r') as f:
            headers = json.load(f)
            self.headers = headers['orderApiHeaders']
    
    def _make_request(self, endpoint, data=None):
        try:
            url = self.__root_url + endpoint[0]
            method = endpoint[1]
            headers = self.headers.copy()

            if method == "POST":
                response = requests.post(url, headers=headers, json=data)
            elif method == "GET":
                response = requests.get(url, headers=headers, params=data)
            elif method == "PUT":
                response = requests.put(url, headers=headers, json=data)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, json=data)
            else:
                raise ValueError("Invalid HTTP method")
            return response
        except Exception as e:
            self.logger.log_message(
                "error", f"Error making request to: {e}"
            )

    def login(self):
        # calling the authenticate endpoint base_url/auth/login
        endpoint = self.endpoints["login"]
        data = {
            "secretKey": self.__api_secret,
            "appKey": self.__api_key,
            "source": self.__source,
        }
        response = self._make_request(endpoint, data=data)
        self.token = response.json()["result"]["token"]
        self.update_headers(self.token)
        self.logger.log_message("info", "Login Interactive API successful")

    def logout(self):
        try:
            endpoint = self.endpoints["logout"]
            response = self._make_request(endpoint)
            if response.status_code == 200:
                self.token = None
                self.update_headers(self.token)
                self.logger.log_message("info", "Logout successful")
            else:
                self.logger.log_message("error", "Logout failed")
        except Exception as e:
            self.logger.log_message("error", f"Error in logout: {e}")
    
    def read_trade_sheet(self):
        try:
            return pd.read_csv(f"Tradesheets/{self.__equity}_trade_sheet.csv")
        except Exception as e:
            self.logger.log_message("error", f"Error in read_trade_sheet: {e}")

    def write_trade_sheet(self, data):
        try:
            data.to_csv(f"Tradesheets/{self.__equity}_trade_sheet.csv", index=False)
        except Exception as e:
            self.logger.log_message("error", f"Error in write_trade_sheet: {e}")

    def reset_trade_sheet(self):
        try:
            data = pd.DataFrame(columns=['TradingSymbol', 'InstrumentID', 'CMP', 'LegPosition', 'BuyAveragePrice', 'BuyQuantity', 'SellAveragePrice', 'SellQuantity', 'NetQuantity', 'UnrealisedM2M', 'RealisedM2M', 'M2M'])
            self.write_trade_sheet(data)
        except Exception as e:
            self.logger.log_message("error", f"Error in reset_trade_sheet: {e}")
            
    def get_order_history(self, app_order_id, client_id):
        try:
            endpoint = self.endpoints["order_history"]
            data = {
                "appOrderID": app_order_id,
                "clientID": client_id
            }
            response = self._make_request(endpoint, data=data)
            return response
        except Exception as e:
            self.logger.log_message("error", f"Error in get_order_history: {e}")
    
    def get_order_status(self, app_order_id, client_id):
        try:
            time.sleep(0.5)
            response = self.get_order_history(app_order_id, client_id)
            print(response.status_code)
            if response.status_code != 200:
                raise Exception("Failed to get order status")
            else:
                data = response.json()
                order_status = data["result"][2]["OrderStatus"]
                message = data["result"][2]["CancelRejectReason"]
                order_average_price = float(data["result"][2]["OrderAverageTradedPrice"])
                trading_symbol = data["result"][2]["TradingSymbol"]
                order_quantity = float(data["result"][2]["OrderQuantity"])
                order_side = data["result"][2]["OrderSide"]
                return order_status, message, order_average_price, trading_symbol, order_quantity, order_side

        except Exception as e:
            self.logger.log_message("error", f"Error in get_order_status function: {e}")

    def get_margin_utilised(self):
        try:
            endpoint = self.endpoints["balance"]
            response = self._make_request(endpoint)
            margin = float(response.json()["result"]['BalanceList'][0]['limitObject']['RMSSubLimits']['marginUtilized'])
            print(f"Margin Utilised: {margin}")
            return margin
        except Exception as e:
            self.logger.log_message("error", f"Error in get_margin_utilised: {e}")

    def get_total_funds(self):
        try:
            return 1000000
        except Exception as e:
            self.logger.log_message("error", f"Error in get_funds: {e}")
    
    def get_available_funds(self):
        total_funds = self.get_total_funds()
        margin_utilised = self.get_margin_utilised()
        return total_funds - margin_utilised

    def set_multiplier(self):
        try:
            # available_funds = self.get_available_funds()
            available_funds = self.get_total_funds()
            self.__multiplier = int(available_funds / 100000)
        except Exception as e:
            self.logger.log_message("error", f"Error in get_multiplier: {e}")

    def get_positions(self):
        try:
            endpoint = self.endpoints["positions"]
            response = self._make_request(endpoint)
            return response
        except Exception as e:
            self.logger.log_message("error", f"Error in get_positions: {e}")

    def get_tradebook(self):
        try:
            endpoint = self.endpoints["tradebook"]
            response = self._make_request(endpoint)
            return response
        except Exception as e:
            self.logger.log_message("error", f"Error in get_tradebook: {e}")

    def read_daily_json(self, ):
        try:
            with open(self.daily_json_path, "r") as f:
                data = json.load(f)
            return data
        except Exception as e:
            self.logger.log_message("error", f"Error in read_daily_json: {e}")

    def write_daily_json(self, data):
        try:
            with open(self.daily_json_path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            self.logger.log_message("error", f"Error in write_daily_json: {e}")


    def check_and_update_highest_m2m(self, m2m):
        try:
            daily_json = self.read_daily_json()
            highest_m2m = daily_json["highest_m2m"]
            if m2m > highest_m2m:
                daily_json["highest_m2m"] = m2m
                self.write_daily_json(daily_json)
                return True
            else:
                return False
        except Exception as e:
            self.logger.log_message("error", f"Error in check_and_update_highest_m2m: {e}")
    
    def calculate_m2m(self):
        try:
            trade_sheet = self.read_trade_sheet()
            
            def calculate_realised_m2m(row):
                if row['BuyQuantity'] != 0 and row['SellQuantity'] != 0 and row['NetQuantity'] > 0:
                    return (row['SellAveragePrice'] * row['SellQuantity']) - (row['BuyAveragePrice'] * (row['BuyQuantity'] - row['NetQuantity']))
                elif row['BuyQuantity'] != 0 and row['SellQuantity'] != 0 and row['NetQuantity'] < 0:
                    return (row['SellAveragePrice'] * (row['SellQuantity'] - abs(row['NetQuantity']))) - (row['BuyAveragePrice'] * row['BuyQuantity'])
                elif row['BuyQuantity'] != 0 and row['SellQuantity'] != 0 and row['NetQuantity'] == 0:
                    return (row['SellAveragePrice'] * row['SellQuantity']) - (row['BuyAveragePrice'] * row['BuyQuantity'])
                else:
                    return 0

            def calculate_unrealised_m2m(row):
                net_quantity = row['NetQuantity']
                if net_quantity == 0:
                    return 0
                elif net_quantity > 0:
                    return abs(net_quantity) * (row['CMP'] - row['BuyAveragePrice'])
                elif net_quantity < 0:
                    return abs(net_quantity) * (row['SellAveragePrice'] - row['CMP'])
                return 0

            trade_sheet['RealisedM2M'] = trade_sheet.apply(calculate_realised_m2m, axis=1)
            trade_sheet['UnrealisedM2M'] = trade_sheet.apply(calculate_unrealised_m2m, axis=1)
            trade_sheet['M2M'] = trade_sheet['RealisedM2M'] + trade_sheet['UnrealisedM2M']

            self.trade_sheet = trade_sheet
            self.write_trade_sheet(trade_sheet)
            
            return trade_sheet['M2M'].sum()

        except Exception as e:
            self.logger.log_message("error", f"Error in calculate_m2m: {e}")

    def check_stoploss(self, position):
        try:
            current_m2m = self.calculate_m2m()
            self.check_and_update_highest_m2m(current_m2m)
            highest_m2m = self.read_daily_json()["highest_m2m"]
            stoploss_price = highest_m2m - (self.get_total_funds()*0.03)
            print(f"{self.__equity} Current M2M: {current_m2m}, Stoploss at: {stoploss_price}")
            data = self.read_daily_json()
            data['Current M2M'] = current_m2m
            data['Stoploss Price'] = stoploss_price
            data['Position'] = position
            self.write_daily_json(data)
            # Add one minute to stoploss_time
            new_stoploss_minute = (self.stoploss_time.tm_min + 1) % 60

            if new_stoploss_minute == time.localtime().tm_min:
                self.stoploss_time = time.localtime()
                if current_m2m < stoploss_price:
                    return True
                else:
                    return False
        except Exception as e:
            self.logger.log_message("error", f"Error in check_stoploss: {e}")

    def place_order_handler(self, data, prev_position, hard_squareoff=False):

        response_codes = [0, 0, 0, 0]
        if hard_squareoff:
            position = "squareoff"
            response = self.read_daily_json()
            response['Position'] = position
            self.write_daily_json(response)
        else:
            position = data["position"]

        if position == "hold":
            print("Ignoring hold position")
            return [200, 200, 200, 200]
        
        # ignore tradesheet square off
        if position == "squareoff" and time.localtime().tm_hour == 15 and time.localtime().tm_min >= 15 and time.localtime().tm_min <= 30 and not hard_squareoff:
            print("Ignoring squareoff from the tradesheet")
            return [200, 200, 200, 200]

        instruments = [int(data["atmce"]), int(data["atmpe"]), int(data["wce"]), int(data["wpe"])]
        strike_prices = [int(data["atmSP"]), int(data["atmSP"]), int(data["wingCall"]), int(data["wingPut"])]

        exchangeSegment = "NSEFO"
        productType = "NRML"
        orderType = "MARKET"
        timeInForce = "DAY"
        disclosedQuantity = 0
        limitPrice = 0
        stopPrice = 0
        orderUniqueIdentifier = "454845"
        clientID = "*****"
        self.set_multiplier()

        positions = {'beginx': [-1, -1, 1, 1], 'buy': [0, -2, 2, 2], 'sell': [-2, 0, 2, 2], 'squareoff': [0, 0, 0, 0], 'hold': [0, 0, 0, 0]}
        print(f"Taking Position: {position}")

        if position == "beginx":
            quantity = self.__multiplier * self.__base_quantity
            response = self.read_daily_json()
            response['highest_m2m'] = 0
            response['Date']=time.strftime("%Y-%m-%d")
            self.write_daily_json(response)
            # stoploss_sheet set 'quantity' to quantity
            self.trade_sheet = self.read_trade_sheet()
        else:
            self.trade_sheet = self.read_trade_sheet()
            quantity = self.__multiplier * self.__base_quantity
        
        if position == "squareoff":
            response = self.read_daily_json()
            response['highest_m2m'] = 0
            self.write_daily_json(response)


        


# while loop until all in all_orders_placed is True
        for order_type in ["BUY", "SELL"]:
            for i in range(4):
                previous_leg_position = positions[prev_position][i]
                leg_position = positions[position][i]
                # taking the difference of the leg position
                # example going from squareoff to beginx for leg 1
                # leg_position = -1, previous_leg_position = 0, hence leg_position_diff = -1, hence sell side
                # example going from beginx to buy for leg 1
                # leg_position = 0, previous_leg_position = -1, hence leg_position_diff = 1, hence buy side
                leg_position_diff = leg_position - previous_leg_position
                if leg_position_diff == 0:
                    print(f"Leg: {i}, No change in position")
                    response_codes[i] = 200
                    continue

                orderSide = "BUY" if leg_position_diff > 0 else "SELL" # If leg_position_diff is positive, then buy, else sell

                if orderSide == order_type:
                    orderQuantity = quantity * abs(leg_position_diff)
                    exchangeInstrumentID = int(instruments[i])

                    # freeze quantity loop
                    while orderQuantity > 0:
                        # placing_order_quantity is the quantity for which orders will be placed
                        if orderQuantity < self.__freeze_quantity:
                            placing_order_quantity = orderQuantity
                        else:
                            placing_order_quantity = self.__freeze_quantity
                        print(f"Placing order for leg: {i}; Instrument: {exchangeInstrumentID}; OrderSide: {orderSide}; Quantity: {placing_order_quantity}")

                        response = self.place_order( # Call the place_order method
                            exchangeSegment=exchangeSegment,
                            exchangeInstrumentID=exchangeInstrumentID,
                            productType=productType,
                            orderType=orderType,
                            orderSide=orderSide,
                            timeInForce=timeInForce,
                            disclosedQuantity=disclosedQuantity,
                            orderQuantity=placing_order_quantity,
                            limitPrice=limitPrice,
                            stopPrice=stopPrice,
                            orderUniqueIdentifier=orderUniqueIdentifier,
                            clientID=clientID,
                        )
                        print("_"*100)
                        print(response.json())
                        print("_"*100)
                        try:
                            order_status, message, order_price, trading_symbol, quantity_response, order_side = self.get_order_status(response.json()["result"]["AppOrderID"], clientID)
                            print(f"{order_status=}, {message=}, {order_price=}, {trading_symbol=}, {quantity_response=}, {order_side=}")
                        except Exception as e:
                            self.logger.log_message("error", f"Error in get_order_status: {e}")
                            response_codes[i] = 400
                            break


                        if 'filled' in order_status.lower():
                            # create a new row in the trade sheet if trading symbol is not present, it is not based on index
                            if exchangeInstrumentID not in self.trade_sheet['InstrumentID'].values:
                                # Create a new row with default values for other columns and TradingSymbol as trading_symbol
                                new_row = pd.DataFrame([{col: 0 for col in self.trade_sheet.columns}])
                                new_row.at[0, 'InstrumentID'] = exchangeInstrumentID
                                
                                # Append the new row using pd.concat
                                self.trade_sheet = pd.concat([self.trade_sheet, new_row], ignore_index=True)
                    

                            # Update the existing row
                            condition = self.trade_sheet['InstrumentID'] == exchangeInstrumentID

                            self.trade_sheet.loc[condition, 'TradingSymbol'] = trading_symbol

                            

                            
                            current_qty = float(self.trade_sheet.loc[condition, f'{order_side.capitalize()}Quantity'].iloc[0])
                            current_avg_price = float(self.trade_sheet.loc[condition, f'{order_side.capitalize()}AveragePrice'].iloc[0])
                            print(float(self.trade_sheet.loc[condition, f'{order_side.capitalize()}AveragePrice']))
                            print(f"{current_qty=}, {current_avg_price=}")
                            new_avg_price = ((current_avg_price * current_qty) + (order_price * quantity_response)) / (current_qty + quantity_response)
                            self.trade_sheet.loc[condition, f'{order_side.capitalize()}AveragePrice'] = float(new_avg_price)
                            self.trade_sheet.loc[condition, f'{order_side.capitalize()}Quantity'] += quantity_response

                            self.trade_sheet.loc[condition, 'LegPosition'] = leg_position
                            self.trade_sheet.loc[condition, 'NetQuantity'] = self.trade_sheet.loc[condition, 'BuyQuantity'] - self.trade_sheet.loc[condition, 'SellQuantity']
                            
                            self.write_trade_sheet(self.trade_sheet)
                            response_codes[i] = 200

                            self.logger.log_message("info", f"Order placed for {trading_symbol}; Instrument: {exchangeInstrumentID}; OrderSide: {order_side}; Quantity: {orderQuantity}")
                            self.telegramBot.send_message(f"Order placed for\n{trading_symbol}\nTicker:{strike_prices[i]}\nOrderSide: {order_side}\nQuantity: {orderQuantity}\nOrderPrice: {order_price}")

                            orderQuantity -= self.__freeze_quantity
                        else:
                            print(f"Order failed to place for {trading_symbol} leg: {i} retrying...")
                            for j in range(3):
                                response = self.place_order( # Call the place_order method
                                    exchangeSegment=exchangeSegment,
                                    exchangeInstrumentID=exchangeInstrumentID,
                                    productType=productType,
                                    orderType=orderType,
                                    orderSide=orderSide,
                                    timeInForce=timeInForce,
                                    disclosedQuantity=disclosedQuantity,
                                    orderQuantity=placing_order_quantity,
                                    limitPrice=limitPrice,
                                    stopPrice=stopPrice,
                                    orderUniqueIdentifier=orderUniqueIdentifier,
                                    clientID=clientID,
                                )
                                order_status, message, order_price, trading_symbol, quantity, order_side = self.get_order_status(response.json()["result"]["AppOrderID"], clientID)
                                print(f"{order_status}")
                                if 'filled' in order_status.lower():
                                    if exchangeInstrumentID not in self.trade_sheet['InstrumentID'].values:
                                        # Create a new row with default values for other columns and TradingSymbol as trading_symbol
                                        new_row = pd.DataFrame([{col: 0 for col in self.trade_sheet.columns}])
                                        new_row.at[0, 'InstrumentID'] = exchangeInstrumentID
                                        
                                        # Append the new row using pd.concat
                                        self.trade_sheet = pd.concat([self.trade_sheet, new_row], ignore_index=True)
                            

                                    # Update the existing row
                                    condition = self.trade_sheet['InstrumentID'] == exchangeInstrumentID

                                    self.trade_sheet.loc[condition, 'TradingSymbol'] = trading_symbol

                                    self.trade_sheet.loc[condition, f'{order_side.capitalize()}Quantity'] += quantity_response

                                    current_qty = float(self.trade_sheet.loc[condition, f'{order_side.capitalize()}Quantity'].iloc[0])
                                    current_avg_price = float(self.trade_sheet.loc[condition, f'{order_side.capitalize()}AveragePrice'].iloc[0])
                                    new_avg_price = ((current_avg_price * current_qty) + (order_price * quantity_response)) / (current_qty + quantity_response)
                                    self.trade_sheet.loc[condition, f'{order_side.capitalize()}AveragePrice'] = float(new_avg_price)

                                    self.trade_sheet.loc[condition, 'LegPosition'] = leg_position
                                    self.trade_sheet.loc[condition, 'NetQuantity'] = self.trade_sheet.loc[condition, 'BuyQuantity'] - self.trade_sheet.loc[condition, 'SellQuantity']
                                    
                                    self.write_trade_sheet(self.trade_sheet)
                                    response_codes[i] = 200

                                    self.logger.log_message("info", f"Order placed for {trading_symbol}; Instrument: {exchangeInstrumentID}; OrderSide: {order_side}; Quantity: {orderQuantity}")
                                    self.telegramBot.send_message(f"Order placed for\n{trading_symbol}\nTicker:{strike_prices[i]}\nOrderSide: {order_side}\nQuantity: {orderQuantity}\nOrderPrice: {order_price}")

                                    orderQuantity -= self.__freeze_quantity
                                    break
                                else:
                                    self.logger.log_message("error", f"Order failed to place for {trading_symbol} leg {i} at retry level: {j}")
                                    self.telegramBot.send_message(f"Order failed to place {trading_symbol}\nleg: {i}\nTicker:{strike_prices[i]}\nOrderSide: {orderSide}\nQuantity: {orderQuantity}\nOrderPrice: {order_price}")
                                    response_codes[i] = 500          
        if hard_squareoff:
            self.logger.log_message("info", "Hard squareoff completed")
            self.telegramBot.send_message("Hard squareoff completed")
        return response_codes
     


    def place_order(
        self,
        exchangeSegment,
        exchangeInstrumentID,
        productType,
        orderType,
        orderSide,
        timeInForce,
        disclosedQuantity,
        orderQuantity,
        limitPrice,
        stopPrice,
        orderUniqueIdentifier,
        clientID=None,
    ):
        try:
            params = {
                "exchangeSegment": exchangeSegment,
                "exchangeInstrumentID": exchangeInstrumentID,
                "productType": productType,
                "orderType": orderType,
                "orderSide": orderSide,
                "timeInForce": timeInForce,
                "disclosedQuantity": disclosedQuantity,
                "orderQuantity": orderQuantity,
                "limitPrice": limitPrice,
                "stopPrice": stopPrice,
                "orderUniqueIdentifier": orderUniqueIdentifier,
                "clientID": clientID
            }

            response = self._make_request(self.endpoints['place_order'], data=params)
            return response
        except Exception as e:
            self.logger.log_message("error", f"Error in place_order: {e}")


# from concurrent.futures import ThreadPoolExecutor, as_completed


# def main():
#     api = SymphonyInteractiveAPI(1, "NIFTY", None)
#     print(2)
#     api.get_headers()
#     print(3)

#     order_params = {
#         "exchangeSegment": "NSEFO",
#         "exchangeInstrumentID": 64444,
#         "productType": "NRML",
#         "orderType": "LIMIT",
#         "orderSide": "BUY",
#         "timeInForce": "DAY",
#         "disclosedQuantity": 0,
#         "orderQuantity": 25,
#         "limitPrice": 0.5,
#         "stopPrice": 0,
#         "orderUniqueIdentifier": "454845",
#         "clientID": "*****",
#     }

#     with ThreadPoolExecutor(max_workers=500) as executor:
#         futures = [executor.submit(api.place_order, **order_params) for _ in range(400)]
        
#         for future in as_completed(futures):
#             response = future.result()
#             print(response.json())

# if __name__ == "__main__":
#     main()
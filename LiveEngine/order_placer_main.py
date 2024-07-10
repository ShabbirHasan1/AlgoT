from ordering_engine import SymphonyInteractiveAPI
import pandas as pd
import time
from main import get_options_data, DF_OPTIONS_DATA
import constants
from logging_config import CustomLogger, TelegramBot, ConfirmationPopup
from datetime import datetime
import tkinter as tk
import json

# Create the root window
root = tk.Tk()
root.withdraw()  # Hide the root window

# Initialize the SymphonyInteractiveAPI
api = SymphonyInteractiveAPI()
api.login()
print("Logged in")

# Initialize the logger
logger = CustomLogger('order_placer_main.py')

# Initialize the Telegram Bot
telegram_bot = TelegramBot()

def squareoff_last_updated_on_sheet(position, df_options_data, response_codes=[200, 200, 200, 200], datetime_value=datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
    df_options_data_last_row = df_options_data.iloc[-1].copy()
    # put last rows date with time 15:30
    df_options_data_last_row['Datetime'] = datetime_value
    df_options_data_last_row['position'] = position
    # append the last row to the df
    df_options_data.loc[len(df_options_data)] = df_options_data_last_row
    df_options_data.loc[df_options_data.index[-1], ['atmceS', 'atmpeS', 'wingceS', 'wingpeS']] = [200, 200, 200, 200]
    df_options_data.to_csv(f"last_update_options_brain.csv", index=False)
    # read write to daily_json the resposne codes
    with open(f"LiveEngine/daily.json", 'r') as f:
        data = json.load(f)
        for i in range (4):
            data[f'leg{i+1}'] = response_codes[i]
    with open(f"LiveEngine/daily.json", 'w') as f:
        json.dump(data, f)


if __name__ == "__main__":
    # Read the initial trade sheet data
    df_options_data = pd.read_csv("last_update_options_brain.csv")
    has_hard_squareoff = False

    # Check if the last entry in last_update_options_brain.csv is of the current date
    last_datetime = df_options_data.iloc[-1]['Datetime']
    last_day = int(last_datetime.split()[0].split("-")[2])  # Extract the day part from the timestamp
    current_day = time.localtime().tm_mday
    if current_day == last_day:
        # if time is past 15:30
        # check if atmceS, atmpeS, wingceS, wingpeS are not 200 for last row
        # if so then remove last row
        if df_options_data.iloc[-1]['position'] == "hard-squareoff":
            print("Exiting because of hard squareoff on same day")
            quit()
            
        try:
            if df_options_data.iloc[-1]['atmceS'] != 200 or df_options_data.iloc[-1]['atmpeS'] != 200 or df_options_data.iloc[-1]['wingceS'] != 200 or df_options_data.iloc[-1]['wingpeS'] != 200:
                df_options_data = df_options_data.iloc[:-1]
                df_options_data.to_csv(f"last_update_options_brain.csv", index=False)
        except:
            pass

        if time.localtime().tm_hour == 15 and time.localtime().tm_min >= 30:
            squareoff_last_updated_on_sheet("squareoff", df_options_data, datetime_value=df_options_data.iloc(-1, ['Datetime']).split()[0] + " 15:30:00")

        if df_options_data.iloc[-1]['position'] == "hard-squareoff":
            print("Exiting because of hard squareoff on same day")
            quit()
        
        
        last_data = df_options_data.iloc[-1]
        # if not, then create a new csv file
        instruments = [int(last_data["atmce"]), int(last_data["atmpe"]), int(last_data["wce"]), int(last_data["wpe"])] 
        positions = {'beginx': [-1, -1, 1, 1], 'buy': [0, -2, 2, 2], 'sell': [-2, 0, 2, 2], 'squareoff': [0, 0, 0, 0], 'hold': [0, 0, 0, 0]}
        position = last_data["position"]
        api.trade_sheet = api.read_trade_sheet()
    else:
        api.trade_sheet = api.reset_trade_sheet()
        squareoff_last_updated_on_sheet("squareoff", df_options_data)


        
    while True:
        # Read the latest trade sheet data
        try:
            latest_df = pd.read_csv("options_brain.csv")
        except:
            time.sleep(1)
            continue

        # Identify new trades based on changes in the last row's "position" value
        latest_position = latest_df.iloc[-1]['position']
        prev_position = df_options_data.iloc[-1]['position']

        if latest_position != prev_position:
            # check if we are in the same 15 min interval as the last trade
            latest_datetime = latest_df.iloc[-1]['Datetime']

            if not time.localtime().tm_hour == latest_datetime.split()[1].split(":")[0] and time.localtime().tm_min == latest_datetime.split()[1].split(":")[1]:
                # compare the date as well
                if not time.localtime().tm_mday == int(latest_datetime.split()[0].split("-")[2]):
                    print("Not executing because of different time interval")
                    continue

            # Place the order using the SymphonyInteractiveAPI for the last trade
            print("Placing order...")
            telegram_bot.send_message(f"Placing order for {latest_df.iloc[-1]['position']}")
            # response_codes = api.place_order_handler(latest_df.iloc[-1], latest_df.iloc[-2]['position'])
            response_codes = api.place_order_handler(latest_df.iloc[-1], prev_position)
            # sum the response codes to check if all orders were placed successfully
            # check if all response codes are 200
            df_options_data = latest_df
            # write to the last row of df df_options_data
            df_options_data.loc[df_options_data.index[-1], ['atmceS', 'atmpeS', 'wingceS', 'wingpeS']] = response_codes
            df_options_data.to_csv("last_update_options_brain.csv", index=False)

            if all([response == 200 for response in response_codes]):
                print("All orders placed successfully")
            else:
                print("Some orders failed to place, respose codes: ", response_codes)
                # Create the root window
                root = tk.Tk()
                root.withdraw()  # Hide the root window
                                

                # Create and show the confirmation popup
                confirmation_popup = ConfirmationPopup(root, "Some orders failed to place, respose codes: " + str(response_codes) + "\nDo you want to continue?")
                result = confirmation_popup.show()
                
                if result == "No":
                    print("Exiting")
                    quit()
                else:
                    df_options_data.loc[df_options_data.index[-1], ['atmceS', 'atmpeS', 'wingceS', 'wingpeS']] = [200, 200, 200, 200]
                    df_options_data.to_csv("last_update_options_brain.csv", index=False)
                    root.quit()




            # check if time is past 15:00 or is 15:00 and less than 30 minutes
            # then check if position is squareoff
            # if no, then squareoff all positions
        
        # checking stop loss
        is_stop_loss = api.check_stoploss(latest_position)

        if (time.localtime().tm_hour == 15 and time.localtime().tm_min >= 15 and time.localtime().tm_min <= 30) or is_stop_loss:
            if not has_hard_squareoff:
                telegram_bot.send_message(f"Placing order for Auto Squareoff")
                response_codes = api.place_order_handler(latest_df.iloc[-1], latest_df.iloc[-1]['position'], hard_squareoff=True)
                
                # write to the last row of df df_options_data
                print(response_codes)
                squareoff_last_updated_on_sheet("hard-squareoff", df_options_data, response_codes)
                if all([response == 200 for response in response_codes]):
                    print("All squareoff orders placed successfully")
                    has_hard_squareoff = True
                    quit()
                else:

                    print("Some squareoff orders failed to place")
                    # Create the root window
                    root = tk.Tk()
                    root.withdraw()  # Hide the root window
                    # Create and show the confirmation popup
                    confirmation_popup = ConfirmationPopup(root, "Some orders failed to place, respose codes: " + str(response_codes) + "\nDo you want to continue?")
                    result = confirmation_popup.show()
                    
                    if result == "No":
                        print("Exiting")
                        quit()
                    else:
                        df_options_data.loc[df_options_data.index[-1], ['atmceS', 'atmpeS', 'wingceS', 'wingpeS']] = [200, 200, 200, 200]
                        df_options_data.to_csv("last_update_options_brain.csv", index=False)
                        root.quit()
            else:
                pass
        else:
            has_hard_squareoff = False

        positions = {'beginx': [-1, -1, 1, 1], 'buy': [0, -2, 2, 2], 'sell': [-2, 0, 2, 2], 'squareoff': [0, 0, 0, 0], 'hold': [0, 0, 0, 0]}

            

        # Wait for a specified interval before checking for updates again
        time.sleep(constants.ORDER_PLACER_INTERVAL)

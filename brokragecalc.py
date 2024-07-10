import pandas as pd
import numpy as np

#load data
bt2023 = pd.read_csv(r'D:\AlgoT\data\15min\backtest2023.csv') 
bt2024 = pd.read_csv(r'D:\AlgoT\data\15min\backtest2024.csv')
bt2022 = pd.read_csv(r'D:\AlgoT\data\15min\backtest2022.csv')
bt2021 = pd.read_csv(r'D:\AlgoT\data\15min\backtest2021.csv')
bt2020 = pd.read_csv(r'D:\AlgoT\data\15min\backtest2020.csv')
bt2019 = pd.read_csv(r'D:\AlgoT\data\15min\backtest2019.csv')

btall = [bt2019, bt2020, bt2021, bt2022, bt2023, bt2024]
data = pd.concat(btall, ignore_index=True)



def calculate_brokerage_from_positions(data):
    # Initialize variables
    total_brokerage = 0
    daily_brokerage = {}
    daily_signals = {}
    last_signal = None
    brokerage_per_lot = 10
    current_day = None
    
    for index, row in data.iterrows():
        timestamp = pd.to_datetime(row['Datetime'])
        signal = row['position']
        
        # New day check
        if current_day != timestamp.date():
            if current_day is not None:
                # Save the previous day's data
                daily_brokerage[current_day] = {
                    'brokerage': daily_signals[current_day]['brokerage'],
                    'order_count': daily_signals[current_day]['order_count']
                }
            # Reset the daily values
            current_day = timestamp.date()
            daily_signals[current_day] = {'signals': [], 'brokerage': 0, 'order_count': 0}
            last_signal = None

        # Only act on signal changes or the first occurrence of a signal for the day
        if signal != last_signal:
            last_signal = signal
            # Determine number of new orders based on signal type
            new_orders = 4 if signal in ['beginx', 'buy', 'sell', 'squareoff', 'hard-squareoff'] else 0
            
            # Update the daily signals data
            daily_signals[current_day]['order_count'] += new_orders
            daily_signals[current_day]['brokerage'] += new_orders * brokerage_per_lot
            total_brokerage += new_orders * brokerage_per_lot

        # Append current signal to daily list
        daily_signals[current_day]['signals'].append(signal)

    # Handle last day in data
    if current_day is not None:
        daily_brokerage[current_day] = {
            'brokerage': daily_signals[current_day]['brokerage'],
            'order_count': daily_signals[current_day]['order_count']
        }

    # Prepare DataFrame data
    data_for_df = [
        {
            'Date': date, 
            'Daily Brokerage': info['brokerage'], 
            'Order Count': info['order_count'], 
            'Signals': ', '.join(info['signals'])
        }
        for date, info in daily_signals.items()
    ]
    
    return daily_brokerage, total_brokerage, data_for_df

# Calculate the total brokerage using the function
daily_brokerage, total_brokerage, data_for_df = calculate_brokerage_from_positions(data)

# Create and display the DataFrame showing each day's activity
df_daily_brokerage = pd.DataFrame(data_for_df)
df_daily_brokerage.sort_values(by='Date', inplace=True)
print(df_daily_brokerage)
print("Total Brokerage:", total_brokerage)

#save to csv
df_daily_brokerage.to_csv(r"D:\AlgoT\brokrage\brokrage.csv")
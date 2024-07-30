# import pandas as pd
# from chart_patterns.chart_patterns.doubles import find_doubles_pattern
# from chart_patterns.chart_patterns.plotting import display_chart_pattern

# # Read in your OHLC data
# csv_path = r"D:\AlgoT\yfinanceData\BAJAJ-AUTO.NS_OHLC_2023.csv"
# try:
#     ohlc = pd.read_csv(csv_path)
#     print(f"Data loaded successfully from {csv_path}")
#     ohlc.columns=['date', 'open', 'high', 'low', 'close','adjclose', 'volume']
    
# except FileNotFoundError:
#     print(f"File not found: {csv_path}")
#     raise

# # Check if the necessary columns exist in the dataframe
# required_columns = ['open', 'high', 'low', 'close']
# if not all(column in ohlc.columns for column in required_columns):
#     raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

# # Find the double bottom pattern
# ohlc = find_doubles_pattern(ohlc, double="bottoms")
# print("Double bottom patterns found and annotated in the DataFrame.")

# # Find the double top pattern
# ohlc = find_doubles_pattern(ohlc, double="tops")
# print("Double top patterns found and annotated in the DataFrame.")

# # Plot the results
# try:
#     display_chart_pattern(ohlc, pattern="double")  # If multiple patterns were found, then plots will be saved inside a folder named images/double
#     print("Chart patterns plotted and saved successfully.")
# except Exception as e:
#     print(f"An error occurred while plotting the chart patterns: {e}")
#     raise


import pandas as pd
import os
from chart_patterns.chart_patterns.head_and_shoulders import find_head_and_shoulders
from chart_patterns.chart_patterns.plotting import display_chart_pattern

# Specify the directory containing your CSV files
directory_path = r"D:\AlgoT\yfinanceData"

# Loop through each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(directory_path, filename)
        try:
            # Read in your OHLC data
            ohlc = pd.read_csv(file_path)
            ohlc.columns=['date', 'open', 'high', 'low', 'close','adjclose', 'volume']
            
            print(f"Data loaded successfully from {file_path}")

            # Check if the necessary columns exist in the dataframe
            required_columns = ['open', 'high', 'low', 'close']
            if not all(column in ohlc.columns for column in required_columns):
                raise ValueError(f"The CSV file {filename} must contain the following columns: {required_columns}")

            # Find the head and shoulders pattern
            ohlc = find_head_and_shoulders(ohlc)
            print(f"Head and shoulders patterns found and annotated in the DataFrame for {filename}.")
            display_chart_pattern(ohlc, pattern="hs")
            # Plot the results
            try:
                display_chart_pattern(ohlc, pattern="hs")  # If multiple patterns were found, then plots will be saved inside a folder named images/hs
                print(f"Chart patterns plotted and saved successfully for {filename}.")
            except Exception as e:
                print(f"An error occurred while plotting the chart patterns for {filename}: {e}")
                raise

        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"An error occurred with file {filename}: {e}")


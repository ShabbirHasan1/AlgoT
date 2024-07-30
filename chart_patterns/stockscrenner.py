# import pandas as pd
# from chart_patterns.doubles import find_doubles_pattern
# from chart_patterns.plotting import display_chart_pattern



# # read in your ohlc data 
# ohlc = pd.read_csv(r"D:\AlgoT\yfinanceData\BAJAJ-AUTO.NS_OHLC_2023.csv")  #headers include - open, high, low, close

# # Find the double bottom pattern
# ohlc = find_doubles_pattern(ohlc, double="bottoms")

# # Find the double tops pattern
# ohlc = find_doubles_pattern(ohlc, double="tops")


# # Plot the results 
# display_chart_pattern(ohlc, pattern="double") # If multiple patterns were found, then plots will saved inside a folder named images/double  
import pandas as pd
from chart_patterns.doubles import find_doubles_pattern
from chart_patterns.plotting import display_chart_pattern

# Read in your OHLC data
csv_path = r"D:\AlgoT\yfinanceData\BAJAJ-AUTO.NS_OHLC_2023.csv"
try:
    ohlc = pd.read_csv(csv_path)
    print(f"Data loaded successfully from {csv_path}")
except FileNotFoundError:
    print(f"File not found: {csv_path}")
    raise

# Check if the necessary columns exist in the dataframe
required_columns = ['open', 'high', 'low', 'close']
if not all(column in ohlc.columns for column in required_columns):
    raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

# Find the double bottom pattern
ohlc = find_doubles_pattern(ohlc, double="bottoms")
print("Double bottom patterns found and annotated in the DataFrame.")

# Find the double top pattern
ohlc = find_doubles_pattern(ohlc, double="tops")
print("Double top patterns found and annotated in the DataFrame.")

# Plot the results
try:
    display_chart_pattern(ohlc, pattern="double")  # If multiple patterns were found, then plots will be saved inside a folder named images/double
    print("Chart patterns plotted and saved successfully.")
except Exception as e:
    print(f"An error occurred while plotting the chart patterns: {e}")
    raise

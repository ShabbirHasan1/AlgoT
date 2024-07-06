import pandas as pd
import numpy as np
import datetime


options_df = pd.read_csv(r"D:\all_data\DATA\BANKNIFTY_FNO_Data_2017\BANKNIFTY_2017_OPTIONS.csv")
futures_df = pd.read_csv(r"D:\all_data\DATA\BANKNIFTY_FNO_Data_2017\BANKNIFTY_2017_FUTURES.csv")

print(options_df.head())
print(futures_df.head())


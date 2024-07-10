from fastapi import FastAPI
import json
import uvicorn

# Create an instance of the FastAPI class
app = FastAPI()

def read_stock_data():
    filepath = 'D:/HirakDrive/workspace/GitHub/Endovia/LiveEngine/daily.json'
    stock_data = {}
    net_m2m = 0
    
    with open(filepath, 'r') as file:
        data = json.load(file)    
        # Update stock data
        stock_data['Nifty'] = data             
        # upper round off to 0 decimal places
        stock_data['Nifty']['Current M2M'] = round(stock_data['Nifty']['Current M2M'], 0)
        stock_data['Nifty']['Stoploss Price'] = round(stock_data['Nifty']['Stoploss Price'], 0)
        net_m2m += stock_data['Nifty']['Current M2M']
        print(stock_data)
    
    return stock_data, net_m2m

@app.get('/nifty_data')
def dashboard_data():
    # Read stock data from files
    stock_data, net_m2m = read_stock_data()
    # Return the stock data as JSON
    return stock_data

if __name__ == "__main__":
    # Run the FastAPI application
    uvicorn.run(app,port=5005,host='0.0.0.0')

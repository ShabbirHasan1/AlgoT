instances = [
    "ICICIBANK",
    "HDFCBANK",
    "AXISBANK",
    "INDUSINDBK",
    "SBIN",
    "INFY",
    "TCS",
    "WIPRO",
    "RELIANCE",
    "ULTRACEMCO",
    "BAJFINANCE",
    "DLF",
    "LT",
    "HINDUNILVR",
    "TATAMOTORS",
    "TITAN",
    "MARUTI",
    "JSWSTEEL",
    "HINDALCO",
    "ASIANPAINT",
    "BHARTIARTL",
    "EICHERMOT",
    "BAJAJAUTO",
    "HDFCLIFE",
    "TECHM",
    "TATACHEM"
]
#instances = ]
import subprocess
import os
# List of instances you want to run
#instances = [ "RELIANCE","DLF","TATAMOTORS"]  # Add your instance names here
os.chdir(r"D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/")
# Path to your virtual environment's activate script
venv_activate_script = r"D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/.venv/Scripts/Activate.ps1"  # Update with the actual path
instance_path =  "D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/main.py"
#instance_path1 =  "D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/order_placer_main.py"

# Loop through each instance and run main.py with it in a new PowerShell window



import subprocess
import time
for instance in instances:
    # Command to activate venv and run main.py in a new PowerShell window
    command = f"Start-Process powershell -ArgumentList '-noexit', '-command', '. {venv_activate_script}; python \"{instance_path}\" {instance}'"
    print(f"Running command: {command}")
    subprocess.run(["powershell", "-Command", command], shell=True)

    
    #Comand to run order_placer_main.py
    
time.sleep(5)
#for instance in instances:    
    #command1 = f"Start-Process powershell -ArgumentList '-noexit', '-command', '. {venv_activate_script}; python \"{instance_path1}\" {instance}'"
    #print(f"Running command: {command1}")
    #subprocess.run(["powershell", "-Command", command1], shell=True)
    
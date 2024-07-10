import subprocess
import tkinter as tk
import os

# instances = [
#     "HDFCBANK", "AXISBANK", "INDUSINDBK", "SBIN", "INFY", "TCS",
#     "BAJFINANCE", "DLF", "LT", "TITAN", "MARUTI", "JSWSTEEL", "HINDALCO", "ASIANPAINT",
#     "BHARTIARTL", "SUNPHARMA","TATAMOTORS","RELIANCE","ULTRACEMCO","BAJAJ-AUTO"
# ]
#instances = ["MARUTI","DLF","HDFCBANK","INFY","BAJFINANCE","ULTRACEMCO","RELIANCE","SUNPHARMA","TATAMOTORS"]
instances=["MARUTI","DLF","JSWSTEEL","HDFCBANK","INFY"]
#instances =["AXISBANK","BAJFINANCE","HDFCBANK","BHARTIARTL","TCS","ULTRACEMCO","DLF","MARUTI","RELIANCE","SUNPHARMA","TATAMOTORS"]
#instances = [ "RELIANCE","AXISBANK","TATAMOTORS","MARUTI","TITAN"]  # Add your instance names here
os.chdir(r"D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/")
venv_activate_script = r"D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/.venv/Scripts/Activate.ps1"
instance_path = "D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/main.py"
instance_path1 = "D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/order_placer_main.py"

running_instances = {}

def run_command(command,title):
    process = subprocess.Popen(["powershell", "-Command", f"$Host.UI.RawUI.WindowTitle = '{title}'; {command}"], shell=True)
    running_instances[process.pid] = command

def start_instances():
    for instance in instances:
        command = f"Start-Process powershell -ArgumentList '-noexit', '-command', '. {venv_activate_script}; python \"{instance_path}\" {instance}'"
        print(f"Starting instance: {instance}")
        run_command(command,instance)

def start_order_placer_instances():
    for instance in instances:
        command = f"Start-Process powershell -ArgumentList '-noexit', '-command', '. {venv_activate_script}; python \"{instance_path1}\" {instance}'"
        print(f"Starting order placer instance: {instance}")
        run_command(command,instance)

def stop_instance(pid):
    command = running_instances.get(pid)
    if command:
        print(f"Stopping instance with PID {pid}: {command}")
        subprocess.run(["taskkill", "/F", "/PID", str(pid)], shell=True)
        del running_instances[pid]
    else:
        print(f"No instance found with PID {pid}")

def stop_instances():
    for pid in list(running_instances.keys()):
        stop_instance(pid)

def update_instances_listbox():
    instances_listbox.delete(0, tk.END)
    for pid in running_instances.keys():
        instances_listbox.insert(tk.END, pid)

root = tk.Tk()
root.title("Instance Manager")

# Set background color
root.configure(bg="lightgray")

start_button = tk.Button(root, text="Start Instances", command=start_instances, bg="green", fg="white")
start_button.pack()

start_order_placer_button = tk.Button(root, text="Start Order Placer Instances", command=start_order_placer_instances, bg="blue", fg="white")
start_order_placer_button.pack()

stop_all_button = tk.Button(root, text="Stop All Instances", command=stop_instances, bg="red", fg="white")
stop_all_button.pack()

instances_label = tk.Label(root, text="Running Instances:", bg="lightgray")
instances_label.pack()

instances_listbox = tk.Listbox(root)
instances_listbox.pack()

stop_selected_button = tk.Button(root, text="Stop Selected Instance", command=lambda: stop_instance(int(instances_listbox.get(tk.ACTIVE))), bg="orange", fg="white")
stop_selected_button.pack()

def update_listbox_periodically():
    update_instances_listbox()
    root.after(1000, update_listbox_periodically)

update_listbox_periodically()

root.mainloop()

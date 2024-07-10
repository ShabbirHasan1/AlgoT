import subprocess
import tkinter as tk
import os

instances = ["DLF"]

os.chdir(r"D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/")
venv_activate_script = r"D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/.venv/Scripts/activate"
instance_path = "D:/ALGO/Soham/Equities/Endovia/LiveEngineMulti/main.py"

running_instances = {}

def run_command(command, title):
    process = subprocess.Popen(command, shell=True)
    running_instances[process.pid] = title

def start_instances():
    for instance in instances:
        command = f"cmd /k title Instance: {instance} && call {venv_activate_script} && python \"{instance_path}\" {instance}"
        print(f"Starting instance: {instance}")
        run_command(command, instance)

def stop_instance(pid):
    if pid in running_instances:
        print(f"Stopping instance with PID {pid}: {running_instances[pid]}")
        subprocess.run(["taskkill", "/F", "/PID", str(pid)], shell=True)
        del running_instances[pid]
    else:
        print(f"No instance found with PID {pid}")

def stop_instances():
    for pid in list(running_instances.keys()):
        stop_instance(pid)

def update_instances_listbox():
    instances_listbox.delete(0, tk.END)
    for pid, title in running_instances.items():
        instances_listbox.insert(tk.END, f"{pid}: {title}")

root = tk.Tk()
root.title("Instance Manager")
root.configure(bg="pink")

start_button = tk.Button(root, text="Start Instances", command=start_instances, bg="green", fg="white")
start_button.pack()

stop_all_button = tk.Button(root, text="Stop All Instances", command=stop_instances, bg="red", fg="white")
stop_all_button.pack()

instances_label = tk.Label(root, text="Running Instances:", bg="lightgray")
instances_label.pack()

instances_listbox = tk.Listbox(root)
instances_listbox.pack()

stop_selected_button = tk.Button(root, text="Stop Selected Instance", command=lambda: stop_instance(int(instances_listbox.get(tk.ACTIVE).split(":")[0])), bg="orange", fg="white")
stop_selected_button.pack()

def update_listbox_periodically():
    update_instances_listbox()
    root.after(1000, update_listbox_periodically)

update_listbox_periodically()

root.mainloop()

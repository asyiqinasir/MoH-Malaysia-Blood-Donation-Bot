#pip install schedule
import schedule
import time
import subprocess

def run_fetch_data():
    subprocess.run(["python", "fetch_data_latest_commit.py"])

def run_send_to_telegram():
    subprocess.run(["python", "send_to_telegram.py"])

schedule.every(1).hours.do(run_fetch_data)
schedule.every(1).hours.at(":30").do(run_send_to_telegram)

while True:
    schedule.run_pending()
    time.sleep(1)

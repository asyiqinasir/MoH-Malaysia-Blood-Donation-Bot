# MoH-Malaysia-Blood-Donation-Bot

## Introduction
This project is designed to automate the process of fetching, analyzing, and sharing updates on blood donation data in Malaysia. It uses datasets provided by the Ministry of Health Malaysia and includes a Telegram bot for disseminating information.

## Files Description

### Dataset Folder
- **data-darah-public**: Contains CSV files of daily donation updates. Data is sourced from [MoH Malaysia's official GitHub repository](https://github.com/MoH-Malaysia/data-darah-public).
- **data-granular**: Includes detailed donor retention data. This information is fetched from [an external data source](https://dub.sh/ds-data-granular).

### EDA.ipynb
An exploratory data analysis (EDA) Jupyter notebook that visualizes the donation data. This notebook serves as a preliminary step to visualize data before compiling it into a Python script. Take a peek to see the visualizations!

### fetch_data_latest_commit.py
A Python script designed to fetch the latest data using API calls. The fetched data is stored in the dataset folder mentioned above.

### requirements.txt
A file listing all the Python packages and their versions required to run the project. This ensures that the project's environment is easily replicable.

### scheduler.py
A scheduler script that automates the data fetching process (`fetch_data_latest_commit.py`) and sends the results to a designated Telegram group using the Bot (`send_to_telegram.py`).

## Installation

To set up this project, clone this repository to your local machine and install the required Python packages:

```bash
git clone https://github.com/asyiqinasir/MoH-Malaysia-Blood-Donation-Bot.git
cd MoH-Malaysia-Blood-Donation-Bot
pip install -r requirements.txt
```

## Usage
1. Setting up the Telegram Bot: Follow the instructions provided by Telegram's BotFather to create your bot and obtain your BOT_TOKEN.
2. Configuring the Environment: Set your BOT_TOKEN and CHAT_ID in an .env file or as environment variables.
3. Running the Scheduler: Execute scheduler.py to start the automated data fetching and sharing process.

import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates
import seaborn as sns
from dotenv import load_dotenv
import asyncio
from telegram import Bot
import nest_asyncio
import glob

load_dotenv()

# Telegram Bot configuration
bot_token = os.environ.get('BOT_TOKEN')
chat_id = os.environ.get('CHAT_ID')

# Initialize the Telegram Bot
bot = Bot(token=bot_token)

#check if the data was fetched
if not os.path.exists('data_fetched.txt'):
    print("File not exists. Exiting script.")
    exit()
    
#if the file exists
with open('data_fetched.txt', 'r') as flag_file:
    if flag_file.read().strip() != 'Data fetched':
        print("Data not fetched. Exiting script.")
        exit()


# Function to load data from a CSV file
def load_data(file_path):
    data = pd.read_csv(file_path)
    return data

# Function to convert date columns to datetime
def convert_date(df, column='date'):
    if df[column].dtype != pd.to_datetime(df[column]).dtype:
        df[column] = pd.to_datetime(df[column])
    return df

# Function to send all PNG files in a folder
def send_all_images_in_folder(folder_path):
    png_files = glob.glob(os.path.join(folder_path, '*.png'))
    for png_file in png_files:
        send_image(png_file)

def send_image(image_path):
    asyncio.get_event_loop().run_until_complete(send_image_async(image_path))

async def send_image_async(image_path):
    with open(image_path, 'rb') as image_file:
        await bot.send_photo(chat_id=chat_id, photo=image_file)

# Function to get the latest donation count and today's date
def get_latest_donation_count(data):
    today = pd.to_datetime(datetime.today().date())
    
    # Filter data for 'Malaysia'
    malaysia_data = data[data['state'] == 'Malaysia']

    # Get the latest date in the data
    latest_date = malaysia_data['date'].max()

    if latest_date == today:
        latest_data = malaysia_data[malaysia_data['date'] == latest_date]

        total_donations = latest_data['daily'].sum()

        return today, total_donations
    else:
        return today, None

async def send_latest_donation_info(data):
    latest_date = data['date'].max()  #get the latest date
    latest_data = data[data['date'] == latest_date]  #filter the data for the latest date only

    if not latest_data.empty:
        total_donations = latest_data['daily'].iloc[0]  #assume the column name for 'daily' is 'total_donations'
        formatted_latest_date = latest_date.strftime('%Y-%m-%d')
        message = f"""Total blood donations today: +{total_donations}
(last update: {formatted_latest_date})"""
    else:
        formatted_latest_date = latest_date.strftime('%Y-%m-%d')
        message = f"No donation data available for {formatted_latest_date}"

    await bot.send_message(chat_id=chat_id, text=message)


# Function to count new donors by year and create a bar chart
def count_new_donors_by_year(data, start_year, end_year):
    data['date'] = pd.to_datetime(data['date'])
    data['year'] = data['date'].dt.year
    annual_new_donors = data[data['year'].between(start_year, end_year)].groupby('year')['total'].sum()

    plt.bar(annual_new_donors.index, annual_new_donors, color='maroon', width=0.5)
    plt.xlabel('Year')
    plt.ylabel('Total New Donors')

    title_font = {'fontfamily': 'serif', 'fontsize': 15, 'fontweight': 'bold'}
    plt.title(f'Annual New Donors from {start_year} to {end_year}', fontdict=title_font)

    # Manually set tick locations and labels for better spacing
    plt.xticks(annual_new_donors.index, rotation=0)

    plt.tight_layout()

    #create 'output' folder if it doesn't exist
    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    #save the plot in the 'output' folder
    plt.savefig(os.path.join(output_folder, '1-new_donors_plot.png'))

    plt.show()

    return annual_new_donors

#function to plot monthly blood donation trends and create a line chart
def plot_blood_donation_trends(df, start_year, end_year):
    # Ensure 'date' is in datetime format
    df['date'] = pd.to_datetime(df['date'])

    filtered_df = df[(df['date'].dt.year >= start_year) & (df['date'].dt.year <= end_year) & (df['state'] == 'Malaysia')]

    monthly_total_donations = filtered_df.groupby([filtered_df['date'].dt.to_period('M')])['daily'].sum().reset_index()

    monthly_total_donations['date'] = monthly_total_donations['date'].dt.to_timestamp()

    monthly_total_donations.rename(columns={'daily': 'total_donations'}, inplace=True)

    plt.figure(figsize=(15, 6))
    sns.lineplot(x='date', y='total_donations', data=monthly_total_donations, color='maroon', marker='o')
    plt.title(f'Monthly Blood Donations Trend in Malaysia ({start_year} - {end_year})')
    plt.xlabel('Month and Year')
    plt.ylabel('Total Donations')

    #format x-axis to show 'Month Year' for each tick
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))

    #set major locator to every 3rd month for less clutter
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    #optionally, can add minor ticks with a different interval
    plt.gca().xaxis.set_minor_locator(mdates.MonthLocator(interval=6))

    plt.xticks(rotation=90)  # Rotate for better readability

    plt.grid(True, which='both', linestyle='--', linewidth=0.5)

    plt.tight_layout()  # Adjust layout to fit all labels

    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    plt.savefig(os.path.join(output_folder, '2-monthly_donations_trend.png'))
    plt.show()

async def analyze_donor_data(parquet_file):
    data = pd.read_parquet(parquet_file)

    # convert 'visit_date' to datetime and extract the year
    data['visit_date'] = pd.to_datetime(data['visit_date'])
    data['donation_year'] = data['visit_date'].dt.year

    #find the first donation year for each donor
    first_donation_year = data.groupby('donor_id')['donation_year'].min().reset_index()
    first_donation_year.rename(columns={'donation_year': 'first_donation_year'}, inplace=True)

    #merge df
    data_with_first_year = pd.merge(data, first_donation_year, on='donor_id')

    #mark 'new' donors in their first ever donation year
    data_with_first_year['donor_status'] = 'Returning'
    data_with_first_year.loc[data_with_first_year['donation_year'] == data_with_first_year['first_donation_year'], 'donor_status'] = 'New'

    #calculate days between visits & age at visit
    data_with_first_year['previous_visit_date'] = data_with_first_year.groupby('donor_id')['visit_date'].shift(1)
    data_with_first_year['days_between_visits'] = (data_with_first_year['visit_date'] - data_with_first_year['previous_visit_date']).dt.days
    data_with_first_year['age_at_visit'] = data_with_first_year['donation_year'] - data_with_first_year['birth_date']

    # analyze returning donors
    returning_donors = data_with_first_year[data_with_first_year['donor_status'] == 'Returning'].copy()
    returning_donors['years_since_first_donation'] = (returning_donors['visit_date'] - pd.to_datetime(returning_donors['previous_visit_date'])).dt.days / 365

    #calculate returning rate (a.k.a retention rates) for 1 to 5 years
    returning_rates = {}
    for years in range(1, 6):
        returning_rates[f'return_within_{years}_years'] = (returning_donors['years_since_first_donation'] <= years).mean()
        returning_rates[f'return_within_{years}_years_formatted'] = "{:.2%}".format(returning_rates[f'return_within_{years}_years'])

    most_recent_date = data_with_first_year['visit_date'].max()
    most_old_date = data_with_first_year['visit_date'].min()

    message = f"""The "retention rate" refers to the frequency with which donors return to donate again after their first donation. 
Blood banks monitor this rate closely as it provides insights into donor retention and the sustainability of the blood supply. 
 - Retention rate 1 year: {returning_rates['return_within_1_years_formatted']} 
 - Retention rate 2 years: {returning_rates['return_within_2_years_formatted']}
 - Retention rate 3 years: {returning_rates['return_within_3_years_formatted']}
 - Retention rate 4 years: {returning_rates['return_within_4_years_formatted']}
 - Retention rate 5 years: {returning_rates['return_within_5_years_formatted']}
On average, in 1 year, {returning_rates['return_within_1_years_formatted']} will return and donate after their first donation. 
(last update: {most_old_date.strftime('%Y-%m-%d')}-{most_recent_date.strftime('%Y-%m-%d')})"""

    await bot.send_message(chat_id=chat_id, text=message)

    #call the plot function
    plot_return_rates(range(1, 6), [returning_rates[f'return_within_{years}_years'] for years in range(1, 6)])
    plot_active_donor_counts(data)

def plot_return_rates(years, return_rates):
    plt.figure(figsize=(8, 6))
    plt.bar(years, return_rates, width=0.4, color='blue')
    plt.xlabel('Years Since First Donation')
    plt.ylabel('Return Rate')
    plt.xticks(years)
    plt.title('Retention Rates Over Time')
    #plt.show()

    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    plt.savefig(os.path.join(output_folder, '3-Retention Rate Over Time.png'))


def plot_active_donor_counts(data):
    donor_counts_per_year = data.groupby('donation_year')['donor_id'].nunique()

    plt.figure(figsize=(10, 6))
    bars = plt.bar(donor_counts_per_year.index.astype(str), donor_counts_per_year.values, color='skyblue')

    plt.grid(axis='y', linestyle='--', alpha=0.7)

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.05, int(yval), ha='center', va='bottom')

    plt.title('Count of Active Donors Per Year')
    plt.xlabel('Year')
    plt.ylabel('Count of Active Donors')
    plt.xticks(rotation=0)
    plt.tight_layout()
    
    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    plt.savefig(os.path.join(output_folder, '4-Count Of Active Donor.png'))

# Main function to orchestrate data analysis and reporting
async def main():
    folder_path = './data-darah-public'

    #load all CSV files in the folder into a dictionary
    datasets = {}
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            dataset_key = os.path.splitext(file_name)[0]
            datasets[dataset_key] = load_data(file_path)

    #initilize the dataset keys and year range
    newdonors_state = datasets['newdonors_state'] 
    donations_state = datasets['donations_state']  
    start_year = 2019
    end_year = 2024

    # Part 1 - Trends
    count_new_donors_by_year(newdonors_state, start_year, end_year)

    plot_blood_donation_trends(donations_state, start_year, end_year)

    await send_latest_donation_info(donations_state)
    
    # Part 2 - Retention rate
    parquet_file = './data-granular/ds-data-granular'
    await analyze_donor_data(parquet_file)

    send_all_images_in_folder('output')

if __name__ == "__main__":
    # Apply the event loop to run the main function asynchronously
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


    if os.path.exists('data_fetched.txt'):
        os.remove('data_fetched.txt')
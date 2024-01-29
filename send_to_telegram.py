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

#===========Telegram Bot configuration==============
bot_token = os.environ.get('BOT_TOKEN')
chat_id = os.environ.get('CHAT_ID')

# Initialize the Telegram Bot
bot = Bot(token=bot_token)

#===============Check for latest commit==============
#check if the data was fetched
if not os.path.exists('data_fetched.txt'):
    print("File not exists. Exiting script.")
    exit()
    
#open file, if the file not exists, quit.
with open('data_fetched.txt', 'r') as flag_file:
    if flag_file.read().strip() != 'Data fetched':
        print("Data not fetched. Exiting script.")
        exit()


#to load data from a CSV file
def load_data(file_path):
    data = pd.read_csv(file_path)
    return data

#to send all PNG files in a folder
async def send_all_images_in_folder(folder_path):
    png_files = glob.glob(os.path.join(folder_path, '*.png'))
    for png_file in png_files:
        await send_image(png_file)

#def send_image(image_path):
#    asyncio.get_event_loop().run_until_complete(send_image_async(image_path))

async def send_image_with_caption(image_path, caption):
    with open(image_path, 'rb') as image_file:
        await bot.send_photo(chat_id=chat_id, photo=image_file, caption=caption)

async def send_image_async(image_path):
    with open(image_path, 'rb') as image_file:
        await bot.send_photo(chat_id=chat_id, photo=image_file)

async def send_image(image_path):
    await send_image_async(image_path)


#====================Plot Visualisation=====================

async def send_latest_donation_info(data):
    try:
        data['date'] = pd.to_datetime(data['date'])
        malaysia_data = data[data['state'] == 'Malaysia']
        max_date = malaysia_data['date'].max()
        latest_data = malaysia_data[malaysia_data['date'] == max_date]
        total_donations_latest_date = latest_data['daily'].sum()
        current_year = datetime.now().year
        current_year_data = malaysia_data[malaysia_data['date'].dt.year == current_year]
        total_donations_current_year = current_year_data['daily'].sum()
        formatted_max_date = max_date.strftime('%Y-%m-%d')

        message = (
            f"TODAY'S UPDATE! 🩸🩸🩸\n"
            f"\n"
            f"Blood donations count today: +{total_donations_latest_date}\n"
            f"\n"
            f"Total blood donations {current_year}: {total_donations_current_year}\n"
            f"(data as of: {formatted_max_date})"
        )
        await bot.send_message(chat_id=chat_id, text=message)  
    except Exception as e:
        await bot.send_message(chat_id=chat_id, text=f"An error occurred while processing the data: {e}")  



#To count new donors by year and create a bar chart
def count_new_donors_by_year(data, start_year, end_year):
    # Use .copy() to explicitly work with a copy of the filtered DataFrame
    malaysia_data = data[data['state'] == 'Malaysia'].copy()
    
    malaysia_data['date'] = pd.to_datetime(malaysia_data['date'])
    latest_date = malaysia_data['date'].max()  # Find the max (latest) date in the data
    print(f"data as of: {latest_date.strftime('%Y-%m-%d')}") 

    malaysia_data.loc[:, 'total'] = pd.to_numeric(malaysia_data['total'], errors='coerce')
    malaysia_data.loc[:, 'year'] = malaysia_data['date'].dt.year

    #group by year & sum the total new donors for each year
    new_donors_by_year = malaysia_data[malaysia_data['year'].between(start_year, end_year)].groupby('year')['total'].sum()

    plt.figure(figsize=(9, 5)) 
    bars = plt.bar(new_donors_by_year.index, new_donors_by_year, color='blue', width=0.6)  
    plt.xlabel('Year')
    plt.ylabel('Total')
    plt.title(f'New Donors ({start_year} to {end_year})')
    plt.xticks(new_donors_by_year.index, rotation=0)

    #annotation
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, height, f'{int(height)}', ha='center', va='bottom')

    plt.tight_layout()    
    output_folder = 'output' 
    os.makedirs(output_folder, exist_ok=True)
    plt.savefig(os.path.join(output_folder, '1-New_Donors_Plot.png'))

    return new_donors_by_year


#to plot monthly blood donation trends, and create a line chart
def plot_blood_donation_trends(data, start_year, end_year):
    data['date'] = pd.to_datetime(data['date'])
    
    #filter data based on the certain years and 'state' must be from 'Malaysia'
    filtered_data = data[(data['date'].dt.year >= start_year) & (data['date'].dt.year <= end_year) & (data['state'] == 'Malaysia')]

    #group by month and then sum up the daily donations
    monthly_total_donations = filtered_data.groupby([filtered_data['date'].dt.to_period('M')])['daily'].sum().reset_index()
    monthly_total_donations['date'] = monthly_total_donations['date'].dt.to_timestamp()
    monthly_total_donations.rename(columns={'daily': 'total_donations'}, inplace=True)

    plt.figure(figsize=(15, 6))
    sns.lineplot(x='date', y='total_donations', data=monthly_total_donations, color='maroon', marker='o')
    plt.title(f'Trend of Total Monthly Blood Donations in Malaysia ({start_year} - {end_year})')
    plt.xlabel('Month and Year')
    plt.ylabel('Total Donations')

    #format x-axis to show 'Month Year'
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.xticks(rotation=90)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)

    #annotate, but only the most recent data point (bcs its too crowded if all is annotated)
    if not monthly_total_donations.empty:
        last_row = monthly_total_donations.iloc[-1]
        plt.text(last_row['date'], last_row['total_donations'], f"{last_row['total_donations']}", color='black', ha='center', va='bottom')

    plt.tight_layout()

    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)
    plt.savefig(os.path.join(output_folder, '2-Monthly_Donations_Trend.png'))

def plot_blood_donation_trends_by_state(data, start_year, end_year):
    data['date'] = pd.to_datetime(data['date'])
    data['year'] = data['date'].dt.year
    data['month'] = data['date'].dt.month

    filtered_data = data[(data['year'] >= start_year) & (data['year'] <= end_year) & (data['state'] != 'Malaysia')] #state != Malaysia
    yearly_donations = filtered_data.groupby(['state', 'year'])['daily'].sum().reset_index()

    #pivot the data to hv years as columns
    pivoted_data = yearly_donations.pivot(index='state', columns='year', values='daily').fillna(0)

    #sum of donations for each state
    total_donations_by_state = pivoted_data.sum(axis=1).sort_values(ascending=True)
    sorted_pivoted_data = pivoted_data.loc[total_donations_by_state.index]

    overall_total = total_donations_by_state.sum()

    plt.figure(figsize=(10, 10))
    ax = sorted_pivoted_data.plot(kind='barh', stacked=True)
    plt.title(f'Comparison of Total Blood Donations by State ({start_year}-{end_year})')
    plt.xlabel('Total Donations')
    plt.ylabel('State')

    ax.grid(axis='x', linestyle='--', alpha=0.7)
    #annotate to show count & %
    for idx, state in enumerate(sorted_pivoted_data.index):
        total_donations = sorted_pivoted_data.loc[state].sum()
        percentage = (total_donations/overall_total)*100
        plt.annotate(f'{total_donations:,.0f} ({percentage:.1f}%)', (total_donations + 500, idx), fontsize=10, va='center')

    #remove x-axis tick labels bcs crowded
    plt.xticks([])
    plt.legend(title='Year')
    
    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    plt.savefig(os.path.join(output_folder, '5-Donations_by_State.png'))

def plot_returning_new_donor_counts(data):
    data['visit_date'] = pd.to_datetime(data['visit_date'])
    data['donation_year'] = data['visit_date'].dt.year

    first_donation_year = data.groupby('donor_id')['donation_year'].min().reset_index()
    first_donation_year.rename(columns={'donation_year': 'first_donation_year'}, inplace=True)

    data_donor_status = pd.merge(data, first_donation_year, on='donor_id')

    #determine status; whether each donation was made by a 'New' or 'Returning' donor
    data_donor_status['donor_status'] = 'Returning'  # Assume 'Returning' by default
    #mark the first donation for each donor as 'New'
    data_donor_status.loc[data_donor_status['donation_year'] == data_donor_status['first_donation_year'], 'donor_status'] = 'New'

    #agg count unique donors per year by status; new or returning
    donor_counts_per_year = data_donor_status.groupby(['donation_year', 'donor_status'])['donor_id'].nunique().unstack(fill_value=0)

    plt.figure(figsize=(10, 6))

    #stacked bar chart
    bars_new = plt.bar(donor_counts_per_year.index.astype(str), donor_counts_per_year['New'], color='lightgreen', label='New')
    bars_returning = plt.bar(donor_counts_per_year.index.astype(str), donor_counts_per_year['Returning'], bottom=donor_counts_per_year['New'], color='skyblue', label='Returning')

    plt.grid(axis='y', linestyle='--', alpha=0.7)

    #annotate
    for bars in [bars_new, bars_returning]:
        for bar in bars:
            yval = bar.get_height()
            if yval > 0:  # Only annotate non-zero bars
                plt.text(bar.get_x() + bar.get_width() / 2, bar.get_y() + yval / 2, int(yval), ha='center', va='center')

    plt.title('Count of New-Donors & Returning-Donors Per Year')
    plt.xlabel('Year')
    plt.ylabel('Count of Donors')
    plt.xticks(rotation=0) 
    plt.legend()
    plt.tight_layout()

    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    plt.savefig(os.path.join(output_folder, '4-Count_new_returning_donor.png'))

def plot_donor_counts_by_age_and_year(data, start_year, end_year):
    data['visit_date'] = pd.to_datetime(data['visit_date'])
    data['donation_year'] = data['visit_date'].dt.year
    data['age_at_visit'] = data['donation_year'] - data['birth_date']

    bins = [17, 25, 30, 35, 40, 45, 50, 55]
    labels = ['17-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50-54']

    data['age_group'] = pd.cut(data['age_at_visit'], bins=bins, labels=labels, right=False)

    #filter for the years between start_year and end_year
    filtered_data = data[data['donation_year'].between(start_year, end_year)]
    #make sure count unique donor_id
    age_group_year_counts = filtered_data.groupby(['age_group', 'donation_year'], observed=True)['donor_id'].nunique().reset_index() 
    #observed=true bcs the default observe=false is deprecating (got FutureWarning)

    age_group_year_counts = age_group_year_counts[age_group_year_counts['age_group'].isin(labels)] #labels is only until age 50-54

    plt.figure(figsize=(10, 6))
    sns.barplot(x='age_group', y='donor_id', hue='donation_year', data=age_group_year_counts) #huee is the year
    plt.title(f'Count of Donors by Age Group and Year ({start_year}-{end_year})')
    plt.xlabel('Age Group')
    plt.ylabel('Count of Donors')
    plt.legend(title='Donation Year', bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()
    output_folder = 'output'
    os.makedirs(output_folder, exist_ok=True)

    plt.savefig(os.path.join(output_folder, '6-Donor_Count_Age_Year.png'))
    
#====================================MAIN===========================================
async def main():
    folder_path = './data-darah-public'
    
    #load all CSV files in the folder into a dictionary
    datasets = {}
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            dataset_key = os.path.splitext(file_name)[0]
            datasets[dataset_key] = load_data(file_path)

    #initialize the dataset keys
    newdonors_state = datasets['newdonors_state'] 
    donations_state = datasets['donations_state'] 

    #what year?
    start_year = 2019
    end_year = 2024

    # ====Part 1 - Trends====
    await send_latest_donation_info(donations_state)
    count_new_donors_by_year(newdonors_state, start_year, end_year) 
    plot_blood_donation_trends(donations_state, start_year, end_year)
    plot_blood_donation_trends_by_state(donations_state, start_year, end_year)

    await send_image_with_caption('output/1-New_Donors_Plot.png', "How many new donors this year?🥳") #caption
    await send_image_with_caption('output/2-Monthly_Donations_Trend.png', "Monthly Donation Trend!") #caption
    await send_image_with_caption('output/5-Donations_by_State.png', "Which state in Malaysia contributes most donation?") #caption 
    await send_image_with_caption('output/6-Donor_Count_Age_Year.png', "Which age group contributes most donation per Year? 😎") #caption
    
    # ====Part 2 - Retention rate====
    retention_data_path = './data-granular/ds-data-granular'
    retention_data = pd.read_parquet(retention_data_path)

    plot_returning_new_donor_counts(retention_data)
    plot_donor_counts_by_age_and_year(retention_data, start_year, end_year)

    await send_image_with_caption('output/4-Count_new_returning_donor.png', "Donor Retention: Does the previous donor come back? or we gain more Newbies each year?👶") #caption

    #===shortcut to send all images in folder=====
    #await send_all_images_in_folder('output')

if __name__ == "__main__":
    asyncio.run(main())

    if os.path.exists('data_fetched.txt'): 
        os.remove('data_fetched.txt')
import requests
import os
from datetime import datetime
from io import BytesIO # for parquet

REPO = "MoH-Malaysia/data-darah-public"  # GitHub repo
BRANCH = "main"  # Branch to monitor
CSV_FOLDER = "data-darah-public"  # Folder where CSV files will be saved
LAST_COMMIT_FILE = "last_commit.txt"  # File to store the SHA of the last processed commit

def fetch_last_commit_sha():
    """
    Fetch the SHA of the last processed commit from a file.
    """
    if os.path.exists(LAST_COMMIT_FILE):
        with open(LAST_COMMIT_FILE, "r") as file:
            return file.read().strip()
    return None

def update_last_commit_sha(commit_sha):
    """
    Update the SHA of the last processed commit in a file.
    """
    with open(LAST_COMMIT_FILE, "w") as file:
        file.write(commit_sha)

def fetch_latest_commit():
    """
    Fetch the latest commit from the GitHub repository.
    """
    url = f"https://api.github.com/repos/{REPO}/commits"
    params = {"sha": BRANCH, "per_page": 1}
    response = requests.get(url, params=params)
    return response.json()[0]  # Return the latest commit

def download_file(url, filename):
    """
    Download a file from the given URL.
    """
    response = requests.get(url)
    if response.status_code == 200:
        os.makedirs(CSV_FOLDER, exist_ok=True)
        filepath = os.path.join(CSV_FOLDER, filename)
        with open(filepath, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {filename}")

def process_latest_commit(commit):
    """
    Process the latest commit, download CSV files if the commit is new.
    """
    data_fetched = False  # Flag to indicate if new data was fetched
    last_commit_sha = fetch_last_commit_sha()
    current_commit_sha = commit['sha']

    # Check if the current commit is different from the last processed commit
    if last_commit_sha != current_commit_sha:
        commit_date = datetime.strptime(commit['commit']['author']['date'], "%Y-%m-%dT%H:%M:%SZ").date()
        today = datetime.now().date()

        # Process the commit only if it's from today
        if commit_date == today:
            print("Latest commit is from today! Fetching the data now..")
            commit_url = f"https://api.github.com/repos/{REPO}/commits/{current_commit_sha}"
            commit_response = requests.get(commit_url).json()
            files = commit_response.get("files", [])
            for file in files:
                if file['filename'].endswith(".csv"):
                    download_file(file['raw_url'], file['filename'])
                    data_fetched = True  # Set flag to True as new data is fetched
            update_last_commit_sha(current_commit_sha)
        else:
            print("No new commit for today.")
    else:
        print("Latest commit already processed. No new files to download.")

    return data_fetched


def fetch_parquet_data(url):

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Read the content of the response in bytes
        file_bytes = BytesIO(response.content)

        df = pd.read_parquet(file_bytes)
        
        os.makedirs('data-granular', exist_ok=True)

        print("Downloaded: data-granular")
        filepath = os.path.join('data-granular', 'ds-data-granular')
        
        df.to_parquet(filepath)

    else:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

def fetch_parquet_data(url,save_filename):
    # Send a GET request to the URL
    save_directory='data-granular'
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Create the save directory if it doesn't exist
        os.makedirs(save_directory, exist_ok=True)

        # Specify the full path to save the Parquet file
        filepath = os.path.join(save_directory, save_filename)

        # Save the Parquet data to the specified file
        with open(filepath, 'wb') as file:
            file.write(response.content)

        print(f"Downloaded: {save_filename}")
    else:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

def main():
    #fetch data-darah-public
    latest_commit = fetch_latest_commit()
    data_fetched = process_latest_commit(latest_commit)

    if data_fetched:
        with open('data_fetched.txt', 'w') as flag_file:
            flag_file.write('Data fetched')

    #fetch granular data
    url = 'https://dub.sh/ds-data-granular'
    save_filename='ds-data-granular'
    fetch_parquet_data(url,save_filename)

if __name__ == "__main__":
    main()

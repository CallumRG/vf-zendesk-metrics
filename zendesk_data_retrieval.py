import requests
import time
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

subdomain = os.getenv('ZENDESK_SUBDOMAIN')
email_address = os.getenv('ZENDESK_EMAIL')
api_token = os.getenv('ZENDESK_API_TOKEN')

# Base URL for the Zendesk API

start_time = 0  #begining
auth = (f'{email_address}/token', api_token)
url = f"https://{subdomain}.zendesk.com/api/v2/incremental/tickets.json?exclude_deleted=true&include=metric_sets,users&start_time={start_time}"

# Lists to hold the results
tickets, users, metric_sets = [], [], []


while url:
    print(f"Fetching data from URL: {url}")
    response = requests.get(url, auth=auth)
    
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
        continue
    
    elif response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        break

    data = response.json()

    # Add tickets, users, and metric sets to lists
    tickets.extend(data['tickets'])
    users.extend(data.get('users', []))
    metric_sets.extend(data.get('metric_sets', []))

    # Get the next URL and end_time for pagination
    new_url = data.get('next_page', None)

    # Break the loop if the next URL is stuck on the same start_time or if there is no new data
    if url == new_url or not new_url:
        print("No new data or stuck at the same timestamp. Exiting.")
        break

    url = new_url  # Update URL for the next page


# Convert lists to pandas DataFrames
tickets_df = pd.DataFrame(tickets)
tickets_df.drop(columns=['subject', 'raw_subject', 'description'], inplace=True)
tickets_df = tickets_df.sort_values(by='updated_at').drop_duplicates(subset='id', keep='last')

users_df = pd.DataFrame(users)
users_df = users_df.sort_values(by='updated_at').drop_duplicates(subset='id', keep='last')

metrics_df = pd.DataFrame(metric_sets)
metrics_df = metrics_df.sort_values(by='updated_at').drop_duplicates(subset='ticket_id', keep='last')

# Merge ticket metrics and user details into the tickets DataFrame
# Assuming 'id' is the key for tickets and 'assignee_id' matches 'id' in users
tickets_with_metrics = pd.merge(left = tickets_df, right = metrics_df, left_on='id', right_on='ticket_id', how='left')
final_df = pd.merge(left = tickets_with_metrics, right = users_df[['id', 'name']], left_on='assignee_id', right_on='id', how='left', suffixes=('', '_assignee'))

# Save the final combined data to a single CSV file
final_df.to_csv('zendesk_metric_data.csv', index=False)
#tickets_df.to_csv('tickets.csv', index=False)
#users_df.to_csv('users.csv', index=False)
#metrics_df.to_csv('metrics.csv', index=False)

print(f"Data saved to 'zendesk_tickets_with_sideloaded_data.csv'.")

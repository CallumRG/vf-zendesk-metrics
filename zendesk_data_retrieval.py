import requests
import time
import pandas as pd
from dotenv import load_dotenv
import os

def fetch_zendesk_data(start_time=0, save_csv=False, save_pickle=False, load_dotenv = False):
    # Load environment variables
    if load_dotenv:
        load_dotenv()

    subdomain = os.getenv('ZENDESK_SUBDOMAIN')
    email_address = os.getenv('ZENDESK_EMAIL')
    api_token = os.getenv('ZENDESK_API_TOKEN')

    auth = (f'{email_address}/token', api_token)
    url = f"https://{subdomain}.zendesk.com/api/v2/incremental/tickets.json?exclude_deleted=true&include=metric_sets,users&start_time={start_time}"

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

    # convert lists to pandas dfs
    tickets_df = pd.DataFrame(tickets)
    tickets_df.drop(columns=['subject', 'raw_subject', 'description'], inplace=True)
    tickets_df = tickets_df.sort_values(by='updated_at').drop_duplicates(subset='id', keep='last')

    users_df = pd.DataFrame(users)
    users_df = users_df.sort_values(by='updated_at').drop_duplicates(subset='id', keep='last')

    metrics_df = pd.DataFrame(metric_sets)
    metrics_df = metrics_df.sort_values(by='updated_at').drop_duplicates(subset='ticket_id', keep='last')

    # merge ticket metrics and user details into the tickets DataFrame
    tickets_with_metrics = pd.merge(left=tickets_df, right=metrics_df, left_on='id', right_on='ticket_id', how='left')
    final_df = pd.merge(left=tickets_with_metrics, right=users_df[['id', 'name']], left_on='assignee_id', right_on='id', how='left', suffixes=('', '_assignee'))
    
    if save_csv:
        final_df.to_csv('zendesk_metric_data.csv', index=False)
    
    if save_pickle:
        final_df.to_pickle('zendesk_metric_data.pkl')

    return final_df

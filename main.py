from zendesk_data_retrieval import fetch_zendesk_data
from metrics_process_email import send_support_metrics_email
import pandas as pd

def main():
    # Retrieve Zendesk data
    df = fetch_zendesk_data(save_pickle=False) 

    #pkl for test running (without needing to do api calls everytime)
    #df = pd.read_pickle('zendesk_metric_data.pkl')

    # Send support metrics email
    send_support_metrics_email(df)
    
    print("Data retrieval and email sending completed.")

if __name__ == "__main__":
    main()
import pandas as pd
from datetime import datetime, timedelta
import ast
from dotenv import load_dotenv
import os
import resend

def send_support_metrics_email(df):
    # Load environment variables
    load_dotenv()

    resend.api_key = os.getenv('RESEND_API_KEY')
    audience_id = os.getenv('RESEND_AUDIENCE_ID')
    email_from = os.getenv('EMAIL_FROM')

    # Retrieve audience contacts
    audience = resend.Contacts.list(audience_id=audience_id)

    #emails = [contact['email'] for contact in audience['data']]
    #emails = ['callum.gillies@voiceflow.com', 'tahsim.ahmed@voiceflow.com','braden@voiceflow.com']
    emails = ['callum.gillies@voiceflow.com']

    print(f'emails: {emails}')

    # Convert necessary columns to datetime and remove timezone information
    df['solved_at'] = pd.to_datetime(df['solved_at']).dt.tz_localize(None)

    # Calculate start and end dates
    """ today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    start = start_of_week - timedelta(weeks=1)
    end = start + timedelta(days=6) """

    end = datetime.now()
    start = end - timedelta(weeks=1)


    # Filter only tickets solved in the past 2 weeks or open/pending
    df = df[( (df['solved_at'] >= start) & (df['solved_at'] <= end))| (~df['status'].isin(['closed', 'solved']))]

    # Extract the 'score' field from 'satisfaction_rating' dictionary-like column
    df['satisfaction_score'] = df['satisfaction_rating'].apply(
        lambda x: x.get('score') if isinstance(x, dict) else (eval(x).get('score') if pd.notnull(x) and isinstance(x, str) else None)
    )

    # Function to extract the value for a specific field id from the custom_fields
    def get_custom_field_value(custom_fields_str, field_id):
        if pd.notnull(custom_fields_str):
            try:
                if isinstance(custom_fields_str, str):
                    custom_fields = ast.literal_eval(custom_fields_str)
                elif isinstance(custom_fields_str, list):
                    custom_fields = custom_fields_str
                else:
                    return None

                for field in custom_fields:
                    if field['id'] == field_id:
                        return field.get('value')
            except (ValueError, SyntaxError):
                return None
        return None

    # Apply the function to extract the specific field value for the given field_id
    df['customer_type'] = df['custom_fields'].apply(
        lambda x: get_custom_field_value(x, 5661060584461)
    )

    # Define business time values instead of calendar time for relevant metrics
    df['reply_time_in_minutes'] = df['reply_time_in_minutes'].apply(
        lambda x: x.get('business') if isinstance(x, dict) else (eval(x).get('business') if pd.notnull(x) and isinstance(x, str) else None)
    )
    df['first_resolution_time_in_minutes'] = df['first_resolution_time_in_minutes'].apply(
        lambda x: x.get('business') if isinstance(x, dict) else (eval(x).get('business') if pd.notnull(x) and isinstance(x, str) else None)
    )
    df['full_resolution_time_in_minutes'] = df['full_resolution_time_in_minutes'].apply(
        lambda x: x.get('business') if isinstance(x, dict) else (eval(x).get('business') if pd.notnull(x) and isinstance(x, str) else None)
    )
    df['agent_wait_time_in_minutes'] = df['agent_wait_time_in_minutes'].apply(
        lambda x: x.get('business') if isinstance(x, dict) else (eval(x).get('business') if pd.notnull(x) and isinstance(x, str) else None)
    )
    df['requester_wait_time_in_minutes'] = df['requester_wait_time_in_minutes'].apply(
        lambda x: x.get('business') if isinstance(x, dict) else (eval(x).get('business') if pd.notnull(x) and isinstance(x, str) else None)
    )

    # Convert time metrics to hours
    df['reply_time_in_hours'] = df['reply_time_in_minutes'] / 60
    df['first_resolution_time_in_hours'] = df['first_resolution_time_in_minutes'] / 60
    df['full_resolution_time_in_hours'] = df['full_resolution_time_in_minutes'] / 60
    df['agent_wait_time_in_hours'] = df['agent_wait_time_in_minutes'] / 60
    df['requester_wait_time_in_hours'] = df['requester_wait_time_in_minutes'] / 60

    # Check if the assignee is 'Tico | Voiceflow Assistant' and group all others into 'Support Team'
    df['assignee_group'] = df['name'].apply(lambda x: 'Tico | Voiceflow Assistant' if x == 'Tico | Voiceflow Assistant' else 'Support Team')

    # Only consider 'closed' or 'solved' tickets
    df_completed_tickets = df[df['status'].isin(['closed', 'solved'])]

    #df_support_team = df[df['assignee_group'] == 'Support Team']
    df_support_team = df

    df_support_team_completed = df_support_team[df_support_team['status'].isin(['closed', 'solved'])]

    # Group by 'assignee_group' and 'name' with aggregation functions
    by_rep = df_completed_tickets.groupby(['name']).agg(
        solved_tickets=('id_x', 'count'),
        enterprise_tickets=('customer_type', lambda x: (x == 'enterprise').sum()),
        teams_tickets=('customer_type', lambda x: (x == 'teams').sum()),
        good_ratings=('satisfaction_score', lambda x: (x == 'good').sum()),
        bad_ratings=('satisfaction_score', lambda x: (x == 'bad').sum()),
        satisfaction_percentage=('satisfaction_score', lambda x: (x == 'good').sum() / ((x == 'good').sum() + (x == 'bad').sum()) * 100),
        percentage_tickets_rated=('satisfaction_score', lambda x: ((x == 'good').sum() + (x == 'bad').sum()) / ((x == 'good').sum() + (x == 'bad').sum() + (x == 'offered').sum()) * 100),
        one_touch_percentage=('replies', lambda x: (x == 1).mean() * 100),
        avg_first_reply_time=('reply_time_in_hours', 'mean'),
        avg_requester_wait_time=('requester_wait_time_in_hours', 'mean'),
        avg_last_assignment_to_resolution_time=('agent_wait_time_in_hours', 'mean'),
        avg_full_resolution_time=('full_resolution_time_in_hours', 'mean')
        
    ).reset_index()

    by_group = df_support_team.groupby(['assignee_group']).agg(
        solved_tickets=('status', lambda x: ((x == 'solved') | (x == 'closed')).sum()),
        backlog_tickets=('status', lambda x: ((x == 'open') | (x == 'pending')).sum()),
        open_tickets=('status', lambda x: (x == 'open').sum()),
        pending_tickets=('status', lambda x: (x == 'pending').sum())
    ).reset_index()

    by_group = pd.merge(by_group, df_support_team_completed.groupby(['assignee_group']).agg(
        enterprise_tickets=('customer_type', lambda x: (x == 'enterprise').sum()),
        teams_tickets=('customer_type', lambda x: (x == 'teams').sum()),
        good_ratings=('satisfaction_score', lambda x: (x == 'good').sum()),
        bad_ratings=('satisfaction_score', lambda x: (x == 'bad').sum()),
        satisfaction_percentage=('satisfaction_score', lambda x: (x == 'good').sum() / ((x == 'good').sum() + (x == 'bad').sum()) * 100),
        percentage_tickets_rated=('satisfaction_score', lambda x: ((x == 'good').sum() + (x == 'bad').sum()) / ((x == 'good').sum() + (x == 'bad').sum() + (x == 'offered').sum()) * 100),
        one_touch_percentage=('replies', lambda x: (x == 1).mean() * 100),
        avg_first_reply_time=('reply_time_in_hours', 'mean'),
        avg_requester_wait_time=('requester_wait_time_in_hours', 'mean'),
        avg_last_assignment_to_resolution_time=('agent_wait_time_in_hours', 'mean'),
        avg_full_resolution_time=('full_resolution_time_in_hours', 'mean')
    ).reset_index(), on='assignee_group', how='left')

    by_group.columns = ['Team', 'Solved Tickets', 'Backlog Tickets', 'Open Tickets', 'Pending Tickets', 
                        'Enterprise Tickets', 'Teams Tickets', 'Good Ratings', 'Bad Ratings', 
                        'Satisfaction %', '% of Tickets Rated', '% of One-Touch Tickets', 
                        'Avg First Reply Time (hrs)', 'Avg Requester Wait Time (hrs)', 
                        'Avg Last Assignment to Resolution (hrs)', 'Avg Full Resolution Time (hrs)']

    by_rep.columns = ['Agent Name', 'Solved Tickets', 'Enterprise Tickets', 'Teams Tickets', 'Good Ratings', 
                    'Bad Ratings', 'Satisfaction %', '% of Tickets Rated', '% of One-Touch Tickets', 
                    'Avg First Reply Time (hrs)', 'Avg Requester Wait Time (hrs)', 
                    'Avg Last Assignment to Resolution (hrs)', 'Avg Full Resolution Time (hrs)']

    # Function to format percentages and round decimals
    def format_percentage(val):
        if pd.isna(val):
            return "Not Rated"
        return f"{val:.2f}%"

    def round_to_two_decimals(val):
        if pd.isna(val):
            return "Not Rated"
        return round(val, 2)

    # Apply formatting to the percentage columns in by_rep and by_group
    by_rep['Satisfaction %'] = by_rep['Satisfaction %'].apply(format_percentage)
    by_rep['% of Tickets Rated'] = by_rep['% of Tickets Rated'].apply(format_percentage)
    by_rep['% of One-Touch Tickets'] = by_rep['% of One-Touch Tickets'].apply(format_percentage)

    by_group['Satisfaction %'] = by_group['Satisfaction %'].apply(format_percentage)
    by_group['% of Tickets Rated'] = by_group['% of Tickets Rated'].apply(format_percentage)
    by_group['% of One-Touch Tickets'] = by_group['% of One-Touch Tickets'].apply(format_percentage)

    # Round decimal values to 2 decimal places for the relevant columns in by_rep and by_group
    decimal_columns_rep = ['Avg First Reply Time (hrs)', 'Avg Requester Wait Time (hrs)', 
                        'Avg Last Assignment to Resolution (hrs)', 'Avg Full Resolution Time (hrs)']
    decimal_columns_group = ['Avg First Reply Time (hrs)', 'Avg Requester Wait Time (hrs)', 
                            'Avg Last Assignment to Resolution (hrs)', 'Avg Full Resolution Time (hrs)']

    # Apply rounding to the decimal columns in by_rep
    for col in decimal_columns_rep:
        by_rep[col] = by_rep[col].apply(round_to_two_decimals)

    # Apply rounding to the decimal columns in by_group
    for col in decimal_columns_group:
        by_group[col] = by_group[col].apply(round_to_two_decimals)

    # Remove the specified columns from the final tables
    columns_to_remove = ['Avg Requester Wait Time (hrs)', 'Avg Last Assignment to Resolution (hrs)', 'Avg Full Resolution Time (hrs)']

    by_rep = by_rep.drop(columns=columns_to_remove)
    by_group = by_group.drop(columns=columns_to_remove)

    # Convert dataframes to HTML
    by_rep_html = by_rep.to_html(index=False, classes='table table-striped', border=0)
    by_group_html = by_group.to_html(index=False, classes='table table-striped', border=0)

    # Prepare the email HTML content
    html_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                color: #333;
                line-height: 1.6;
                margin: 0;
                padding: 0;
                background-color: #f4f4f4;
            }}
            .container {{
                width: 80%;
                margin: auto;
                padding: 20px;
                background-color: #ffffff;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            }}
            h1 {{
                color: #0066cc;
                font-size: 28px;
                margin-bottom: 10px;
            }}
            h2 {{
                color: #333;
                font-size: 22px;
                border-bottom: 3px solid #0066cc;
                padding-bottom: 8px;
                margin-bottom: 20px;
            }}
            p {{
                font-size: 16px;
                color: #666;
            }}
            .table {{
                width: 100%;
                max-width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                background-color: #fff;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                table-layout: auto;
                word-wrap: break-word;
            }}
            .table td, .table th {{
                border: 1px solid #ddd;
                padding: 6px; /* Reduced padding */
                text-align: left;
                font-size: 12px; /* Reduced font size */
            }}
            .table th {{
                background-color: #0066cc;
                color: #fff;
                font-weight: bold;
            }}
            .table tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            .table tr:hover {{
                background-color: #f1f1f1;
            }}
            .footer {{
                text-align: center;
                padding: 20px;
                font-size: 14px;
                color: #999;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Weekly Support Team Metrics</h1>
            <p>Dear Team,</p>
            <p>Here is a summary of the support team metrics for the week of {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}. Please review the detailed breakdown below:</p>
            <h2>Metric By Assignee Group (Overall)</h2>
            {by_group_html}
            <h2>Metrics By Ticket Assignee</h2>
            {by_rep_html}
            <div class="footer">
                <p>If you have any questions or need further details, feel free to reach out to us.</p>
                <p>Best regards,<br>Customer Support, Voiceflow Inc.</p>
            </div>
        </div>
    </body>
    </html>
    """

    # For now, print the HTML content for preview
    params: resend.Emails.SendParams = {
        "from": email_from,
        "to": emails,
        "subject": f'Support Team Metrics ({start.strftime('%Y-%m-%d')} - {end.strftime('%Y-%m-%d')})',
        "html": html_content
    }

    email = resend.Emails.send(params)

    print(f'Email sent: {email}')

    """ with open("zendesk_report_preview.html", "w") as file:
        file.write(html_content) """

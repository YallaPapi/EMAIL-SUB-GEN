import pandas as pd
import random

df = pd.read_csv('output/funding_leads_with_website_url_head200_with_emails_20250925_175732.csv')

# Get 5 random indices
indices = random.sample(range(len(df)), 5)

for i, idx in enumerate(indices, 1):
    row = df.iloc[idx]
    print(f'\n{"="*80}')
    print(f'EMAIL #{i} - Row {idx+1}')
    print(f'Name: {row["firstName"]} from {row.get("organization_name", "Unknown")}')
    print(f'{"="*80}\n')
    print(f'SUBJECT LINE:')
    print(f'{row["subject"]}\n')
    print(f'EMAIL BODY:')
    print(row["emailBody"])
    print()
import pandas as pd

df = pd.read_csv('output/funding_leads_with_website_url_head200_with_emails_20250925_175732.csv')

print('SAMPLE OF GENERATED EMAILS:')
print('=' * 100)

for i in range(min(5, len(df))):
    print(f'\nROW {i+1}:')
    print(f'Name: {df.iloc[i]["firstName"]}')
    print(f'Company: {df.iloc[i].get("organization_name", "N/A")}')
    print(f'Subject: {df.iloc[i]["subject"]}')
    print(f'Email Body Preview:')
    print(df.iloc[i]["emailBody"][:400] + '...')
    print('-' * 100)
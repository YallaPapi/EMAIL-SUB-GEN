import pandas as pd

df = pd.read_csv('output/funding_leads_with_website_url_head200_with_emails_20250925_175732.csv', encoding='utf-8')

# Get rows 10, 25, 50, 100, 150
rows = [10, 25, 50, 100, 150]

for row_num in rows:
    row = df.iloc[row_num-1]
    print('='*80)
    print(f'EMAIL FROM ROW {row_num}')
    print(f'Name: {row["firstName"]} from {row.get("organization_name", "?")}')
    print('='*80)
    print()
    print(f'SUBJECT: {row["subject"]}')
    print()
    print('EMAIL:')
    # Clean up unicode characters for display
    email = row["emailBody"]
    email = email.replace('\u2011', '-').replace('\u2019', "'").replace('\u201c', '"')
    email = email.replace('\u201d', '"').replace('\u2013', '-').replace('\u2014', '-')
    print(email)
    print()
    print()
import pandas as pd
import random
import re

# Load the generated CSV
df = pd.read_csv('output/funding_leads_with_apollo_websites_cleaned_with_emails_20250927_095832.csv')

# Get 100 random indices
random.seed(42)
indices = random.sample(range(len(df)), min(100, len(df)))

# Check each entry
correct_count = 0
name_mismatch = []
company_mismatch = []
no_content = []

for idx in indices:
    row = df.iloc[idx]
    first_name = str(row.get('firstName', ''))
    org_name = str(row.get('organization_name', ''))
    email_body = str(row.get('emailBody', ''))
    subject = str(row.get('subject', ''))
    website = str(row.get('organization_website_url', ''))

    # Skip if no data
    if not email_body or email_body == 'nan' or not subject or subject == 'nan':
        no_content.append({
            'row': idx + 1,
            'firstName': first_name,
            'org_name': org_name
        })
        continue

    # Derive short name from website
    short_name = None
    if website and website != 'nan':
        domain_match = re.search(r'https?://(?:www\.|m\.)?([^/]+)', website)
        if domain_match:
            domain = domain_match.group(1)
            parts = domain.split('.')
            if len(parts) >= 2:
                short_name = parts[-2]
            else:
                short_name = parts[0]
            # Clean up
            short_name = re.sub(r'\b(incorporated|inc|llc|ltd|limited|insurance|agency|corp|corporation|group|co|company|holdings)\b', '', short_name, flags=re.IGNORECASE)
            short_name = re.sub(r'[^A-Za-z0-9]+', ' ', short_name).strip().title()

    # Check if name is in email (first few words to avoid false positives)
    name_found = False
    if first_name and first_name != 'nan':
        if first_name.lower() in email_body.lower()[:200]:  # Check in greeting area
            name_found = True

    # Check if company is mentioned
    company_found = False
    org_variations = [
        org_name,
        short_name,
        org_name.split()[0] if org_name and org_name != 'nan' else '',
    ]

    for variation in org_variations:
        if variation and len(variation) > 2 and variation != 'nan':
            if variation.lower() in email_body.lower():
                company_found = True
                break

    if name_found and company_found:
        correct_count += 1
    else:
        if not name_found:
            name_mismatch.append({
                'row': idx + 1,
                'firstName': first_name,
                'org_name': org_name,
                'email_start': email_body[:150] if email_body else ''
            })
        if not company_found:
            company_mismatch.append({
                'row': idx + 1,
                'firstName': first_name,
                'org_name': org_name,
                'short_name': short_name,
                'subject': subject[:100] if subject else '',
                'email_snippet': email_body[:200] if email_body else ''
            })

print(f"QUALITY CHECK OF 100 RANDOM ENTRIES")
print(f"=" * 80)
print(f"Total checked: {len(indices)}")
print(f"Correct (name + company): {correct_count}")
print(f"Name mismatches: {len(name_mismatch)}")
print(f"Company mismatches: {len(company_mismatch)}")
print(f"No content: {len(no_content)}")
print(f"Accuracy rate: {correct_count}/{len(indices) - len(no_content)} = {correct_count/(len(indices) - len(no_content))*100:.1f}%")

if name_mismatch:
    print(f"\nNAME MISMATCHES (first 5):")
    print(f"-" * 80)
    for i, m in enumerate(name_mismatch[:5], 1):
        print(f"{i}. Row {m['row']} - Expected: {m['firstName']}")
        print(f"   Email start: {m['email_start']}")
        print()

if company_mismatch:
    print(f"\nCOMPANY MISMATCHES (first 5):")
    print(f"-" * 80)
    for i, m in enumerate(company_mismatch[:5], 1):
        print(f"{i}. Row {m['row']} - {m['firstName']} from {m['org_name']}")
        print(f"   Short name: {m['short_name']}")
        print(f"   Subject: {m['subject']}")
        print(f"   Email: {m['email_snippet']}")
        print()
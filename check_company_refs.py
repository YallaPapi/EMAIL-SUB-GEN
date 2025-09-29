import pandas as pd
import random
import re

# Load the generated CSV
df = pd.read_csv('output/funding_leads_with_website_url_with_emails_20250925_200906.csv')

# Get 20 random indices for detailed analysis
random.seed(42)
indices = random.sample(range(len(df)), min(20, len(df)))

print("DETAILED ANALYSIS OF 20 RANDOM ENTRIES")
print("=" * 100)

for i, idx in enumerate(indices, 1):
    row = df.iloc[idx]
    org_name = str(row.get('organization_name', ''))
    email_body = str(row.get('emailBody', ''))
    first_name = str(row.get('firstName', ''))
    website = str(row.get('organization_website_url', ''))

    # Skip if no data
    if org_name == 'nan' or email_body == 'nan':
        continue

    print(f"\n{i}. Row {idx + 1} - {first_name}")
    print(f"   Organization: {org_name}")
    print(f"   Website: {website}")

    # Extract short name from website
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

    print(f"   Expected short name: {short_name}")
    print(f"   Email snippet: {email_body[:200]}...")

    # Check what company names appear in the email
    import re
    # Look for capitalized words that might be company names
    potential_companies = re.findall(r'\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\b', email_body[:500])
    # Filter out common words
    common_words = {'Hey', 'I', 'We', 'Our', 'The', 'This', 'That', 'For', 'With', 'Would', 'If', 'LinkedIn', 'Loom'}
    companies_mentioned = [c for c in potential_companies if c not in common_words and len(c) > 2]

    print(f"   Companies mentioned in email: {companies_mentioned[:5]}")

    # Check if correct company is mentioned
    found = False
    if org_name in email_body:
        print(f"   [CHECK] Found exact org name")
        found = True
    elif short_name and short_name in email_body:
        print(f"   [CHECK] Found short name: {short_name}")
        found = True
    else:
        # Check first word of org name
        first_word = org_name.split()[0] if org_name else ''
        if first_word and len(first_word) > 2 and first_word in email_body:
            print(f"   [CHECK] Found first word: {first_word}")
            found = True
        else:
            print(f"   [FAIL] Company NOT found in email!")

    print("-" * 100)
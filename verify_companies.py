import pandas as pd
import random
import re

# Load the generated CSV
df = pd.read_csv('output/funding_leads_with_website_url_with_emails_20250925_200906.csv')

# Get 100 random indices
random.seed(42)  # For reproducibility
indices = random.sample(range(len(df)), min(100, len(df)))

# Check each entry
mismatches = []
correct = []

for idx in indices:
    row = df.iloc[idx]
    org_name = str(row.get('organization_name', ''))
    email_body = str(row.get('emailBody', ''))
    first_name = str(row.get('firstName', ''))

    # Skip if no org name or email body or NaN
    if not org_name or not email_body or org_name == 'nan' or email_body == 'nan':
        continue

    # Extract the short name that should be in the email
    # The generator uses derive_short_name logic
    website = row.get('organization_website_url', '')
    if website:
        # Extract domain and derive short name
        domain_match = re.search(r'https?://(?:www\.|m\.)?([^/]+)', website)
        if domain_match:
            domain = domain_match.group(1)
            parts = domain.split('.')
            if len(parts) >= 2:
                short_name = parts[-2]
            else:
                short_name = parts[0]
            # Clean up the short name
            short_name = re.sub(r'\b(incorporated|inc|llc|ltd|limited|insurance|agency|corp|corporation|group|co|company|holdings)\b', '', short_name, flags=re.IGNORECASE)
            short_name = re.sub(r'[^A-Za-z0-9]+', ' ', short_name).strip().title()
    else:
        short_name = org_name

    # Check if the email mentions the company
    # Look for various forms of the company name
    org_variations = [
        org_name,
        org_name.replace(',', '').replace('.', ''),
        short_name,
        org_name.split()[0] if org_name else '',  # First word of org name
        org_name.replace(' ', '')  # No spaces version
    ]

    found = False
    found_name = None
    for variation in org_variations:
        if variation and len(variation) > 2:  # Skip very short strings
            if variation.lower() in email_body.lower():
                found = True
                found_name = variation
                break

    if not found:
        # Check if it mentions "LinkedIn" which might be a generic mistake
        if 'linkedin' in email_body.lower() and 'linkedin' not in org_name.lower():
            mismatches.append({
                'row': idx + 1,
                'firstName': first_name,
                'org_name': org_name,
                'short_name': short_name,
                'email_snippet': email_body[:200] + '...'
            })
    else:
        correct.append({
            'row': idx + 1,
            'org_name': org_name,
            'found_as': found_name
        })

print(f"Checked {len(indices)} random entries")
print(f"Correct company references: {len(correct)}")
print(f"Potential mismatches: {len(mismatches)}")
print()

if mismatches:
    print("ENTRIES WITH POTENTIAL COMPANY MISMATCHES:")
    print("=" * 80)
    for i, mismatch in enumerate(mismatches[:10], 1):  # Show first 10
        print(f"\n{i}. Row {mismatch['row']} - {mismatch['firstName']}")
        print(f"   Expected company: {mismatch['org_name']} (or {mismatch['short_name']})")
        print(f"   Email body: {mismatch['email_snippet']}")
        print("-" * 80)

    if len(mismatches) > 10:
        print(f"\n... and {len(mismatches) - 10} more mismatches")

print(f"\nAccuracy rate: {len(correct)}/{len(indices)} = {len(correct)/len(indices)*100:.1f}%")
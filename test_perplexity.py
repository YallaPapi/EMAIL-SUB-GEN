import pandas as pd
import os
from dotenv import load_dotenv
from src.research import fetch_hooks
import json

load_dotenv()

# Load the CSV with real company websites
df = pd.read_csv('funding_leads_with_apollo_websites_cleaned.csv')

# Get first 5 companies with real websites
test_companies = []
for i in range(min(5, len(df))):
    row = df.iloc[i]
    if pd.notna(row.get('organization_website_url')) and not 'linkedin.com' in str(row.get('organization_website_url', '')):
        test_companies.append({
            'organization_website_url': row.get('organization_website_url'),
            'organization_name': row.get('organization_name'),
            'industry': row.get('industry', ''),
            'city': row.get('city', ''),
            'linkedin_url': row.get('linkedin_url', ''),
            'shortName': row.get('organization_name', '').split()[0] if pd.notna(row.get('organization_name')) else ''
        })
        if len(test_companies) >= 5:
            break

# Test Perplexity API with each company
api_key = os.getenv('PERPLEXITY_API_KEY')
model = 'sonar-pro'

print("Testing Perplexity API responses:")
print("=" * 80)

for i, company in enumerate(test_companies, 1):
    print(f"\n{i}. Testing: {company['organization_name']}")
    print(f"   Website: {company['organization_website_url']}")
    print(f"   Industry: {company['industry']}")

    try:
        hooks = fetch_hooks(api_key, model, company)
        print(f"   Hooks returned: {len(hooks)}")
        if hooks:
            for j, hook in enumerate(hooks, 1):
                print(f"      {j}. {hook}")
        else:
            print("   ERROR: No hooks returned!")
    except Exception as e:
        print(f"   ERROR: {str(e)}")

    print("-" * 80)

print("\nSUMMARY: If Perplexity is not returning hooks, it may be due to:")
print("1. Rate limiting on the API")
print("2. The model not finding recent news for these companies")
print("3. API configuration issues with OpenRouter")
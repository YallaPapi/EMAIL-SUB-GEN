#!/usr/bin/env python3
"""
Post-process CSV files to add paragraph breaks to email bodies.
"""

import pandas as pd
import re
import sys
import argparse

def format_email_with_paragraphs(email_body):
    """Add paragraph breaks to email body based on content patterns."""
    if not email_body or pd.isna(email_body):
        return email_body

    formatted = str(email_body)

    # For funding emails
    formatted = formatted.replace(". I wanted to reach out", ".\n\nI wanted to reach out")
    formatted = formatted.replace(". I wanted to run", ".\n\nI wanted to run")

    # Service description paragraph
    formatted = formatted.replace(". We work with", ".\n\nWe work with")
    formatted = formatted.replace(". We run a", ".\n\nWe run a")

    # Company-specific benefit paragraph - use regex for any company name
    formatted = re.sub(r'\. (For \w+,)', r'.\n\n\1', formatted)

    # Call to action paragraph
    formatted = formatted.replace(". Would you be", ".\n\nWould you be")
    formatted = formatted.replace(". Want a quick", ".\n\nWant a quick")
    formatted = formatted.replace(". Would it be", ".\n\nWould it be")
    formatted = formatted.replace(". Want to see", ".\n\nWant to see")
    formatted = formatted.replace(". Would you like", ".\n\nWould you like")

    return formatted.strip()

def process_csv(input_file, output_file=None):
    """Process CSV file to add paragraph breaks to email bodies."""

    # Read CSV
    df = pd.read_csv(input_file)

    # Check if emailBody column exists
    if 'emailBody' not in df.columns:
        print(f"Warning: 'emailBody' column not found in {input_file}")
        return

    # Apply formatting to emailBody column
    df['emailBody'] = df['emailBody'].apply(format_email_with_paragraphs)

    # Determine output file
    if output_file is None:
        # Default: add _formatted before extension
        base = input_file.rsplit('.', 1)[0]
        output_file = f"{base}_formatted.csv"

    # Save formatted CSV
    df.to_csv(output_file, index=False)
    print(f"Formatted emails saved to: {output_file}")

    # Show sample of formatting
    print("\nSample formatted email:")
    print("-" * 50)
    if not df.empty and not pd.isna(df.iloc[0]['emailBody']):
        print(df.iloc[0]['emailBody'])
    print("-" * 50)

def main():
    parser = argparse.ArgumentParser(description='Add paragraph breaks to generated emails')
    parser.add_argument('input', help='Input CSV file with generated emails')
    parser.add_argument('-o', '--output', help='Output CSV file (default: input_formatted.csv)')

    args = parser.parse_args()

    process_csv(args.input, args.output)

if __name__ == "__main__":
    main()
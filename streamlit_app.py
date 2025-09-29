import streamlit as st
import pandas as pd
import os
import tempfile
from datetime import datetime
import json
from io import StringIO

# Import your existing modules
from src.pipeline import process_csv as run_pipeline
from src.config import Config
from format_emails import format_email_with_paragraphs

# Page config
st.set_page_config(
    page_title="Email Generator Pro",
    page_icon="📧",
    layout="wide"
)

# Initialize session state
if 'generated_emails' not in st.session_state:
    st.session_state.generated_emails = None
if 'api_keys' not in st.session_state:
    st.session_state.api_keys = {}

def load_prompt_templates():
    """Load existing prompt templates"""
    templates = {
        "Database Reactivation": open("updatedprompt.txt", "r").read() if os.path.exists("updatedprompt.txt") else "",
        "MCA Funding": open("fundingprompt.txt", "r").read() if os.path.exists("fundingprompt.txt") else "",
        "Custom": ""
    }
    return templates

def save_custom_prompt(name, content):
    """Save custom prompt to file"""
    filename = f"custom_prompts/{name.lower().replace(' ', '_')}.txt"
    os.makedirs("custom_prompts", exist_ok=True)
    with open(filename, "w") as f:
        f.write(content)
    return filename

def generate_emails(df, prompt_content, config):
    """Generate emails using the pipeline"""
    # Save prompt to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(prompt_content)
        prompt_file = f.name

    # Save CSV to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.to_csv(f, index=False)
        input_file = f.name

    # Output file
    output_file = tempfile.mktemp(suffix='.csv')

    try:
        # Run pipeline
        run_pipeline(
            input_csv=input_file,
            output_csv=output_file,
            prompt_file=prompt_file,
            config=config
        )

        # Read results
        result_df = pd.read_csv(output_file)

        # Apply formatting to email bodies
        if 'emailBody' in result_df.columns:
            result_df['emailBody'] = result_df['emailBody'].apply(format_email_with_paragraphs)

        return result_df

    finally:
        # Cleanup temp files
        for f in [prompt_file, input_file, output_file]:
            if os.path.exists(f):
                os.remove(f)

def main():
    st.title("📧 Email Generator Pro")
    st.markdown("Generate personalized cold emails at scale for any offer")

    # Sidebar for API keys and settings
    with st.sidebar:
        st.header("⚙️ Settings")

        st.subheader("API Keys")
        openai_key = st.text_input(
            "OpenAI API Key",
            type="password",
            value=st.session_state.api_keys.get('openai', ''),
            help="Your OpenAI API key for GPT-5-mini"
        )

        perplexity_key = st.text_input(
            "Perplexity API Key",
            type="password",
            value=st.session_state.api_keys.get('perplexity', ''),
            help="Your Perplexity API key for research"
        )

        if openai_key:
            st.session_state.api_keys['openai'] = openai_key
            os.environ['OPENAI_API_KEY'] = openai_key

        if perplexity_key:
            st.session_state.api_keys['perplexity'] = perplexity_key
            os.environ['PERPLEXITY_API_KEY'] = perplexity_key

        st.subheader("Processing Settings")
        concurrency = st.slider("Concurrent Workers", 1, 20, 4)
        os.environ['CONCURRENCY'] = str(concurrency)
        os.environ['PPLX_MAX_CONC'] = str(concurrency)

        model = st.selectbox(
            "Model",
            ["gpt-5-mini", "gpt-4o-mini", "gpt-3.5-turbo"],
            index=0
        )

    # Main content area
    tabs = st.tabs(["📝 Generate Emails", "📚 Prompt Library", "📊 Results"])

    with tabs[0]:
        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("1️⃣ Upload Your Data")
            uploaded_file = st.file_uploader(
                "Choose a CSV file",
                type="csv",
                help="Must contain: first_name, email, organization_name, and other prospect data"
            )

            if uploaded_file:
                df = pd.read_csv(uploaded_file)
                st.success(f"✅ Loaded {len(df)} rows")

                # Show preview
                with st.expander("Preview Data"):
                    st.dataframe(df.head())
                    required_cols = ['first_name', 'email', 'organization_name']
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    if missing_cols:
                        st.error(f"Missing required columns: {missing_cols}")

        with col2:
            st.subheader("2️⃣ Choose Email Template")

            templates = load_prompt_templates()
            template_choice = st.selectbox(
                "Select Template",
                list(templates.keys()) + ["Create New"]
            )

            if template_choice == "Create New":
                st.info("💡 Create a custom email template for any offer")

                # Template builder
                offer_type = st.text_input("What's your offer?", placeholder="e.g., SEO Services, Consulting, Software Demo")

                if offer_type:
                    # Generate a basic template structure
                    suggested_template = f"""Write a cold email and subject line for {offer_type}.

Rules:
- Subject format: "{{{{firstName}}}} + Specific Hook + Context"
- Mention company by name (shortened naturally)
- Start with personalized opener using research data
- Explain why you're reaching out
- Present your {offer_type} value proposition
- Include specific benefit for their company
- End with soft call-to-action
- Keep it 80-120 words
- Use the research data provided below

Research data will be provided for each prospect."""

                    prompt_content = st.text_area(
                        "Edit Your Prompt Template",
                        value=suggested_template,
                        height=400,
                        help="Use {{firstName}} and {{organization_name}} as variables"
                    )

                    # Save template option
                    if st.button("💾 Save Template"):
                        template_name = st.text_input("Template Name", value=offer_type)
                        if template_name:
                            save_custom_prompt(template_name, prompt_content)
                            st.success(f"Saved template: {template_name}")
            else:
                prompt_content = st.text_area(
                    "Edit Prompt",
                    value=templates.get(template_choice, ""),
                    height=400
                )

        st.markdown("---")

        # Generate button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🚀 Generate Emails", type="primary", use_container_width=True):
                if not uploaded_file:
                    st.error("Please upload a CSV file")
                elif not st.session_state.api_keys.get('openai'):
                    st.error("Please enter your OpenAI API key")
                elif not st.session_state.api_keys.get('perplexity'):
                    st.error("Please enter your Perplexity API key")
                elif not prompt_content:
                    st.error("Please select or create a prompt template")
                else:
                    with st.spinner("Generating emails... This may take a few minutes."):
                        try:
                            # Create config
                            config = Config()
                            config.openai_api_key = st.session_state.api_keys['openai']
                            config.perplexity_api_key = st.session_state.api_keys['perplexity']
                            config.model = model

                            # Generate emails
                            result_df = generate_emails(df, prompt_content, config)
                            st.session_state.generated_emails = result_df
                            st.success(f"✅ Generated {len(result_df)} emails!")

                        except Exception as e:
                            st.error(f"Error: {str(e)}")

    with tabs[1]:
        st.subheader("📚 Prompt Template Library")

        # Load all templates
        templates = load_prompt_templates()

        # Add custom templates
        if os.path.exists("custom_prompts"):
            for file in os.listdir("custom_prompts"):
                if file.endswith(".txt"):
                    name = file.replace(".txt", "").replace("_", " ").title()
                    with open(f"custom_prompts/{file}", "r") as f:
                        templates[name] = f.read()

        # Display templates
        for name, content in templates.items():
            if content:
                with st.expander(f"📄 {name}"):
                    st.text_area(
                        "Template Content",
                        value=content,
                        height=200,
                        key=f"template_{name}",
                        disabled=True
                    )
                    if st.button(f"Copy {name}", key=f"copy_{name}"):
                        st.info("Template copied! Paste it in the Generate tab.")

    with tabs[2]:
        st.subheader("📊 Generated Results")

        if st.session_state.generated_emails is not None:
            df = st.session_state.generated_emails

            # Stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Emails", len(df))
            with col2:
                success_rate = len(df[df['emailBody'].notna()]) / len(df) * 100 if len(df) > 0 else 0
                st.metric("Success Rate", f"{success_rate:.1f}%")
            with col3:
                avg_length = df['emailBody'].str.len().mean() if 'emailBody' in df.columns else 0
                st.metric("Avg Email Length", f"{avg_length:.0f} chars")

            # Display results
            st.dataframe(df)

            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv,
                file_name=f"generated_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

            # Preview emails
            st.subheader("Email Previews")
            for idx, row in df.head(3).iterrows():
                with st.expander(f"Email {idx + 1}: {row.get('first_name', 'Unknown')} at {row.get('organization_name', 'Unknown')}"):
                    st.write(f"**Subject:** {row.get('subject', 'N/A')}")
                    st.write("**Body:**")
                    st.text(row.get('emailBody', 'N/A'))
        else:
            st.info("No emails generated yet. Go to the Generate tab to start!")

if __name__ == "__main__":
    main()
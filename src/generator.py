from typing import Dict, List
from openai import OpenAI


def generate_email(
    openai_api_key: str,
    model: str,
    prospect: Dict[str, str],
    hooks: List[str],
    prompt_template: str,
    timeout: int = 60,
) -> Dict[str, str]:
    client = OpenAI(api_key=openai_api_key, timeout=timeout)

    first_name = (prospect.get("firstName") or "").strip()
    short_name = (prospect.get("shortName") or "").strip()
    industry = (prospect.get("industry") or "").strip()

    hooks_text = "\n".join(f"- {h}" for h in hooks)

    # Use updatedprompt verbatim, then attach data + hooks
    input_text = (
        f"{prompt_template}\n\n"
        "Prospect + research data:\n"
        f"firstName: {first_name}\n"
        f"title: {prospect.get('title','')}\n"
        f"email: {prospect.get('email','')}\n"
        f"organization_website_url: {prospect.get('organization_website_url','')}\n"
        f"shortName: {short_name}\n"
        f"city: {prospect.get('city','')}\n"
        f"industry: {industry}\n"
        f"linkedin_url: {prospect.get('linkedin_url','')}\n\n"
        f"Hooks:\n{hooks_text}\n"
    )

    # GPT-5-mini via Responses API, normal text output
    resp = client.responses.create(
        model=model,
        input=input_text,
        max_output_tokens=1000,
        text={"format": {"type": "text"}, "verbosity": "low"},
        reasoning={"effort": "low"},
    )

    content = getattr(resp, "output_text", None) or ""
    if not content:
        try:
            d = resp.to_dict()  # type: ignore
            parts = []
            for item in d.get("output", []) or []:
                for c in item.get("content", []) or []:
                    txt = c.get("text")
                    if isinstance(txt, str) and txt.strip():
                        parts.append(txt)
            content = "\n".join(parts)
        except Exception:
            content = ""

    # Parse: Subject: ... first line, rest is body
    subject = None
    lines = (content or "").splitlines()
    body_lines: List[str] = []
    for line in lines:
        if subject is None and line.strip().lower().startswith("subject:"):
            subject = line.split(":", 1)[1].strip()
            continue
        body_lines.append(line)
    email_body = "\n".join(body_lines).strip()

    if not subject or not email_body:
        preview = (content or "").strip()[:200]
        raise RuntimeError(f"Model output missing subject or body: '{preview}'")

    return {"subject": subject, "emailBody": email_body}


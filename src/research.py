import json
from typing import List, Dict
import requests


def fetch_hooks(perplexity_api_key: str, model: str, prospect: Dict[str, str], timeout: int = 60) -> List[str]:
    system = (
        "You are a research assistant. Given a company domain/name, return 3-6 recent, factual hooks "
        "from the last 6-12 months: funding, awards, partnerships, launches, community work, compliance, expansions. "
        "Return STRICT JSON with the shape: {\"hooks\":[\"...\"]}."
    )
    user = (
        f"Company domain: {prospect.get('organization_website_url','')}.\n"
        f"Company short name: {prospect.get('shortName','')}.\n"
        f"Industry: {prospect.get('industry','')}. City: {prospect.get('city','')}.\n"
        f"LinkedIn: {prospect.get('linkedin_url','')}.\n"
        "Return 3-6 hooks as short phrases."
    )

    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {perplexity_api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 400,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Perplexity API error {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except Exception as e:
        raise RuntimeError(f"Unexpected Perplexity response: {str(e)}")

    try:
        obj = json.loads(content)
    except Exception:
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            obj = json.loads(content[start : end + 1])
        else:
            raise RuntimeError("Perplexity did not return JSON content")

    hooks = obj.get("hooks") or []
    hooks = [h for h in hooks if isinstance(h, str) and h.strip()]
    if not (3 <= len(hooks) <= 6):
        raise RuntimeError("Perplexity returned invalid hooks count")
    return hooks


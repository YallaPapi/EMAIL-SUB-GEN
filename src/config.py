import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class AppConfig:
    openai_api_key: str
    perplexity_api_key: str
    openai_model: str = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
    perplexity_model: str = os.environ.get("PERPLEXITY_MODEL", "sonar-pro")
    request_timeout: int = int(os.environ.get("REQUEST_TIMEOUT", "60"))
    batch_size: int = int(os.environ.get("BATCH_SIZE", "50"))
    concurrency: int = int(os.environ.get("CONCURRENCY", "4"))
    batch_retries: int = int(os.environ.get("BATCH_RETRIES", "1"))
    output_dir: str = os.environ.get("OUTPUT_DIR", "output")


def load_config(env_path: Optional[str] = ".env") -> AppConfig:
    if env_path:
        load_dotenv(env_path)

    openai_key = os.environ.get("OPENAI_API_KEY")
    pplx_key = os.environ.get("PERPLEXITY_API_KEY")

    missing = []
    if not openai_key:
        missing.append("OPENAI_API_KEY")
    if not pplx_key:
        missing.append("PERPLEXITY_API_KEY")
    if missing:
        raise RuntimeError("Missing required environment variables: " + ", ".join(missing))

    return AppConfig(openai_api_key=openai_key, perplexity_api_key=pplx_key)


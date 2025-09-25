import concurrent.futures
from typing import Any, Dict, List, Tuple
import os
import time

from .config import AppConfig
from .io_utils import read_csv, write_csv, normalize_columns, sanitize_text
from .research import fetch_hooks
from .generator import generate_email
from .metrics import COUNTERS


def _process_batch(
    cfg: AppConfig,
    batch: List[Tuple[int, Dict[str, str]]],
    prompt_text: str,
) -> List[Tuple[int, Dict[str, str]]]:
    results: List[Tuple[int, Dict[str, str]]] = []
    for idx, prospect in batch:
        try:
            hooks = fetch_hooks(
                perplexity_api_key=cfg.perplexity_api_key,
                model=cfg.perplexity_model,
                prospect=prospect,
                timeout=cfg.request_timeout,
            )
        except Exception:
            # If research fails (e.g., rate limit), proceed with empty hooks
            hooks = []
        gen = generate_email(
            openai_api_key=cfg.openai_api_key,
            model=cfg.openai_model,
            prospect=prospect,
            hooks=hooks,
            prompt_template=prompt_text,
            timeout=cfg.request_timeout,
        )
        gen["subject"] = sanitize_text(gen["subject"]) 
        gen["emailBody"] = sanitize_text(gen["emailBody"]) 
        if not gen["subject"] or not gen["emailBody"]:
            raise RuntimeError("Empty subject or email body")
        results.append((idx, gen))
    return results


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def run_pipeline(
    cfg: AppConfig,
    input_path: str,
    output_path: str,
) -> None:
    df = read_csv(input_path)
    df, row_maps = normalize_columns(df)

    rows: List[Tuple[int, Dict[str, str]]] = [(idx, row_maps[idx]) for idx in df.index]

    # Load updated prompt only (no fallback)
    prompt_path = os.path.join(os.getcwd(), "updatedprompt.txt")
    if not os.path.exists(prompt_path):
        raise RuntimeError(f"Prompt file not found: {prompt_path}")
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt_text = f.read()

    start = time.time()
    all_results: List[Tuple[int, Dict[str, str]]] = []

    # Threaded row-level parallelism
    def run_row(item: Tuple[int, Dict[str, str]]) -> Tuple[int, Dict[str, str]]:
        idx, prospect = item
        try:
            hooks = fetch_hooks(
                perplexity_api_key=cfg.perplexity_api_key,
                model=cfg.perplexity_model,
                prospect=prospect,
                timeout=cfg.request_timeout,
            )
        except Exception:
            hooks = []
        hooks_valid = 2 <= len(hooks) <= 8
        gen = generate_email(
            openai_api_key=cfg.openai_api_key,
            model=cfg.openai_model,
            prospect=prospect,
            hooks=hooks,
            prompt_template=prompt_text,
            timeout=cfg.request_timeout,
        )
        gen["subject"] = sanitize_text(gen["subject"]) 
        gen["emailBody"] = sanitize_text(gen["emailBody"]) 
        if not gen["subject"] or not gen["emailBody"]:
            raise RuntimeError("Empty subject or email body")
        gen["hookTestFailed"] = "true" if not hooks_valid else "false"
        return idx, gen

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg.concurrency) as pool:
        futures = [pool.submit(run_row, item) for item in rows]
        try:
            for fut in concurrent.futures.as_completed(futures):
                idx, gen = fut.result()
                all_results.append((idx, gen))
        except Exception as e:
            # Cancel all outstanding work and fail fast
            for f in futures:
                f.cancel()
            raise

    for idx, gen in all_results:
        df.loc[idx, "subject"] = gen["subject"]
        df.loc[idx, "emailBody"] = gen["emailBody"]
        df.loc[idx, "hookTestFailed"] = gen.get("hookTestFailed", "false")

    write_csv(df, output_path)
    duration = time.time() - start
    # Metrics snapshot
    m = COUNTERS.snapshot()
    print(
        f"Processed {len(rows)} rows in {duration:.1f}s. Output: {output_path}\n"
        f"OpenAI tokens - input: {m['openai_input']}, output: {m['openai_output']}\n"
        f"Perplexity tokens - input: {m['perplexity_input']}, output: {m['perplexity_output']}"
    )

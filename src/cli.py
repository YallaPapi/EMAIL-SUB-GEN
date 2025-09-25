import argparse
import os
from datetime import datetime

from .config import load_config
from .pipeline import run_pipeline


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bulk Email & Subject Line Generator")
    p.add_argument("--input", required=True, help="Path to input CSV")
    p.add_argument("--output", required=False, help="Path to output CSV")
    p.add_argument("--batch-size", type=int, default=None, help="Rows per batch (default from env)")
    p.add_argument("--concurrency", type=int, default=None, help="Parallel workers (default from env)")
    p.add_argument("--batch-retries", type=int, default=None, help="Batch retries (default from env)")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config()
    if args.batch_size:
        cfg.batch_size = args.batch_size
    if args.concurrency:
        cfg.concurrency = args.concurrency
    if args.batch_retries is not None:
        cfg.batch_retries = args.batch_retries

    input_path = args.input
    if not os.path.exists(input_path):
        raise SystemExit(f"Input file not found: {input_path}")

    if args.output:
        output_path = args.output
    else:
        os.makedirs(cfg.output_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(cfg.output_dir, f"{base}_with_emails_{ts}.csv")

    run_pipeline(cfg, input_path, output_path)


if __name__ == "__main__":
    main()


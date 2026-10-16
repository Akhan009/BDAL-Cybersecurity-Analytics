import argparse
import time
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATASET = PROJECT_DIR / "data" / "Global_Cybersecurity_Threats_2015-2024.csv"
INCOMING = PROJECT_DIR / "incoming"


def main():
    parser = argparse.ArgumentParser(
        description="Simulate cybersecurity events arriving in batches for Apache Flume."
    )
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--max-batches", type=int, default=0,
                        help="Maximum batches to emit; 0 means all records.")
    args = parser.parse_args()

    if args.batch_size < 1 or args.delay < 0:
        parser.error("--batch-size must be positive and --delay cannot be negative")

    INCOMING.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(DATASET)

    total = len(df)
    batches = 0

    print(f"Dataset: {DATASET.name}")
    print(f"Records available: {total}")
    print("Mode: simulated batch ingestion (not a live threat feed)")

    for start in range(0, total, args.batch_size):
        if args.max_batches and batches >= args.max_batches:
            break

        batch = df.iloc[start:start + args.batch_size]

        # Write outside the spool directory, then atomically rename the
        # completed file into it so Flume never reads a partial batch.
        temp_path = PROJECT_DIR / f".batch_{batches + 1:04d}.tmp"
        final_path = INCOMING / f"events_{batches + 1:04d}.csv"

        batch.to_csv(temp_path, index=False, header=False)
        temp_path.replace(final_path)

        batches += 1
        print(f"[{batches}] Emitted {len(batch)} records: {final_path.name}")

        if start + args.batch_size < total and (
            not args.max_batches or batches < args.max_batches
        ):
            time.sleep(args.delay)

    print(f"Finished: {batches} batches emitted.")


if __name__ == "__main__":
    main()

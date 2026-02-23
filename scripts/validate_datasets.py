from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from eoapi.datasets import load_datasets


def main() -> int:
    datasets = load_datasets()
    if not datasets:
        print("No dataset YAML files found.")
        return 1

    print(f"Validated {len(datasets)} dataset definition(s):")
    for dataset_id in sorted(datasets.keys()):
        print(f"- {dataset_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

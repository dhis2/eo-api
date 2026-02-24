from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from eoapi.processing.registry import (
    DEFAULT_PROVIDER_REGISTRY_PATH,
    load_dataset_registry,
)


def main() -> int:
    datasets = load_dataset_registry()
    if not datasets:
        print("No process registry datasets found.")
        return 1

    print(f"Provider registry file: {DEFAULT_PROVIDER_REGISTRY_PATH}")
    print(f"Validated {len(datasets)} process dataset definition(s):")
    for dataset_id in sorted(datasets.keys()):
        provider = datasets[dataset_id].provider.name
        print(f"- {dataset_id} (provider: {provider})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

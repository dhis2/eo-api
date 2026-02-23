import yaml
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
CONFIGS_DIR = SCRIPT_DIR / 'registry'

def list_datasets():
    """
    Loops through configs folder, loads YAML files, and returns a list
    of datasets.
    """
    datasets = []
    folder = CONFIGS_DIR
    
    # Check if directory exists
    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder}")

    # Iterate over .yaml and .yml files
    for file_path in folder.glob('*.y*ml'):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Use safe_load to avoid security risks
                file_datasets = yaml.safe_load(f)
                datasets.extend( file_datasets )
        except Exception as e:
            print(f"Error loading {file_path.name}: {e}")

    return datasets

def get_dataset(dataset_id):
    """
    Get dataset dict for a given id
    """
    from . import cache
    datasets_lookup = {d['id']: d for d in list_datasets()}
    if dataset_id in datasets_lookup:
        # get base dataset info
        dataset = datasets_lookup[dataset_id]

        # return
        return dataset

def get_dataset_with_cache_info(dataset_id):
    # get base dataset info
    dataset = get_dataset(dataset_id)
    
    if dataset:
        # add info from dataset cache
        from . import cache
        cache_info = cache.get_cache_info(dataset)
        dataset.update(cache_info)

        # return
        return dataset

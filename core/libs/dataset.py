"""
A set of utilities for handling datasets and containers of datasets.
"""
import logging
from core.filebrowser.ruciowrapper import ruciowrapper

_logger = logging.getLogger("bigpandamon")


def get_dataset_locations(name, is_full_replicas_only=False):
    """
    Get the locations of a dataset.

    Args:
        name (str): The name of the dataset, should have `scope:name` format.
        is_full_replicas_only (bool): True or False.

    Returns:
        rse_list: A list of locations where the dataset is available.
    """

    rse_list = []
    if not isinstance(name, str) or len(name) == 0 or ":" not in name:
        _logger.info("Invalid dataset or container name provided, returning empty list.")
        return rse_list

    rucio_client = ruciowrapper()
    replicas = rucio_client.getRSEbyDID(dids=[{'scope': name.split(":")[0], 'name': name.split(":")[1]}])

    if replicas is not None and len(replicas) > 0:
        for r in replicas:
            if is_full_replicas_only:
                if r['available_bytes'] == r['bytes'] and r['state'] == 'AVAILABLE':
                    rse_list.append(r['rse'])
            else:
                rse_list.append(r['rse'])

    return rse_list


def get_scope(dataset_name: str) -> str:
    """
    Get the scope of a dataset.

    Args:
        dataset_name (str): The name of the dataset, should have `scope:name` format.

    Returns:
        str: The scope of the dataset.
    """
    if not isinstance(dataset_name, str) or len(dataset_name) == 0:
        _logger.info("Invalid dataset or container name provided, returning empty string.")
        return ""

    if ':' in dataset_name:
        scope = dataset_name.split(':')[0]
    elif dataset_name.startswith('user') or dataset_name.startswith('group'):
        scope = '.'.join(dataset_name.split('.')[:2])
    else:
        scope = str(dataset_name).split('.')[0]

    return scope
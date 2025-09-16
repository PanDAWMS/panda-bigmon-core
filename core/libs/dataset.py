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
                if r['available_bytes'] == r['total_bytes']:
                    rse_list.extend([rse for rse, state in r['states'].items() if state == 'AVAILABLE'])
            else:
                rse_list.extend([rse for rse, state in r['states'].items() if state == 'AVAILABLE'])

    return rse_list



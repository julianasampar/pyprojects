from pathlib import Path

from claude.agents.data_discovery_agent__final.tools.utils.reader import get_datasource
from claude.agents.data_discovery_agent__final.tools.utils.writer import get_storage

def read_profiles(storage_type: str, folder_path: str) -> dict:
    storage = get_storage(
        storage_type=storage_type,
        folder_path=folder_path
    )

    return storage.read_json_from_storage(folder_path)


def read_data_sample(source_type: str, **kwargs) -> dict:
    source = get_datasource(
        source_type=source_type,
        **kwargs
    )

    return source.read_table(**kwargs)

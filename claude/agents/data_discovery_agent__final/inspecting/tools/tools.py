import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.writer import get_storage

def read_profiles(storage_type: str, folder_path: str) -> dict:
    storage = get_storage(
        storage_type=storage_type,
        folder_path=folder_path
    )

    return storage.read_json_from_storage(folder_path)

read_profiles__schema = {
        "name": "read_profiles",
        "description": """
            Reads profiling JSON files from the configured storage location and
            returns their contents. The storage can be local or cloud-based,
            depending on the source_type provided.
        """,
        "input_schema": {
            "type": "object",
            "properties": {
                "storage_type": {
                    "type": "string",
                    "description": """
                        Type of storage to read the profiling files from.
                        Supported values: 'local', 'aws'.
                    """
                },
                "folder_path": {
                    "type": "string",
                    "description": """
                        Path or prefix where the profiling JSON files are stored.
                        For local storage, this should be a directory path.
                        For AWS storage, this should be the S3 prefix containing
                        the profiling files.
                    """
                }
            },
            "required": [
                "source_type",
                "folder_path"
            ]
        }
    }
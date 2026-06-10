from pathlib import Path

from claude.agents.data_discovery_agent__final.tools.utils.reader import get_datasource
from claude.agents.data_discovery_agent__final.tools.utils.writer import get_storage

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

def read_data_sample(source_type: str, **kwargs) -> dict:
    source = get_datasource(
        source_type=source_type,
        **kwargs
    )

    return source.read_table(**kwargs)

read_data_sample__schema = {
    "name": "read_data_sample",
    "description": """
        Reads a sample of records from a table in the specified data source.
        Use this tool when you need to inspect actual data values rather than
        metadata or profiling information. Supports optional filtering and
        configurable sample size.
    """,
    "input_schema": {
        "type": "object",
        "properties": {
            "source_type": {
                "type": "string",
                "description": """
                    Type of data source to query.
                    Supported values: 'snowflake', 'csv'.
                """
            },
            "table_name": {
                "type": "string",
                "description": """
                    Name of the table to sample data from.
                """
            },
            "filter": {
                "type": "object",
                "description": """
                    Optional filter to apply before sampling.
                    Example:
                    {"<name_of_datasource>": {"column": "<name_of_column>", "value_to_filer":"value_1"}}
                    """
            },
            "sample_size": {
                "type": "integer",
                "description": """
                    Maximum number of rows to return.
                    Defaults to 1000.
                """,
                "default": 1000
            }
        },
        "required": [
            "source_type",
            "table_name"
        ]
    }
}
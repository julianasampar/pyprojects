"""
storage.py

Handles saving and loading profiling results to/from disk.
Sits between the profiler (Stage 1) and the LLM agents (Stage 2).

File structure produced:
    profiles/
    ├── manifest.json          ← list of all profiled tables + metadata
    ├── rental.json
    ├── film.json
    ├── customer.json
    └── ...
"""

import json
from pathlib import Path
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────────

def save_profiles(profiles: dict, output_dir: str = "./profiles") -> Path:
    """
    Saves profiling results to disk. One JSON file per table.
    Also writes a manifest.json summarising what was profiled and when.

    Parameters:
        profiles   : the dict returned by profile_all_tables()
        output_dir : folder to write files into (created if it doesn't exist)

    Returns:
        Path to the output directory

    Usage:
        results = profile_all_tables(source)
        save_profiles(results, output_dir="/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/.claude/.agents/data_discovery_agent/profiling/resources")
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    profiled_at = datetime.now(timezone.utc).isoformat()

    # Write one JSON file per table
    for table_name, table_profile in profiles.items():
        file_path = output_path / f"{table_name}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(table_profile, f, indent=2, default=str)

        print(f"  Saved: {file_path}")

    # Write the manifest — a lightweight index of what's available
    # The orchestrator reads this first to know which tables exist
    manifest = {
        "profiled_at": profiled_at,
        "table_count": len(profiles),
        "tables": [
            {
                "name":      name,
                "row_count": profile["row_count"],
                "file":      f"{name}.json",
            }
            for name, profile in profiles.items()
        ],
    }

    manifest_path = output_path / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"\n  Manifest written: {manifest_path}")
    print(f"  Profiled at: {profiled_at}")

    return output_path


# ─────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────

def load_manifest(profiles_dir: str = "./profiles") -> dict:
    """
    Loads the manifest — the index of all profiled tables.
    The orchestrator calls this first to know what's available.

    Returns a dict like:
        {
            "profiled_at": "2024-01-15T10:30:00+00:00",
            "table_count": 15,
            "tables": [
                {"name": "rental", "row_count": 16044, "file": "rental.json"},
                ...
            ]
        }
    """
    manifest_path = Path(profiles_dir) / "manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"No manifest found at {manifest_path}. "
            f"Run the profiler first with save_profiles()."
        )

    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_table_profile(table_name: str, profiles_dir: str = "./profiles") -> dict:
    """
    Loads the profile for a single table.
    The Descriptor and Behavior Analyst sub-agents call this per table.

    Parameters:
        table_name   : name of the table (without .json extension)
        profiles_dir : folder where profiles are stored

    Returns the full profile dict for that table.
    """
    file_path = Path(profiles_dir) / f"{table_name}.json"

    if not file_path.exists():
        raise FileNotFoundError(
            f"Profile not found for table '{table_name}' at {file_path}."
        )

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_all_profiles(profiles_dir: str = "./profiles") -> dict:
    """
    Loads all table profiles into memory at once.
    Useful for the Behavior Analyst, which needs cross-table context.

    Returns a dict keyed by table name:
        {
            "rental":   { ... },
            "film":     { ... },
            "customer": { ... },
        }
    """
    manifest = load_manifest(profiles_dir)

    profiles = {}
    for entry in manifest["tables"]:
        table_name = entry["name"]
        profiles[table_name] = load_table_profile(table_name, profiles_dir)

    return profiles
from pathlib import Path

from iot_platform.ingestion.manifest import (
    ProcessedFileManifest,
)


def discover_new_files(
    raw_path: str,
    manifest: ProcessedFileManifest,
) -> list[Path]:
    """
    Return raw files that have not yet been processed.

    Only JSON/JSONL files are considered input files.
    """

    path = Path(raw_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Raw input path does not exist: {path}"
        )

    files = sorted(
        file
        for file in path.iterdir()
        if file.is_file()
        and file.suffix in {".json", ".jsonl"}
    )

    return [
        file
        for file in files
        if not manifest.contains(file.name)
    ]
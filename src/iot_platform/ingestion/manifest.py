import json
from pathlib import Path 


class ProcessedFileManifest:
    """
    Tracks which raw input files have already been processed.

    This provides file-level idempotency for batch ingestion.
    """
    
    def __init__(self, path: str):
        self.path = Path(path)
        self._processed_files: set[str] = set()
        
        self._load()
        
    def _load(self) -> None:
        """Load previously processed files."""
        
        if not self.path.exists():
            return
        
        with self.path.open("r", encoding="utf-8") as file:
            data = json.load(file)  
        
        self._processed_files = set(data)
        
    def contains(self, file_name: str) -> bool:
        """Return True if the file has already been processed."""

        return file_name in self._processed_files

    def add(self, file_name: str) -> None:
        """Mark a file as successfully processed."""

        self._processed_files.add(file_name)

    def save(self) -> None:
        """Persist the manifest atomically."""

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = self.path.with_suffix(".tmp")

        with temporary_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                sorted(self._processed_files),
                file,
                indent=2,
            )

        temporary_path.replace(self.path)

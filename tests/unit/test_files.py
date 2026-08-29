from iot_platform.ingestion.files import (
    discover_new_files,
)
from iot_platform.ingestion.manifest import (
    ProcessedFileManifest,
)


def test_discover_new_files(tmp_path):

    raw_path = tmp_path / "raw"
    raw_path.mkdir()

    file_one = raw_path / "telemetry_001.jsonl"
    file_two = raw_path / "telemetry_002.jsonl"

    file_one.write_text("{}\n")
    file_two.write_text("{}\n")

    manifest = ProcessedFileManifest(
        str(tmp_path / "manifest.json")
    )

    manifest.add(file_one.name)

    new_files = discover_new_files(
        str(raw_path),
        manifest,
    )

    assert new_files == [file_two]
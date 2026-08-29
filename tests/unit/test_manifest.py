from iot_platform.ingestion.manifest import (
    ProcessedFileManifest,
)


def test_manifest_tracks_processed_files(tmp_path):

    manifest_path = (
        tmp_path / "processed_files.json"
    )

    manifest = ProcessedFileManifest(
        str(manifest_path)
    )

    assert not manifest.contains(
        "telemetry-001.json"
    )

    manifest.add(
        "telemetry-001.json"
    )

    assert manifest.contains(
        "telemetry-001.json"
    )

    manifest.save()

    reloaded = ProcessedFileManifest(
        str(manifest_path)
    )

    assert reloaded.contains(
        "telemetry-001.json"
    )
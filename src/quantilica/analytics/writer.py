"""Standardized Parquet writing with metadata injection."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import polars as pl
from quantilica.core.manifests import DownloadManifest

_TMP_SUFFIX = ".tmp.parquet"
_SIDECAR_SUFFIX = ".manifest.json"


def manifest_to_metadata(manifest: DownloadManifest | None) -> dict[str, str]:
    """Derive the ``quantilica.*`` metadata keys from a download manifest.

    The returned mapping is what gets persisted in the Parquet file-level
    metadata and in the JSON sidecar manifest. Optional manifest fields whose
    value is None (e.g. ``producer``) are omitted so null values are never
    embedded.

    Args:
        manifest (DownloadManifest | None): The provenance manifest. May be None,
            in which case an empty dict is returned.

    Returns:
        dict[str, str]: A ``quantilica.*``-prefixed mapping ready for
            Parquet metadata injection.
    """
    if manifest is None:
        return {}

    derived = {
        "quantilica.source_id": manifest.source_id,
        "quantilica.dataset_id": manifest.dataset_id,
        "quantilica.origin_url": manifest.url,
        "quantilica.origin_sha256": manifest.sha256,
        "quantilica.fetched_at": manifest.fetched_at,
        "quantilica.producer": manifest.producer,
    }
    return {key: str(value) for key, value in derived.items() if value is not None}


def to_parquet(
    data: pl.DataFrame | pl.LazyFrame,
    output_path: str | Path,
    *,
    manifest: DownloadManifest | None = None,
    compression: str = "zstd",
    add_sidecar: bool = True,
    **kwargs: Any,
) -> Path:
    """Write a Polars DataFrame to Parquet with optional manifest metadata.

    The write is atomic: the payload is materialized in a temporary file
    (``<target>.tmp.parquet``) in the same directory as the target and then
    atomically renamed with :func:`os.replace`, so a crash or failure never
    leaves a partial file at the target path (readers always see either the
    previous file or the complete new one).

    When a ``manifest`` is provided, its provenance is persisted in two
    places: as ``quantilica.*`` keys injected into the Parquet file-level
    metadata and as a JSON sidecar file written next to the target
    (``<target>.manifest.json``).

    Args:
        data (pl.DataFrame | pl.LazyFrame): The DataFrame or LazyFrame to write.
            LazyFrames are collected before writing.
        output_path (str | Path): The destination path for the Parquet file.
        manifest (DownloadManifest | None, optional): The download manifest
            containing provenance metadata. Defaults to None.
        compression (str, optional): The compression algorithm to use. Defaults to
            "zstd".
        add_sidecar (bool, optional): Whether to persist the JSON sidecar
            manifest (``<target>.manifest.json``) alongside the Parquet file
            when a manifest is provided. Defaults to True.
        **kwargs (Any): Extra keyword arguments forwarded to the underlying
            ``write_parquet`` method.

    Returns:
        Path: The path to the written Parquet file.
    """
    if kwargs.get("partition_by") is not None:
        raise ValueError(
            "partition_by writes a dataset directory and cannot be atomically "
            "renamed; split the data explicitly if needed"
        )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    metadata = manifest_to_metadata(manifest)
    metadata = metadata or None

    tmp_path = output.with_name(f"{output.stem}.tmp.parquet")
    try:
        if isinstance(data, pl.LazyFrame):
            data.collect().write_parquet(
                tmp_path, compression=compression, metadata=metadata, **kwargs
            )
        else:
            data.write_parquet(
                tmp_path, compression=compression, metadata=metadata, **kwargs
            )
        os.replace(tmp_path, output)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    if manifest is not None and add_sidecar:
        sidecar_path = output.with_name(f"{output.name}{_SIDECAR_SUFFIX}")
        sidecar_path.write_text(manifest.to_json(), encoding="utf-8")

    return output

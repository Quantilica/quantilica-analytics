"""Standardized Parquet writing with metadata injection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
from quantilica.core.manifests import DownloadManifest


def to_parquet(
    data: pl.DataFrame | pl.LazyFrame,
    output_path: str | Path,
    *,
    manifest: DownloadManifest | None = None,
    compression: str = "zstd",
    **kwargs: Any,
) -> Path:
    """Write a Polars DataFrame to Parquet with optional manifest metadata.

    The manifest's source information and hash are injected into the Parquet
    file's metadata for end-to-end traceability.
    """
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Prepare metadata for injection
    metadata = {}
    if manifest:
        metadata["quantilica.source_id"] = manifest.source_id
        metadata["quantilica.dataset_id"] = manifest.dataset_id
        metadata["quantilica.origin_url"] = manifest.url
        metadata["quantilica.origin_sha256"] = manifest.sha256
        metadata["quantilica.fetched_at"] = manifest.fetched_at
        metadata["quantilica.producer"] = manifest.producer

    # Write to Parquet
    if isinstance(data, pl.LazyFrame):
        data.collect().write_parquet(
            output, compression=compression, metadata=metadata, **kwargs
        )
    else:
        data.write_parquet(output, compression=compression, metadata=metadata, **kwargs)

    return output

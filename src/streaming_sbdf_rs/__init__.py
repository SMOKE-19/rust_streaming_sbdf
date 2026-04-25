"""Rust-backed streaming SBDF writer and Parquet export helpers."""

from __future__ import annotations

from pathlib import Path
from .streaming_sbdf_rs import (
    SBDFError,
    StreamingSbdfWriter,
    parquet_to_sbdf_streaming as _parquet_to_sbdf_streaming,
)


def parquet_to_sbdf_streaming(
    parquet_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
) -> Path:
    """Stream Parquet rows into SBDF table slices."""
    parquet_path = Path(parquet_path)
    sbdf_path = Path(sbdf_path)
    sbdf_path.parent.mkdir(parents=True, exist_ok=True)
    _parquet_to_sbdf_streaming(
        str(parquet_path),
        str(sbdf_path),
        batch_size=batch_size,
        column_types=column_types,
    )
    return sbdf_path


__all__ = ["SBDFError", "StreamingSbdfWriter", "parquet_to_sbdf_streaming"]

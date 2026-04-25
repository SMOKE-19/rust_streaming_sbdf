"""Rust-backed streaming SBDF writer and Parquet export helpers."""

from __future__ import annotations

from pathlib import Path
from .streaming_sbdf_rs import (
    SBDFError,
    StreamingSbdfWriter,
    parquet_files_to_sbdf_streaming as _parquet_files_to_sbdf_streaming,
    parquet_to_sbdf_streaming as _parquet_to_sbdf_streaming,
)


def parquet_to_sbdf_streaming(
    parquet_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
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
        encoding_rle=encoding_rle,
    )
    return sbdf_path


def parquet_files_to_sbdf_streaming(
    parquet_files: list[str | Path],
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
) -> Path:
    """Stream multiple Parquet files into one SBDF table."""
    parquet_files = [Path(path) for path in parquet_files]
    sbdf_path = Path(sbdf_path)
    sbdf_path.parent.mkdir(parents=True, exist_ok=True)
    _parquet_files_to_sbdf_streaming(
        [str(path) for path in parquet_files],
        str(sbdf_path),
        batch_size=batch_size,
        column_types=column_types,
        encoding_rle=encoding_rle,
    )
    return sbdf_path


def parquet_dataset_to_sbdf_streaming(
    dataset_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    recursive: bool = False,
) -> Path:
    """Stream all .parquet files from a dataset directory into one SBDF table."""
    dataset_path = Path(dataset_path)
    if not dataset_path.is_dir():
        raise ValueError(f"dataset_path is not a directory: {dataset_path}")
    pattern = "**/*.parquet" if recursive else "*.parquet"
    parquet_files = sorted(dataset_path.glob(pattern))
    return parquet_files_to_sbdf_streaming(
        parquet_files,
        sbdf_path,
        batch_size=batch_size,
        column_types=column_types,
        encoding_rle=encoding_rle,
    )


def parquet_manifest_to_sbdf_streaming(
    manifest_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
) -> Path:
    """Stream Parquet files listed in a line-based manifest into one SBDF table."""
    manifest_path = Path(manifest_path)
    base_path = manifest_path.parent
    parquet_files = []
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        path = Path(entry)
        parquet_files.append(path if path.is_absolute() else base_path / path)
    return parquet_files_to_sbdf_streaming(
        parquet_files,
        sbdf_path,
        batch_size=batch_size,
        column_types=column_types,
        encoding_rle=encoding_rle,
    )


__all__ = [
    "SBDFError",
    "StreamingSbdfWriter",
    "parquet_dataset_to_sbdf_streaming",
    "parquet_files_to_sbdf_streaming",
    "parquet_manifest_to_sbdf_streaming",
    "parquet_to_sbdf_streaming",
]

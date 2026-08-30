"""Tests unitaires du module d'ingestion bronze."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.bronze import ingest_file_to_bronze


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Crée un petit CSV d'exemple, structurellement proche de transactions.csv."""
    csv_path = tmp_path / "transactions_sample.csv"
    csv_path.write_text(
        "transaction_id,amount,currency\n"
        "tx-001,1000.50,XOF\n"
        "tx-002,2500.00,GNF\n"
    )
    return csv_path


def test_ingest_success_adds_traceability_columns(tmp_path: Path, sample_csv: Path) -> None:
    bronze_root = tmp_path / "bronze"

    result = ingest_file_to_bronze(
        source_path=sample_csv,
        dataset_name="transactions",
        bronze_root=bronze_root,
        batch_id="batch-test-001",
        ingestion_date=date(2026, 1, 15),
    )

    assert result.status == "SUCCESS"
    assert result.rows_read == 2
    assert result.output_path is not None

    df = pd.read_parquet(result.output_path)
    assert "_ingested_at" in df.columns
    assert "_source_file" in df.columns
    assert "_batch_id" in df.columns
    assert (df["_batch_id"] == "batch-test-001").all()
    assert (df["_source_file"] == "transactions_sample.csv").all()


def test_ingest_partitions_by_ingestion_date(tmp_path: Path, sample_csv: Path) -> None:
    bronze_root = tmp_path / "bronze"

    result = ingest_file_to_bronze(
        source_path=sample_csv,
        dataset_name="transactions",
        bronze_root=bronze_root,
        batch_id="batch-test-002",
        ingestion_date=date(2026, 3, 1),
    )

    expected_dir = bronze_root / "transactions" / "ingestion_date=2026-03-01"
    assert expected_dir.exists()
    assert Path(result.output_path).parent == expected_dir


def test_ingest_missing_file_returns_failed_status(tmp_path: Path) -> None:
    missing_path = tmp_path / "does_not_exist.csv"
    bronze_root = tmp_path / "bronze"

    result = ingest_file_to_bronze(
        source_path=missing_path,
        dataset_name="transactions",
        bronze_root=bronze_root,
        batch_id="batch-test-003",
    )

    assert result.status == "FAILED"
    assert result.error_message is not None
    assert "introuvable" in result.error_message


def test_ingest_empty_file_returns_failed_status(tmp_path: Path) -> None:
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("")
    bronze_root = tmp_path / "bronze"

    result = ingest_file_to_bronze(
        source_path=empty_csv,
        dataset_name="transactions",
        bronze_root=bronze_root,
        batch_id="batch-test-004",
    )

    assert result.status == "FAILED"


def test_ingest_preserves_raw_string_fidelity(tmp_path: Path, sample_csv: Path) -> None:
    """La zone bronze ne doit pas typer/convertir les données (rôle de la silver)."""
    bronze_root = tmp_path / "bronze"

    result = ingest_file_to_bronze(
        source_path=sample_csv,
        dataset_name="transactions",
        bronze_root=bronze_root,
        batch_id="batch-test-005",
    )

    df = pd.read_parquet(result.output_path)
    # La valeur doit être préservée telle quelle (chaîne "1000.50"), pas castée
    # en float (1000.5) : c'est le rôle de la couche silver, pas de la bronze.
    assert df.loc[0, "amount"] == "1000.50"
    assert not pd.api.types.is_float_dtype(df["amount"])

"""
Tests unitaires du loader silver -> staging.

Utilise SQLite en mémoire (schema=None) plutôt qu'un vrai PostgreSQL : la
logique testée (dtype -> type SQL, DROP/CREATE, insertion par lots,
idempotence, gestion des NULL) est indépendante du moteur SQL sous-jacent.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.loading.postgres_loader import load_parquet_to_staging


@pytest.fixture
def engine(tmp_path: Path) -> Engine:
    db_path = tmp_path / "test.db"
    return create_engine(f"sqlite:///{db_path}")


@pytest.fixture
def parquet_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "silver" / "transactions"
    directory.mkdir(parents=True)
    df = pd.DataFrame(
        {
            "transaction_id": ["TRX-1", "TRX-2", "TRX-3"],
            "amount": [100.5, 200.0, None],
            "is_valid": [True, False, True],
        }
    )
    df.to_parquet(directory / "part-0.parquet", index=False)
    return directory


def test_loads_all_rows(engine: Engine, parquet_dir: Path) -> None:
    n = load_parquet_to_staging(parquet_dir, "stg_transactions", engine, schema=None)

    assert n == 3
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM stg_transactions")).scalar()
    assert count == 3


def test_reload_is_idempotent_not_duplicating(engine: Engine, parquet_dir: Path) -> None:
    load_parquet_to_staging(parquet_dir, "stg_transactions", engine, schema=None)
    load_parquet_to_staging(parquet_dir, "stg_transactions", engine, schema=None)

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM stg_transactions")).scalar()
    assert count == 3


def test_preserves_null_values(engine: Engine, parquet_dir: Path) -> None:
    load_parquet_to_staging(parquet_dir, "stg_transactions", engine, schema=None)

    with engine.connect() as conn:
        amount = conn.execute(
            text("SELECT amount FROM stg_transactions WHERE transaction_id = 'TRX-3'")
        ).scalar()
    assert amount is None


def test_converts_nat_datetime_to_null(engine: Engine, tmp_path: Path) -> None:
    """
    Non-régression : une colonne datetime64 avec des NaT (dates manquantes,
    ex. `completed_at` sur une transaction PENDING/FAILED) doit être chargée
    comme NULL en base, pas comme la chaîne littérale "NaT" — ce qui a été
    observé en conditions réelles et fait planter l'insertion PostgreSQL
    avec une erreur InvalidDatetimeFormat.
    """
    directory = tmp_path / "silver" / "with_nat"
    directory.mkdir(parents=True)
    df = pd.DataFrame(
        {
            "transaction_id": ["TRX-1", "TRX-2"],
            "completed_at": [pd.Timestamp("2026-01-01 10:00:00"), pd.NaT],
        }
    )
    df.to_parquet(directory / "part-0.parquet", index=False)

    load_parquet_to_staging(directory, "stg_with_nat", engine, schema=None)

    with engine.connect() as conn:
        completed_at = conn.execute(
            text("SELECT completed_at FROM stg_with_nat WHERE transaction_id = 'TRX-2'")
        ).scalar()
    assert completed_at is None


def test_preserves_boolean_values(engine: Engine, parquet_dir: Path) -> None:
    load_parquet_to_staging(parquet_dir, "stg_transactions", engine, schema=None)

    with engine.connect() as conn:
        values = conn.execute(text("SELECT is_valid FROM stg_transactions ORDER BY transaction_id")).fetchall()
    assert [v[0] for v in values] == [1, 0, 1]  # SQLite représente les booléens en 0/1


def test_handles_large_volume(engine: Engine, tmp_path: Path) -> None:
    n_rows = 20_000
    directory = tmp_path / "silver" / "big"
    directory.mkdir(parents=True)
    df = pd.DataFrame({"id": range(n_rows), "value": [i * 1.5 for i in range(n_rows)]})
    df.to_parquet(directory / "part-0.parquet", index=False)

    n = load_parquet_to_staging(directory, "stg_big", engine, schema=None, chunksize=5_000)

    assert n == n_rows
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM stg_big")).scalar()
    assert count == n_rows

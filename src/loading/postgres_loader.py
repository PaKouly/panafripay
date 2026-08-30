"""
Chargement silver -> PostgreSQL (staging) — PanAfriPay.

Charge les fichiers Parquet de la zone silver dans des tables de staging
PostgreSQL, préalable nécessaire au modèle dimensionnel dbt (Phase 5) :
dbt transforme des tables SQL, pas des fichiers Parquet directement.

Implémentation en SQLAlchemy Core pur (Table/MetaData/insert), et non
`pandas.to_sql()` : à partir de pandas 2.0, `to_sql()` exige SQLAlchemy>=2.0,
alors que le sujet impose Apache Airflow 2.9.3, qui contraint
`sqlalchemy<2.0,>=1.4.36` (cf. requirements.txt). Utiliser `pandas.to_sql()`
provoquerait un `AttributeError` silencieux et confus dans cet environnement
précis — un cas d'incompatibilité de versions entre dépendances imposées par
le sujet, contourné ici en restant au niveau de l'API SQLAlchemy stable.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    MetaData,
    Table,
    Text,
)
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _sqlalchemy_type_for(dtype) -> type:
    """Fait correspondre un dtype pandas à un type de colonne SQLAlchemy."""
    kind = dtype.kind
    if kind in ("i", "u"):
        return BigInteger
    if kind == "f":
        return Float
    if kind == "b":
        return Boolean
    if kind == "M":  # datetime64
        return DateTime
    return Text  # object, string, catégorie, etc. -> texte par défaut


def load_parquet_to_staging(
    parquet_dir: Path,
    table_name: str,
    engine: Engine,
    schema: str | None = "staging",
    chunksize: int = 10_000,
) -> int:
    """
    Charge tous les fichiers Parquet d'un dossier (une couche silver) dans
    une table de staging PostgreSQL, en la remplaçant intégralement.

    Args:
        parquet_dir: dossier contenant le(s) fichier(s) Parquet à charger
            (ex: data/silver/transactions).
        table_name: nom de la table de destination (ex: "stg_transactions").
        engine: moteur SQLAlchemy déjà connecté à la base cible.
        schema: schéma PostgreSQL de destination (créé s'il n'existe pas).
            Passer None pour les moteurs sans support de schéma (ex: SQLite,
            utilisé dans les tests).
        chunksize: nombre de lignes envoyées par lot INSERT.

    Returns:
        Le nombre de lignes chargées.

    Remplace intégralement la table à chaque exécution (DROP puis CREATE) :
    le staging n'est qu'un miroir de la silver, pas un historique — c'est le
    modèle dimensionnel (Phase 5, DIM_CUSTOMER en SCD2) qui porte l'historique,
    pas cette étape.
    """
    df = pd.read_parquet(parquet_dir)

    metadata = MetaData(schema=schema)
    columns = [Column(col, _sqlalchemy_type_for(df[col].dtype)) for col in df.columns]
    table = Table(table_name, metadata, *columns)

    with engine.begin() as conn:
        if schema is not None:
            conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        table.drop(conn, checkfirst=True)
        table.create(conn)

        # .astype(object) est indispensable AVANT .where() : sans cela, une
        # colonne restée en dtype datetime64 reconvertit automatiquement
        # tout None réassigné en NaT (pandas.NaT), que psycopg2 ne sait pas
        # adapter et transmet à PostgreSQL comme la chaîne littérale "NaT",
        # provoquant une InvalidDatetimeFormat. Caster en object d'abord
        # fait perdre à pandas cette reconversion automatique.
        records = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
        for start in range(0, len(records), chunksize):
            chunk = records[start : start + chunksize]
            if chunk:
                conn.execute(table.insert(), chunk)

    qualified_name = f"{schema}.{table_name}" if schema else table_name
    logger.info("Chargé %d lignes dans %s depuis %s", len(df), qualified_name, parquet_dir)
    return len(df)


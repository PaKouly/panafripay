"""
Module d'ingestion bronze — PanAfriPay.

Ingère un fichier CSV source tel quel (fidélité maximale, pas de typage ni de
nettoyage) et l'écrit en Parquet dans la zone bronze, partitionné par date
d'ingestion, avec des colonnes de traçabilité.

Principe bronze (cf. sujet, section 9) : conserver les données brutes telles
qu'ingérées. Le typage, le nettoyage et la normalisation sont du ressort de
la couche silver (Phase 4), pas de cette couche.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Résultat d'une ingestion bronze pour un fichier source."""

    dataset_name: str
    status: str  # "SUCCESS" ou "FAILED"
    rows_read: int = 0
    output_path: str | None = None
    error_message: str | None = None


def ingest_file_to_bronze(
    source_path: Path,
    dataset_name: str,
    bronze_root: Path,
    batch_id: str,
    ingestion_date: date | None = None,
) -> IngestionResult:
    """
    Ingère un fichier CSV source vers la zone bronze au format Parquet.

    Args:
        source_path: chemin du fichier CSV source (ex: data/raw/transactions.csv).
        dataset_name: nom logique du jeu de données (ex: "transactions").
        bronze_root: racine de la zone bronze (ex: data/bronze).
        batch_id: identifiant unique du lot d'ingestion (ex: un UUID),
            partagé par tous les fichiers ingérés dans la même exécution.
        ingestion_date: date d'ingestion utilisée pour le partitionnement.
            Par défaut, la date du jour (UTC).

    Returns:
        IngestionResult décrivant le statut de l'ingestion (succès ou échec),
        le nombre de lignes lues et le chemin de sortie.

    Ne lève jamais d'exception : les erreurs (fichier absent, CSV corrompu,
    fichier vide) sont capturées et renvoyées dans le statut, pour permettre
    à l'appelant (script ou tâche Airflow) de poursuivre l'ingestion des
    autres fichiers du lot plutôt que d'interrompre tout le pipeline.
    """
    ingestion_date = ingestion_date or datetime.now(timezone.utc).date()
    ingested_at = datetime.now(timezone.utc)

    if not source_path.exists():
        message = f"Fichier source introuvable : {source_path}"
        logger.error(message)
        return IngestionResult(dataset_name=dataset_name, status="FAILED", error_message=message)

    try:
        # dtype=str : on préserve la fidélité brute (pas de conversion de type
        # à ce stade, pour éviter toute perte ou erreur d'interprétation).
        df = pd.read_csv(source_path, dtype=str, keep_default_na=True)
    except pd.errors.EmptyDataError:
        message = f"Fichier vide ou illisible : {source_path}"
        logger.error(message)
        return IngestionResult(dataset_name=dataset_name, status="FAILED", error_message=message)
    except (pd.errors.ParserError, UnicodeDecodeError) as exc:
        message = f"Fichier corrompu ou mal formé ({source_path}) : {exc}"
        logger.error(message)
        return IngestionResult(dataset_name=dataset_name, status="FAILED", error_message=message)

    if df.empty:
        message = f"Fichier sans aucune ligne de données : {source_path}"
        logger.warning(message)
        return IngestionResult(dataset_name=dataset_name, status="FAILED", error_message=message)

    # Colonnes de traçabilité exigées par le sujet (section 12, Phase 3)
    df["_ingested_at"] = ingested_at.isoformat()
    df["_source_file"] = source_path.name
    df["_batch_id"] = batch_id

    output_dir = bronze_root / dataset_name / f"ingestion_date={ingestion_date.isoformat()}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{dataset_name}_{batch_id}.parquet"

    df.to_parquet(output_path, index=False, engine="pyarrow")

    logger.info(
        "Ingestion réussie : %s (%d lignes) -> %s", dataset_name, len(df), output_path
    )
    return IngestionResult(
        dataset_name=dataset_name,
        status="SUCCESS",
        rows_read=len(df),
        output_path=str(output_path),
    )

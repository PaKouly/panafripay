"""
Script d'ingestion bronze — PanAfriPay.

Ingère les 5 fichiers CSV source vers la zone bronze (format Parquet,
partitionné par date d'ingestion). Conçu pour être appelé directement en
ligne de commande, ou plus tard depuis une tâche Airflow (Phase 7).

Usage :
    python scripts/run_bronze_ingestion.py
"""

from __future__ import annotations

import logging
import sys
import uuid
from pathlib import Path

# Permet d'exécuter ce script directement (python scripts/run_bronze_ingestion.py)
# sans installer le projet comme paquet.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.bronze import ingest_file_to_bronze  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
BRONZE_ROOT = Path("data/bronze")

# Un fichier source -> un dataset logique en bronze.
DATASETS = {
    "transactions": RAW_DIR / "transactions.csv",
    "customers": RAW_DIR / "customers.csv",
    "agents": RAW_DIR / "agents.csv",
    "operators": RAW_DIR / "operators.csv",
    "fx_rates": RAW_DIR / "fx_rates.csv",
}


def main() -> int:
    batch_id = str(uuid.uuid4())
    logger.info("Démarrage du batch d'ingestion bronze : %s", batch_id)

    results = [
        ingest_file_to_bronze(
            source_path=source_path,
            dataset_name=dataset_name,
            bronze_root=BRONZE_ROOT,
            batch_id=batch_id,
        )
        for dataset_name, source_path in DATASETS.items()
    ]

    print("\n=== Résumé du batch d'ingestion bronze ===")
    print(f"Batch ID : {batch_id}\n")
    print(f"{'Dataset':<15} {'Statut':<10} {'Lignes':>10}  Chemin de sortie / Erreur")
    print("-" * 90)
    for r in results:
        detail = r.output_path if r.status == "SUCCESS" else r.error_message
        print(f"{r.dataset_name:<15} {r.status:<10} {r.rows_read:>10}  {detail}")

    n_failed = sum(1 for r in results if r.status == "FAILED")
    if n_failed:
        logger.error("%d fichier(s) en échec sur %d.", n_failed, len(results))
        return 1

    logger.info("Batch terminé avec succès : %d fichiers ingérés.", len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

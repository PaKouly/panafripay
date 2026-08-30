"""
Script de chargement silver -> PostgreSQL — PanAfriPay.

Charge les 5 fichiers de la zone silver vers des tables de staging
PostgreSQL, prérequis pour les modèles dbt (Phase 5).

Lit la configuration de connexion depuis infra/.env (créé à partir de
infra/.env.example), ou depuis les variables d'environnement si déjà
définies (utile pour Airflow/CI, Phase 7).

Usage :
    python scripts/run_load_to_postgres.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine  # noqa: E402

from src.loading.postgres_loader import load_parquet_to_staging  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

SILVER_ROOT = Path("data/silver")
ENV_FILE = Path("infra/.env")

# dataset silver -> nom de la table de staging
DATASETS = {
    "transactions": "stg_transactions",
    "customers": "stg_customers",
    "agents": "stg_agents",
    "operators": "stg_operators",
    "fx_rates": "stg_fx_rates",
}


def load_env_file(path: Path) -> None:
    """
    Charge un fichier .env (format KEY=VALUE, une variable par ligne) dans
    les variables d'environnement du processus, sans écraser une variable
    déjà définie par ailleurs (permet de surcharger via l'environnement
    réel, utile en CI/Airflow).

    Pas de dépendance à python-dotenv (non présente dans requirements.txt) :
    ce parseur minimal suffit pour un fichier .env simple sans cas complexes
    (guillemets, valeurs multilignes, etc.).
    """
    if not path.exists():
        logger.warning("Fichier %s introuvable, utilisation des variables d'environnement système.", path)
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def build_engine():
    user = os.environ.get("POSTGRES_USER", "panafripay")
    password = os.environ.get("POSTGRES_PASSWORD", "panafripay")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "panafripay")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def main() -> int:
    load_env_file(ENV_FILE)
    engine = build_engine()

    print("\n=== Chargement silver -> staging PostgreSQL ===")
    print(f"{'Dataset':<15} {'Table':<20} {'Lignes':>10}")
    print("-" * 50)

    n_failed = 0
    for dataset_name, table_name in DATASETS.items():
        parquet_dir = SILVER_ROOT / dataset_name
        try:
            n = load_parquet_to_staging(parquet_dir, table_name, engine, schema="staging")
            print(f"{dataset_name:<15} {table_name:<20} {n:>10}")
        except Exception as exc:  # noqa: BLE001
            logger.error("Échec du chargement de %s : %s", dataset_name, exc)
            print(f"{dataset_name:<15} {table_name:<20} {'ERREUR':>10}")
            n_failed += 1

    if n_failed:
        logger.error("%d dataset(s) en échec.", n_failed)
        return 1

    logger.info("Chargement terminé avec succès.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

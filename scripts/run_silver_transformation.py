"""
Script de transformation silver — PanAfriPay.

Lit la dernière partition bronze de chaque fichier, applique les
transformations (normalisation, déduplication, quarantaine), et écrit le
résultat en zone silver (données valides) et en zone quarantaine (données
rejetées, avec motif).

Usage :
    python scripts/run_silver_transformation.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.transformation.spark_utils import build_spark_session  # noqa: E402
from src.transformation.silver import (  # noqa: E402
    transform_agents,
    transform_customers,
    transform_transactions,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

BRONZE_ROOT = Path("data/bronze")
SILVER_ROOT = Path("data/silver")
QUARANTINE_ROOT = Path("data/quarantine")


def latest_bronze_path(dataset_name: str) -> str:
    """
    Renvoie un glob pointant vers toutes les partitions bronze disponibles
    pour un dataset (ex: data/bronze/transactions/ingestion_date=*/*.parquet).

    On lit toutes les partitions plutôt que la seule plus récente : en cas
    de plusieurs exécutions d'ingestion bronze dans la journée, chaque
    fichier reste distinct grâce à `_batch_id`, et Spark les fusionne
    naturellement à la lecture.
    """
    return str(BRONZE_ROOT / dataset_name / "ingestion_date=*" / "*.parquet")


def main() -> int:
    spark = build_spark_session("panafripay-silver")
    logger.info("Session Spark démarrée (version %s).", spark.version)

    transactions_bronze = spark.read.parquet(latest_bronze_path("transactions"))
    customers_bronze = spark.read.parquet(latest_bronze_path("customers"))
    agents_bronze = spark.read.parquet(latest_bronze_path("agents"))
    operators_bronze = spark.read.parquet(latest_bronze_path("operators"))
    fx_rates_bronze = spark.read.parquet(latest_bronze_path("fx_rates"))

    logger.info("Transformation agents...")
    agents_silver = transform_agents(agents_bronze)

    logger.info("Transformation customers...")
    customers_silver, customers_quarantine = transform_customers(customers_bronze)

    logger.info("Transformation transactions...")
    transactions_silver, transactions_quarantine = transform_transactions(
        transactions_bronze, agents_silver, operators_bronze
    )

    # operators et fx_rates : pas d'anomalie identifiée en Phase 2, on se
    # contente d'un typage de base pour la couche silver.
    from pyspark.sql import functions as F

    operators_silver = (
        operators_bronze.withColumn(
            "transaction_limit_daily", F.col("transaction_limit_daily").cast("double")
        )
        .withColumn("transaction_limit_monthly", F.col("transaction_limit_monthly").cast("double"))
        .withColumn("service_start_date", F.col("service_start_date").cast("date"))
    )
    fx_rates_silver = (
        fx_rates_bronze.withColumn("exchange_rate", F.col("exchange_rate").cast("double"))
        .withColumn("rate_date", F.col("rate_date").cast("date"))
    )

    outputs = {
        "transactions": (transactions_silver, transactions_quarantine),
        "customers": (customers_silver, customers_quarantine),
        "agents": (agents_silver, None),
        "operators": (operators_silver, None),
        "fx_rates": (fx_rates_silver, None),
    }

    print("\n=== Résumé de la transformation silver ===")
    print(f"{'Dataset':<15} {'Silver':>10} {'Quarantaine':>12}")
    print("-" * 40)
    for name, (silver_df, quarantine_df) in outputs.items():
        silver_path = SILVER_ROOT / name
        silver_df.write.mode("overwrite").parquet(str(silver_path))
        n_silver = silver_df.count()

        n_quarantine = 0
        if quarantine_df is not None:
            quarantine_path = QUARANTINE_ROOT / name
            quarantine_df.write.mode("overwrite").parquet(str(quarantine_path))
            n_quarantine = quarantine_df.count()

        print(f"{name:<15} {n_silver:>10} {n_quarantine:>12}")

    logger.info("Transformation silver terminée avec succès.")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

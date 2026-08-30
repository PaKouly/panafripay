"""Utilitaire de construction de la SparkSession — PanAfriPay."""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "panafripay-silver") -> SparkSession:
    """
    Construit une SparkSession locale.

    Cette version écrit la couche silver au format Parquet, pour rester
    testable sans dépendance réseau vers un dépôt Maven. Pour activer
    Delta Lake (recommandé par le sujet, section 9), remplacer cette
    fonction par :

        from delta import configure_spark_with_delta_pip
        builder = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    `configure_spark_with_delta_pip` télécharge automatiquement les JARs
    Delta nécessaires depuis Maven Central au premier lancement (connexion
    internet requise une seule fois, puis mis en cache localement). Une
    fois les transformations validées avec cette version Parquet, on
    pourra bosculer vers Delta pour bénéficier du versioning et des
    transactions ACID (utiles pour les mises à jour incrémentales,
    Phase 7).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # raisonnable en local
    )
    return builder.getOrCreate()

"""
Transformations bronze -> silver — PanAfriPay.

Chaque fonction `transform_*` prend le/les DataFrame(s) bronze et renvoie un
tuple (df_silver, df_quarantine) : les lignes valides d'un côté, les lignes
rejetées (avec leur motif de rejet) de l'autre. Aucune ligne n'est supprimée
silencieusement — c'est un principe de traçabilité exigé par le sujet
(section 9 : "aucune perte de donnée silencieuse").
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def normalize_msisdn(df: DataFrame, col_name: str, output_col: str | None = None) -> DataFrame:
    """
    Normalise une colonne MSISDN au format E.164 (+<indicatif><numéro>).

    Corrige trois anomalies connues (cf. profiling Phase 2) :
      - espaces internes ("+224 737197273" -> "+224737197273")
      - préfixe "00" au lieu de "+" ("00224737197273" -> "+224737197273")
      - "+" manquant ("224737197273" -> "+224737197273")

    Le résultat est écrit dans `output_col` (par défaut : même nom que
    `col_name`, écrasant la valeur brute). La colonne d'origine n'est
    modifiée que si `output_col` n'est pas fourni.
    """
    output_col = output_col or col_name

    stripped = F.regexp_replace(F.col(col_name), r"\s+", "")
    without_00_prefix = F.when(
        stripped.startswith("00"), F.concat(F.lit("+"), F.expr("substring(stripped, 3, 20)"))
    ).otherwise(stripped)

    return (
        df.withColumn("_stripped", stripped)
        .withColumn(
            "_without_00",
            F.when(
                F.col("_stripped").startswith("00"),
                F.concat(F.lit("+"), F.expr("substring(_stripped, 3, 20)")),
            ).otherwise(F.col("_stripped")),
        )
        .withColumn(
            output_col,
            F.when(F.col("_without_00").startswith("+"), F.col("_without_00")).otherwise(
                F.concat(F.lit("+"), F.col("_without_00"))
            ),
        )
        .drop("_stripped", "_without_00")
    )


def is_valid_msisdn(col_name: str):
    """Expression Spark : vrai si la colonne respecte le format E.164 (+ 10 à 15 chiffres)."""
    return F.col(col_name).rlike(r"^\+\d{10,15}$")


def deduplicate_keep_latest(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """
    Déduplique `df` sur `key_col`, en conservant la ligne la plus récente
    selon `order_col` (interprété comme timestamp).

    Règle de résolution retenue pour les conflits (cf. profiling Phase 2 :
    140 transaction_id avec des valeurs divergentes) : on garde la version
    la plus récente, jugée la plus probablement correcte / à jour.
    """
    window = Window.partitionBy(key_col).orderBy(F.col(order_col).cast("timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def transform_transactions(
    transactions_bronze: DataFrame,
    agents_silver: DataFrame,
    operators_bronze: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Transforme les transactions bronze en silver.

    Étapes : typage, normalisation des MSISDN, déduplication (dernière
    version conservée), puis séparation valides / quarantaine selon :
      - montant négatif ou supérieur au plafond quotidien de l'opérateur
      - agent_id référencé mais absent du référentiel agents
      - MSISDN émetteur toujours invalide après normalisation
    """
    typed = (
        transactions_bronze.withColumn("amount", F.col("amount").cast("double"))
        .withColumn("initiated_at", F.col("initiated_at").cast("timestamp"))
        .withColumn("completed_at", F.col("completed_at").cast("timestamp"))
    )

    normalized = normalize_msisdn(typed, "sender_msisdn")
    normalized = normalize_msisdn(normalized, "receiver_msisdn")

    deduped = deduplicate_keep_latest(normalized, key_col="transaction_id", order_col="initiated_at")

    with_limits = deduped.join(
        operators_bronze.select(
            "operator_code", F.col("transaction_limit_daily").cast("double").alias("transaction_limit_daily")
        ),
        on="operator_code",
        how="left",
    )

    agent_ids = agents_silver.select("agent_id").distinct()
    with_agent_check = with_limits.join(
        agent_ids.withColumn("_agent_exists", F.lit(True)),
        on="agent_id",
        how="left",
    )

    flagged = with_agent_check.withColumn(
        "_rejection_reasons",
        F.filter(
            F.array(
                F.when(F.col("amount") < 0, F.lit("MONTANT_NEGATIF")),
                F.when(
                    F.col("amount") > F.col("transaction_limit_daily"), F.lit("MONTANT_HORS_PLAFOND")
                ),
                F.when(
                    F.col("agent_id").isNotNull() & F.col("_agent_exists").isNull(),
                    F.lit("AGENT_ID_ORPHELIN"),
                ),
                F.when(~is_valid_msisdn("sender_msisdn"), F.lit("MSISDN_EMETTEUR_INVALIDE")),
            ),
            lambda e: e.isNotNull(),
        ),
    ).drop("_agent_exists")

    silver = flagged.filter(F.size(F.col("_rejection_reasons")) == 0).drop(
        "_rejection_reasons", "transaction_limit_daily"
    )
    quarantine = flagged.filter(F.size(F.col("_rejection_reasons")) > 0)

    return silver, quarantine


def transform_customers(customers_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Transforme les clients bronze en silver.

    Étapes : normalisation MSISDN, déduplication (dernier enregistrement
    conservé selon `registration_date`), puis quarantaine des dates de
    naissance manifestement aberrantes (non parsables, avant 1920, futures).
    """
    typed = customers_bronze.withColumn(
        "registration_date", F.col("registration_date").cast("date")
    ).withColumn("account_balance", F.col("account_balance").cast("double"))

    normalized = normalize_msisdn(typed, "msisdn")

    deduped = deduplicate_keep_latest(
        normalized, key_col="customer_id", order_col="registration_date"
    )

    with_birth_date = deduped.withColumn("_birth_date_parsed", F.col("birth_date").cast("date"))

    flagged = with_birth_date.withColumn(
        "_rejection_reasons",
        F.filter(
            F.array(
                F.when(F.col("_birth_date_parsed").isNull(), F.lit("DATE_NAISSANCE_INVALIDE")),
                F.when(
                    F.col("_birth_date_parsed").isNotNull() & (F.year("_birth_date_parsed") < 1920),
                    F.lit("DATE_NAISSANCE_TROP_ANCIENNE"),
                ),
                F.when(
                    F.col("_birth_date_parsed") > F.current_date(), F.lit("DATE_NAISSANCE_FUTURE")
                ),
            ),
            lambda e: e.isNotNull(),
        ),
    )

    silver = flagged.filter(F.size(F.col("_rejection_reasons")) == 0).drop(
        "_rejection_reasons", "_birth_date_parsed"
    )
    quarantine = flagged.filter(F.size(F.col("_rejection_reasons")) > 0).drop("_birth_date_parsed")

    return silver, quarantine


def transform_agents(agents_bronze: DataFrame) -> DataFrame:
    """
    Transforme les agents bronze en silver : typage et déduplication
    (dernier enregistrement conservé selon `onboarding_date`).

    Pas de quarantaine ici : l'absence de coordonnées géographiques (3,2%
    des agents, cf. profiling) est traitée comme une valeur manquante
    légitime, pas comme une anomalie bloquante.
    """
    typed = (
        agents_bronze.withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("onboarding_date", F.col("onboarding_date").cast("date"))
    )
    return deduplicate_keep_latest(typed, key_col="agent_id", order_col="onboarding_date")

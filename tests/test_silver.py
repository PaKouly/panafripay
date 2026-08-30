"""Tests unitaires des transformations bronze -> silver."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.silver import (
    normalize_msisdn,
    transform_agents,
    transform_customers,
    transform_transactions,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("test-silver")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _string_schema(columns: list[str]) -> StructType:
    """Construit un schéma 100% string, pour éviter les soucis d'inférence
    de type de Spark quand une colonne de test est entièrement à None."""
    return StructType([StructField(c, StringType(), True) for c in columns])


class TestNormalizeMsisdn:
    def test_handles_the_three_known_anomaly_formats(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("+224 737197273",), ("00224737197273",), ("224737197273",), ("+224737197273",)],
            ["msisdn"],
        )
        result = [row.msisdn for row in normalize_msisdn(df, "msisdn").collect()]
        assert result == ["+224737197273"] * 4


class TestTransformAgents:
    def test_deduplicates_keeping_latest_onboarding_date(self, spark: SparkSession) -> None:
        cols = [
            "agent_id", "agent_name", "operator_code", "country_code", "region",
            "city", "latitude", "longitude", "onboarding_date", "agent_status", "commission_tier",
        ]
        data = [
            ("AGT-001", "Agent A (v1)", "MOOV", "GIN", "Conakry", "Conakry", 9.5, -13.7, "2025-01-01", "ACTIVE", "T1"),
            ("AGT-001", "Agent A (v2)", "MOOV", "GIN", "Conakry", "Conakry", 9.5, -13.7, "2025-06-01", "ACTIVE", "T1"),
            ("AGT-002", "Agent B", "OM", "SEN", "Dakar", "Dakar", None, None, "2025-02-01", "ACTIVE", "T2"),
        ]
        df = spark.createDataFrame(data, cols)

        result = transform_agents(df)

        assert result.count() == 2
        kept = result.filter(result.agent_id == "AGT-001").collect()[0]
        assert kept.agent_name == "Agent A (v2)"


class TestTransformCustomers:
    COLS = [
        "customer_id", "msisdn", "first_name", "last_name", "birth_date", "kyc_level",
        "registration_date", "country_code", "region", "customer_status", "account_balance",
    ]

    def test_quarantines_unparseable_and_too_old_birth_dates(self, spark: SparkSession) -> None:
        data = [
            ("CUST-001", "+221771234567", "Awa", "Diop", "1990-05-12", "LEVEL_1", "2024-01-01", "SEN", "Dakar", "ACTIVE", "1000.0"),
            ("CUST-002", "+221772234567", "Modou", "Sy", "1850-01-01", "LEVEL_1", "2024-02-01", "SEN", "Dakar", "ACTIVE", "500.0"),
            ("CUST-003", "00221773234567", "Fatou", "Ndiaye", "invalid-date", "LEVEL_1", "2024-03-01", "SEN", "Dakar", "ACTIVE", "200.0"),
        ]
        df = spark.createDataFrame(data, self.COLS)

        silver, quarantine = transform_customers(df)

        assert silver.count() == 1
        assert quarantine.count() == 2
        reasons = {row.customer_id: row._rejection_reasons for row in quarantine.collect()}
        assert reasons["CUST-002"] == ["DATE_NAISSANCE_TROP_ANCIENNE"]
        assert reasons["CUST-003"] == ["DATE_NAISSANCE_INVALIDE"]

    def test_normalizes_msisdn_on_valid_rows(self, spark: SparkSession) -> None:
        data = [
            ("CUST-003", "00221773234567", "Fatou", "Ndiaye", "1995-01-01", "LEVEL_1", "2024-03-01", "SEN", "Dakar", "ACTIVE", "200.0"),
        ]
        df = spark.createDataFrame(data, self.COLS)

        silver, _ = transform_customers(df)

        assert silver.collect()[0].msisdn == "+221773234567"


class TestTransformTransactions:
    COLS = [
        "transaction_id", "transaction_ref", "transaction_type", "sender_msisdn", "receiver_msisdn",
        "agent_id", "amount", "currency", "fees", "operator_code", "country_code",
        "transaction_status", "initiated_at", "completed_at", "channel",
        "device_imei_hash", "error_code", "partner_merchant_id",
    ]
    OPERATORS_COLS = [
        "operator_code", "operator_name", "country_code", "license_number",
        "transaction_limit_daily", "transaction_limit_monthly", "service_start_date",
    ]

    @pytest.fixture
    def agents_silver(self, spark: SparkSession):
        cols = [
            "agent_id", "agent_name", "operator_code", "country_code", "region",
            "city", "latitude", "longitude", "onboarding_date", "agent_status", "commission_tier",
        ]
        data = [("AGT-001", "Agent A", "MOOV", "GIN", "Conakry", "Conakry", 9.5, -13.7, "2025-01-01", "ACTIVE", "T1")]
        return transform_agents(spark.createDataFrame(data, cols))

    @pytest.fixture
    def operators(self, spark: SparkSession):
        data = [("OM", "Orange Money", "SEN", "LIC-1", "2000000.0", "10000000.0", "2010-01-01")]
        return spark.createDataFrame(data, self.OPERATORS_COLS)

    def test_conflicting_duplicates_keep_most_recent_version(
        self, spark: SparkSession, agents_silver, operators
    ) -> None:
        data = [
            ("TRX-1", "REF-1", "DEPOSIT", "+221771111111", None, "AGT-001", "500.0", "XOF", "5.0", "OM", "SEN", "SUCCESS", "2026-01-01 10:00:00", None, "APP", None, None, None),
            ("TRX-1", "REF-1", "DEPOSIT", "+221771111111", None, "AGT-001", "999.0", "XOF", "5.0", "OM", "SEN", "SUCCESS", "2026-01-01 12:00:00", None, "APP", None, None, None),
        ]
        df = spark.createDataFrame(data, _string_schema(self.COLS))

        silver, quarantine = transform_transactions(df, agents_silver, operators)

        assert silver.count() == 1
        assert quarantine.count() == 0
        assert silver.collect()[0].amount == 999.0

    def test_quarantines_negative_amount(self, spark: SparkSession, agents_silver, operators) -> None:
        data = [
            ("TRX-2", "REF-2", "WITHDRAWAL", "+221772222222", None, "AGT-001", "-100.0", "XOF", "5.0", "OM", "SEN", "SUCCESS", "2026-01-02 10:00:00", None, "APP", None, None, None),
        ]
        df = spark.createDataFrame(data, _string_schema(self.COLS))

        silver, quarantine = transform_transactions(df, agents_silver, operators)

        assert silver.count() == 0
        assert quarantine.collect()[0]._rejection_reasons == ["MONTANT_NEGATIF"]

    def test_quarantines_amount_over_daily_limit(self, spark: SparkSession, agents_silver, operators) -> None:
        data = [
            ("TRX-3", "REF-3", "DEPOSIT", "+221773333333", None, "AGT-001", "5000000.0", "XOF", "5.0", "OM", "SEN", "SUCCESS", "2026-01-03 10:00:00", None, "APP", None, None, None),
        ]
        df = spark.createDataFrame(data, _string_schema(self.COLS))

        silver, quarantine = transform_transactions(df, agents_silver, operators)

        assert silver.count() == 0
        assert quarantine.collect()[0]._rejection_reasons == ["MONTANT_HORS_PLAFOND"]

    def test_quarantines_orphan_agent_id(self, spark: SparkSession, agents_silver, operators) -> None:
        data = [
            ("TRX-4", "REF-4", "DEPOSIT", "+221774444444", None, "AGT-999", "1000.0", "XOF", "5.0", "OM", "SEN", "SUCCESS", "2026-01-04 10:00:00", None, "APP", None, None, None),
        ]
        df = spark.createDataFrame(data, _string_schema(self.COLS))

        silver, quarantine = transform_transactions(df, agents_silver, operators)

        assert silver.count() == 0
        assert quarantine.collect()[0]._rejection_reasons == ["AGENT_ID_ORPHELIN"]

    def test_valid_transaction_passes_through_to_silver(
        self, spark: SparkSession, agents_silver, operators
    ) -> None:
        data = [
            ("TRX-5", "REF-5", "DEPOSIT", "+221775555555", None, "AGT-001", "1000.0", "XOF", "5.0", "OM", "SEN", "SUCCESS", "2026-01-05 10:00:00", None, "APP", None, None, None),
        ]
        df = spark.createDataFrame(data, _string_schema(self.COLS))

        silver, quarantine = transform_transactions(df, agents_silver, operators)

        assert silver.count() == 1
        assert quarantine.count() == 0

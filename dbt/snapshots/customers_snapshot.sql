{# 
    Snapshot dbt pour DIM_CUSTOMER (SCD2).

    Contrairement à un modèle classique (qui recalcule tout à chaque run),
    un snapshot COMPARE l'état actuel de la source à la dernière version
    connue, et n'insère une nouvelle ligne QUE si un des `check_cols` a
    changé. C'est le mécanisme natif de dbt pour le SCD2 — pas quelque
    chose qu'on implémente à la main avec des colonnes valid_from/valid_to
    gérées manuellement.

    Colonnes ajoutées automatiquement par dbt : dbt_valid_from,
    dbt_valid_to (NULL = version actuelle), dbt_scd_id, dbt_updated_at.

    strategy='check' plutôt que 'timestamp' : la source (stg_customers)
    n'a pas de colonne "updated_at" fiable pour détecter un changement,
    donc on compare directement les colonnes métier susceptibles de
    changer dans le temps (kyc_level, customer_status, account_balance).
#}

{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['kyc_level', 'customer_status', 'account_balance', 'region'],
    )
}}

select
    customer_id,
    msisdn,
    first_name,
    last_name,
    birth_date,
    kyc_level,
    registration_date,
    country_code,
    region,
    customer_status,
    account_balance
from {{ ref('stg_customers') }}

{% endsnapshot %}

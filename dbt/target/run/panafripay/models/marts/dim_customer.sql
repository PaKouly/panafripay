
  
    

  create  table "panafripay"."gold_gold"."dim_customer__dbt_tmp"
  
  
    as
  
  (
    -- DIM_CUSTOMER (SCD2) : une ligne par version historique d'un client.
-- Construite au-dessus du snapshot dbt (customers_snapshot), qui gère
-- déjà la détection de changement et l'historisation.
--
-- Clé de substitution en INT (sur demande du mentor) : row_number()
-- génère un entier séquentiel unique par ligne, ordonné par client puis
-- par date de début de validité (assure un ordre stable et reproductible
-- entre deux exécutions).

select
    row_number() over (order by customer_id, dbt_valid_from) as customer_key,
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
    account_balance,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to is null) as is_current
from "panafripay"."snapshots"."customers_snapshot"
  );
  
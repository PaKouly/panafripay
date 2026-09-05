
  
    

  create  table "panafripay"."gold_gold"."dim_operator__dbt_tmp"
  
  
    as
  
  (
    -- DIM_OPERATOR : référentiel très stable (5 lignes), pas de SCD2.
--
-- Clé de substitution en INT (sur demande du mentor).

select
    row_number() over (order by operator_code) as operator_key,
    operator_code,
    operator_name,
    country_code,
    license_number,
    transaction_limit_daily,
    transaction_limit_monthly,
    service_start_date
from "panafripay"."gold_staging_dbt"."stg_operators"
  );
  
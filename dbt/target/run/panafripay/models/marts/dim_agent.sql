
  
    

  create  table "panafripay"."gold_gold"."dim_agent__dbt_tmp"
  
  
    as
  
  (
    -- DIM_AGENT : dimension simple (pas de SCD2 requis, aucune anomalie de
-- changement identifiée en Phase 2 pour ce référentiel).
--
-- Clé de substitution en INT (sur demande du mentor).

select
    row_number() over (order by agent_id) as agent_key,
    agent_id,
    agent_name,
    operator_code,
    country_code,
    region,
    city,
    latitude,
    longitude,
    onboarding_date,
    agent_status,
    commission_tier
from "panafripay"."gold_staging_dbt"."stg_agents"
  );
  
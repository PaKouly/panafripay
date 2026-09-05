
  create view "panafripay"."gold_staging_dbt"."stg_agents__dbt_tmp"
    
    
  as (
    select
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
from "panafripay"."staging"."stg_agents"
  );
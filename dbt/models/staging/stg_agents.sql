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
from {{ source('staging', 'stg_agents') }}

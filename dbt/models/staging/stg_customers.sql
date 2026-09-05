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
from {{ source('staging', 'stg_customers') }}

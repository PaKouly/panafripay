select
    operator_code,
    operator_name,
    country_code,
    license_number,
    transaction_limit_daily,
    transaction_limit_monthly,
    service_start_date
from "panafripay"."staging"."stg_operators"
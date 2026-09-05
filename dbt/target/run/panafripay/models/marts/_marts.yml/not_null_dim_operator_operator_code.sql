select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select operator_code
from "panafripay"."gold_gold"."dim_operator"
where operator_code is null



      
    ) dbt_internal_test
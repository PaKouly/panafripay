select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select agent_id
from "panafripay"."gold_gold"."dim_agent"
where agent_id is null



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    agent_key as unique_field,
    count(*) as n_records

from "panafripay"."gold_gold"."dim_agent"
where agent_key is not null
group by agent_key
having count(*) > 1



      
    ) dbt_internal_test
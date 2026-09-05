select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select agent_key as from_field
    from (select * from "panafripay"."gold_gold"."fact_transaction" where agent_key is not null) dbt_subquery
    where agent_key is not null
),

parent as (
    select agent_key as to_field
    from "panafripay"."gold_gold"."dim_agent"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test
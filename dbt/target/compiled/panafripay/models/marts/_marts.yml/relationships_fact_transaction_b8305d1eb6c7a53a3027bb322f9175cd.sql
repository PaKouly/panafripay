
    
    

with child as (
    select customer_key as from_field
    from (select * from "panafripay"."gold_gold"."fact_transaction" where customer_key is not null) dbt_subquery
    where customer_key is not null
),

parent as (
    select customer_key as to_field
    from "panafripay"."gold_gold"."dim_customer"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



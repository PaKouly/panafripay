
    
    

with child as (
    select operator_key as from_field
    from "panafripay"."gold_gold"."fact_transaction"
    where operator_key is not null
),

parent as (
    select operator_key as to_field
    from "panafripay"."gold_gold"."dim_operator"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



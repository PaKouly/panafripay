
    
    

with child as (
    select transaction_date as from_field
    from "panafripay"."gold_gold"."fact_transaction"
    where transaction_date is not null
),

parent as (
    select full_date as to_field
    from "panafripay"."gold_gold"."dim_date"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null




    
    

select
    operator_key as unique_field,
    count(*) as n_records

from "panafripay"."gold_gold"."dim_operator"
where operator_key is not null
group by operator_key
having count(*) > 1



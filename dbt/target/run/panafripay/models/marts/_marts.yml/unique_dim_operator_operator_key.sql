select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    operator_key as unique_field,
    count(*) as n_records

from "panafripay"."gold_gold"."dim_operator"
where operator_key is not null
group by operator_key
having count(*) > 1



      
    ) dbt_internal_test
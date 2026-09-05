select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    operator_code as unique_field,
    count(*) as n_records

from "panafripay"."gold_gold"."dim_operator"
where operator_code is not null
group by operator_code
having count(*) > 1



      
    ) dbt_internal_test
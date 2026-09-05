
  
    

  create  table "panafripay"."gold_gold"."dim_date__dbt_tmp"
  
  
    as
  
  (
    -- DIM_DATE : générée directement en SQL (generate_series), sans package
-- externe. Couvre la période des données du projet (1er janvier au 31 mars
-- 2026, cf. sujet section 3) avec une marge de quelques jours.
--
-- Clé primaire : full_date (clé naturelle) — pas de clé de substitution
-- pour cette dimension, sur demande explicite du mentor.

with date_spine as (
    select generate_series(
        '2025-12-25'::date,
        '2026-04-05'::date,
        '1 day'::interval
    )::date as full_date
)

select
    full_date,
    extract(year from full_date)::int as year,
    extract(quarter from full_date)::int as quarter,
    extract(month from full_date)::int as month,
    to_char(full_date, 'Month') as month_name,
    extract(day from full_date)::int as day,
    extract(isodow from full_date)::int as day_of_week,  -- 1=lundi ... 7=dimanche
    to_char(full_date, 'Day') as day_name,
    (extract(isodow from full_date) in (6, 7)) as is_weekend
from date_spine
  );
  
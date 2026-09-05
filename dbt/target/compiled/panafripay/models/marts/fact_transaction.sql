-- FACT_TRANSACTION : une ligne par transaction (grain), reliée aux 4
-- dimensions.
--
-- Clé primaire : composite des 4 clés étrangères
-- (transaction_date, customer_key, agent_key, operator_key), sur demande
-- explicite du mentor — pas de clé de substitution dédiée. `transaction_id`
-- reste présent comme attribut dégénéré (traçabilité vers la source), mais
-- n'est plus la clé de la table.
--
-- ATTENTION (point à vérifier avec le mentor) : rien ne garantit qu'un même
-- client ne réalise pas deux transactions le même jour avec le même agent
-- et le même opérateur (ex. deux dépôts distincts). Dans ce cas, la clé
-- composite ne serait plus unique. Le test de non-régression
-- `tests/assert_fact_transaction_composite_key_unique.sql` vérifie
-- justement cette hypothèse contre les données réelles.
--
-- Point notable : la jointure vers DIM_CUSTOMER est une jointure
-- "point-in-time" (pas un simple id = id) — on sélectionne la version du
-- client qui était VALIDE au moment de la transaction (initiated_at compris
-- entre valid_from et valid_to), pas sa version actuelle. C'est tout
-- l'intérêt du SCD2 : une transaction de janvier doit rester associée au
-- kyc_level du client tel qu'il était en janvier, même si ce client a
-- changé de niveau depuis.
--
-- Les transactions n'ont pas de customer_id direct (seulement des MSISDN) :
-- le lien passe par sender_msisdn = dim_customer.msisdn.

with transactions as (
    select * from "panafripay"."gold_staging_dbt"."stg_transactions"
),

customer_at_transaction_time as (
    select
        t.transaction_id,
        dc.customer_key
    from transactions t
    left join "panafripay"."gold_gold"."dim_customer" dc
        on t.sender_msisdn = dc.msisdn
        and t.initiated_at >= dc.valid_from
        and (dc.valid_to is null or t.initiated_at < dc.valid_to)
)

select
    t.initiated_at::date as transaction_date,
    c.customer_key,
    da.agent_key,
    dop.operator_key,
    t.transaction_id,
    t.transaction_type,
    t.channel,
    t.amount,
    t.fees,
    t.currency,
    t.transaction_status,
    t.initiated_at,
    t.completed_at
from transactions t
left join customer_at_transaction_time c on c.transaction_id = t.transaction_id
left join "panafripay"."gold_gold"."dim_agent" da on da.agent_id = t.agent_id
left join "panafripay"."gold_gold"."dim_operator" dop on dop.operator_code = t.operator_code
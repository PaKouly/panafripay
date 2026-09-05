-- Test de non-régression : la clé primaire de FACT_TRANSACTION est la
-- combinaison (transaction_date, customer_key, agent_key, operator_key,
-- transaction_id) — transaction_id (attribut dégénéré) est inclus car les
-- seules clés étrangères des dimensions ne suffisent pas à garantir
-- l'unicité : un même client peut réaliser plusieurs transactions le même
-- jour avec le même agent et le même opérateur (constaté sur les données
-- réelles : 1 014 groupes en conflit avant cet ajustement). Un test dbt
-- "singulier" réussit quand il ne renvoie AUCUNE ligne.

select
    transaction_date,
    customer_key,
    agent_key,
    operator_key,
    transaction_id,
    count(*) as nb_transactions
from {{ ref('fact_transaction') }}
group by transaction_date, customer_key, agent_key, operator_key, transaction_id
having count(*) > 1

-- Vue mince sur la table de staging brute : renommage éventuel, pas de logique métier.
-- Toute la logique de nettoyage a déjà été appliquée en Phase 4 (couche silver, PySpark) ;
-- ce modèle sert juste d'interface stable pour les modèles gold en aval.

select
    transaction_id,
    transaction_ref,
    transaction_type,
    sender_msisdn,
    receiver_msisdn,
    agent_id,
    amount,
    currency,
    fees,
    operator_code,
    country_code,
    transaction_status,
    initiated_at,
    completed_at,
    channel,
    device_imei_hash,
    error_code,
    partner_merchant_id
from "panafripay"."staging"."stg_transactions"
-- MERCHANT RISK PROFILE
-- We use this to see which sources are bringing in the most "Chargeback Value"
SELECT 
    source,
    COUNT(user_id) as total_tx,
    SUM(is_fraud) as total_fraud_instances,
    SUM(CASE WHEN is_chargeback = 1 THEN purchase_value ELSE 0 END) as total_chargeback_value
FROM transactions
GROUP BY 1
ORDER BY 4 DESC;

-- GEOGRAPHIC MISMATCH
-- This finds countries where the average purchase value is suspicious
SELECT 
    ip_country,
    AVG(purchase_value) as avg_spend,
    AVG(velocity_seconds) as avg_speed_to_purchase
FROM transactions
WHERE is_fraud = 1
GROUP BY 1
HAVING COUNT(*) > 10
ORDER BY avg_speed_to_purchase ASC;
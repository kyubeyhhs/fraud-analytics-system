-- FRAUD RATE
SELECT 
    source,
    COUNT(*) as total_tx,
    SUM(is_fraud) as fraud_cases,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percentage
FROM transactions
GROUP BY source;

-- CHARGEBACK WIN RATE
SELECT 
    status,
    COUNT(*) as total_chargebacks,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM chargebacks), 2) as percentage
FROM chargebacks
GROUP BY status;

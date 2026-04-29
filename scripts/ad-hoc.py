import duckdb


con = duckdb.connect('warehouse/fraud_analytics.duckdb')


coverage = """
SELECT 
    ip_country, 
    COUNT(*) as tx_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM f_transactions), 2) as percentage
FROM f_transactions
GROUP BY ip_country
ORDER BY tx_count DESC;
"""
print("Data Coverage...")
print(con.execute(coverage).df())


velocity_check = """
SELECT 
    user_id, 
    velocity_seconds, 
    is_fraud
FROM f_transactions
WHERE velocity_seconds < 60
AND is_fraud = 1
LIMIT 10;
"""
print("\nFraudulent Velocity Patterns...")
print(con.execute(velocity_check).df())


risk = """
SELECT 
    ip_country, 
    COUNT(*) as total_tx,
    AVG(purchase_value) as avg_value,
    SUM(is_fraud) as total_fraud_cases
FROM f_transactions
GROUP BY ip_country
HAVING total_fraud_cases > 5
ORDER BY avg_value DESC;
"""
print("\nRisk Analysis by Country...")
print(con.execute(risk).df())
-- Calculating if a day's fraud is an outlier (Z-Score > 2)
WITH daily_fraud AS (
    SELECT CAST(purchase_time AS DATE) as date, COUNT(*) as f_count
    FROM transactions WHERE is_fraud = 1 GROUP BY 1
),
stats AS (
    SELECT date, f_count, AVG(f_count) OVER() as avg_f, STDDEV(f_count) OVER() as std_f
    FROM daily_fraud
)
SELECT *, (f_count - avg_f) / NULLIF(std_f, 0) as z_score
FROM stats WHERE ABS((f_count - avg_f) / NULLIF(std_f, 0)) > 2;
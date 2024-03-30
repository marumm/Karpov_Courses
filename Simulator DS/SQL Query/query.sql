SELECT date_trunc('week', date) as weeks, SUM(amount) as sum_receipt
FROM new_payments
GROUP BY weeks
ORDER BY weeks

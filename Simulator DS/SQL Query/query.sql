SELECT date_trunc('week', date) as week, SUM(amount) as sum_receipt
FROM new_payments
WHERE status='Confirmed'
GROUP BY week
ORDER BY week

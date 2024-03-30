SELECT date_trunc('week', date) as weeks, SUM(amount) as sum_receipt
FROM new_payments
GROUP BY weeks
ORDER BY weeks


--В этой задаче построил дашборд с временным рядом суммы оплат за неделю. На оси X - неделя, на оси Y - сумма всех успешных оплат за неделю.

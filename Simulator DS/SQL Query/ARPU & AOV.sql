--Запрос для вывода arppu (средняя прибыль с пользователя) и aov (средний чек)
SELECT date(date_trunc('month', date)) as time, SUM(amount)/COUNT(DISTINCT(email_id)) as arppu, SUM(amount)/COUNT(amount) as aov
FROM new_payments
WHERE status='Confirmed'
GROUP BY time
ORDER BY time

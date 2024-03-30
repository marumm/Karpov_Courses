SELECT toStartOfMonth(toDate(buy_date)) as month, sum(check_amount)/count(check_amount) as avg_check, (quantileExactExclusive(0.5)(check_amount)) as median_check
FROM default.view_checks
GROUP BY month



--Посчитал средний чек по месяцам и медиану по чекам. Все расчеты велись в ClickHouse

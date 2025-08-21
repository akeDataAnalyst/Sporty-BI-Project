-- Time-Series Analysis

SELECT
    DATE(bet_timestamp) AS bet_date,
    SUM(profit_loss) AS daily_profit_loss,
    COUNT(DISTINCT user_id) AS daily_active_users
FROM
    bets
GROUP BY
    bet_date
ORDER BY
    bet_date ASC;
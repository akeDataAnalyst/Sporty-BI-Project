-- A/B Testing Analysis
SELECT
    CASE 
        WHEN user_id % 2 = 0 THEN 'Control'
        ELSE 'Test'
    END AS feature_group,
    COUNT(DISTINCT user_id) AS total_users,
    SUM(profit_loss) AS total_profit_loss,
    AVG(bet_amount) AS average_bet_amount
FROM
    bets
GROUP BY
    feature_group;
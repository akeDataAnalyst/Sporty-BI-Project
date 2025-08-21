--  Key Performance Indicators (KPIs)

SELECT
    sport,
    COUNT(bet_id) AS total_bets,
    SUM(bet_amount) AS total_staked_amount,
    SUM(profit_loss) AS total_profit_loss,
    AVG(bet_amount) AS avg_bet_amount
FROM
    bets
GROUP BY
    sport
ORDER BY
    total_profit_loss DESC;

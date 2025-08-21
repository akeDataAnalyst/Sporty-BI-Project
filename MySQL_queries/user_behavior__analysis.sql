-- User Behavior Analysis
WITH UserSportRank AS (
    SELECT
        user_id,
        sport,
        COUNT(*) AS bet_count,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
    FROM
        bets
    GROUP BY
        user_id, sport
)
SELECT
    b.user_id,
    SUM(b.profit_loss) AS total_profit_loss,
    COUNT(b.bet_id) AS total_bets,
    usr.sport AS most_bet_sport
FROM
    bets AS b
JOIN
    UserSportRank AS usr ON b.user_id = usr.user_id AND usr.rn = 1
GROUP BY
    b.user_id, usr.sport
ORDER BY
    total_profit_loss DESC
LIMIT 10;
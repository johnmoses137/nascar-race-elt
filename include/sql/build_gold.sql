CREATE OR REPLACE TABLE NASCAR_RACE_ELT.GOLD.DRIVER_SEASON_SUMMARY AS
SELECT
    r.race_season,
    rr.driver_id,
    MAX(rr.driver_name) AS driver_name,
    COUNT(*) AS starts,
    SUM(CASE WHEN rr.finishing_position = 1 THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN rr.finishing_position BETWEEN 1 AND 5 THEN 1 ELSE 0 END)
        AS top_five_finishes,
    ROUND(AVG(CASE
        WHEN rr.finishing_position > 0 THEN rr.finishing_position
    END), 2) AS average_finish
FROM NASCAR_RACE_ELT.SILVER.RACE_RESULTS AS rr
JOIN NASCAR_RACE_ELT.SILVER.RACES AS r
    ON rr.race_id = r.race_id
WHERE rr.starting_position > 0
GROUP BY r.race_season, rr.driver_id;
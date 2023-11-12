SELECT year, SUM(births) AS total_birth
FROM (
    SELECT year, births
    FROM birth2000_delta b2000
    UNION ALL
    SELECT year, births
    FROM birth1994_delta b1994
) AS merged_tables 
GROUP BY year
ORDER BY year;
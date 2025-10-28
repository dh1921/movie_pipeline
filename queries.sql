

USE movie_db;

-- Which movie has the highest average rating?

SELECT title, rating
FROM movies
WHERE rating IS NOT NULL
ORDER BY rating DESC
LIMIT 1;


-- What are the top 5 movie genres that have the highest average rating?

SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(genres, ',', n.n), ',', -1)) AS genre,
ROUND(AVG(rating), 2) AS avg_rating
FROM movies
JOIN (
        SELECT a.N + b.N * 10 + 1 AS n
        FROM 
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
    ) n
WHERE n.n <= 1 + LENGTH(genres) - LENGTH(REPLACE(genres, ',', ''))
GROUP BY genre
ORDER BY avg_rating DESC
LIMIT 5;


-- Who is the director with the most movies in this dataset?

SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director <> 'Unknown'
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;


-- What is the average rating of movies released each year?

SELECT year,ROUND(AVG(rating), 2) AS avg_rating
FROM movies
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year;


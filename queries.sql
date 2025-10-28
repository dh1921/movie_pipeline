

USE movie_db;

-- Which movie has the highest average rating?

SELECT title, rating
FROM (
    SELECT title, rating,
	ROW_NUMBER() OVER (ORDER BY rating DESC) AS row_num
    FROM movies
    WHERE rating IS NOT NULL
) ranked
WHERE row_num = 1;


-- What are the top 5 movie genres that have the highest average rating?

WITH genre_avg AS (
    SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(genres, ',', n.n), ',', -1)) AS genre,
	ROUND(AVG(rating), 2) AS avg_rating
    FROM movies
    JOIN (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    ) n ON n.n <= 1 + LENGTH(genres) - LENGTH(REPLACE(genres, ',', ''))
    WHERE genres IS NOT NULL AND genres <> ''
    GROUP BY genre
)
SELECT genre, avg_rating
FROM (
    SELECT genre, avg_rating,
        DENSE_RANK() OVER (ORDER BY avg_rating DESC) AS rank_num
    FROM genre_avg
) ranked
WHERE rank_num <= 5;




-- Who is the director with the most movies in this dataset?

SELECT director, movie_count
FROM (
    SELECT director,COUNT(*) AS movie_count,
	ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_num
    FROM movies
    WHERE director IS NOT NULL AND director <> 'Unknown'
    GROUP BY director
) ranked
WHERE row_num = 1;



-- What is the average rating of movies released each year?

SELECT DISTINCT year,
ROUND(AVG(rating) OVER (PARTITION BY year), 2) AS avg_rating
FROM movies
WHERE year IS NOT NULL
ORDER BY year;


create database movie_db;

-- connect this database with jupyter notebook 

USE movie_db;

CREATE TABLE IF NOT EXISTS movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    year INT,
    director VARCHAR(255) DEFAULT 'Unknown',
    rating DECIMAL(3,1),
    genres VARCHAR(255),
    decade INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ratings (
    userId INT NOT NULL,
    movieId INT NOT NULL,
    rating DECIMAL(2,1) NOT NULL,
    timestamp BIGINT,
    PRIMARY KEY (userId, movieId),
    FOREIGN KEY (movieId) REFERENCES movies(movie_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);


select * from movies;

drop database movie_db;


# Movie Data Pipeline

This project implements a complete **ETL (Extract, Transform, Load)** pipeline that integrates data from the **MovieLens dataset** and the **OMDb API**, cleans and enriches it, and loads it into a **MySQL database** for analysis.  
The entire workflow was developed and tested in **Jupyter Notebook**.

---

## Overview

The goal of this project is to design and implement a data pipeline that:
1. Extracts movie and rating data from CSV files and the OMDb API.  
2. Transforms and enriches the data using Python and pandas.  
3. Loads the processed data into a MySQL database.  
4. Executes analytical SQL queries to answer movie-related questions.

---



---

## ⚙️ Environment Setup

### 1. Prerequisites
Make sure you have the following installed:
- **Python 3.9+**
- **MySQL Server** (with username and password)
- **Jupyter Notebook** (for development and testing)
- **pip** (Python package installer)

---

### 2. Create and Configure MySQL Database

1. Open MySQL Workbench or terminal.  
2. Create a new database:
   ```sql
   CREATE DATABASE movie_db;
   USE movie_db;
   
### 3. Install Python Dependencies
Install all required libraries using:
```python
pip install jupyter pandas sqlalchemy requests pymysql
```
### 4.Verify Connection Between MySQL and Jupyter Notebook
Test your MySQL connection using the following code inside Jupyter Notebook:
```
from sqlalchemy import create_engine, text

engine = create_engine("mysql+pymysql://root:1921@localhost:3306/movie_db")

with engine.connect() as conn:
    result = conn.execute(text("SELECT DATABASE();"))
    print(" Connected to database:", result.scalar())
```
### 5. Load the CSV Files in Jupyter Notebook
Loading the csv files
```
import pandas as pd

movies_df = pd.read_csv("movies.csv")
ratings_df = pd.read_csv("ratings.csv")

print("Movies shape:", movies_df.shape)
print("Ratings shape:", ratings_df.shape)

display(movies_df.head())
display(ratings_df.head())
```

### 6. Obtain OMDb API Key

1.Visit the OMDb API website.

2.Register for a free API key.

3.Save your key in your code as:
```
OMDB_API_KEY = "your_api_key_here"
```

### 7. Verify Your OMDb API Key

Test your API connection with the following code:
```
#Extrenal Api

import requests

API_KEY = "8f9c2fcb"
title = "Inception"

url = f"http://www.omdbapi.com/?t={title}&apikey={API_KEY}"
response = requests.get(url)
data = response.json()
print(data)

```
### How to Run the Project
Follow these steps to execute the complete ETL pipeline.

1. Run the Schema Script
Make sure you already have a MySQL database named movie_db created.
If not, create it once:
```
CREATE DATABASE movie_db;
```

Then, run the schema.sql script to create the necessary tables inside movie_db.

2. Run the ETL Script
Once the tables are created, run your ETL pipeline:
```
python etl.py
```
This will:

Extract data from the CSV files (movies.csv, ratings.csv)

Fetch additional details from the OMDb API

Transform and clean the data using pandas

Load the final dataset into your MySQL database (movie_db)

3.Verify Loaded Data
Open MySQL and check if your data has been successfully loaded:
```
USE movie_db;
SHOW TABLES;
SELECT COUNT(*) FROM movies;
SELECT COUNT(*) FROM ratings;
```
### Design Choices & Assumptions
 Design Choices

1.Database:

- Used MySQL for reliability, relational modeling, and easy integration with SQLAlchemy.

- Tables are designed to store movies, ratings, and enriched metadata such as director and genres.

2.Python Libraries:

- pandas for CSV ingestion and data transformation.

- requests for calling the OMDb API.

- SQLAlchemy + PyMySQL for database connectivity and idempotent inserts.

- json for simple caching of OMDb API responses (omdb_cache.json) to avoid redundant API calls.

3.ETL Flow:

Extract: Read movies.csv and ratings.csv.

Transform:

- Parse movie titles and extract release years.

- Split genres (|-separated) into a comma-separated format.

- Fetch additional data (e.g., Director) from OMDb.

- Compute average ratings per movie.

- Add a derived decade column for analytical use.

Load: Insert data into MySQL tables using ON DUPLICATE KEY UPDATE for idempotency (safe to re-run ETL multiple times).

4. OMDb API Integration:

- Each movie’s title (and year, if available) is used to fetch details from OMDb.

- Results are cached locally (omdb_cache.json) to minimize API calls and handle rate limits gracefully.

5. Jupyter Notebook:

- Used for testing connectivity, verifying dataset loads, and checking transformations interactively.

6. Performance Choice:

- Introduced SAMPLE_MODE = True to limit the dataset to 20 movies for quick testing; can be set to False for full dataset runs.

### Assumptions

1. Database Configuration:

- The MySQL database movie_db already exists and is accessible at localhost:3306 with user and password .

- Tables are pre-created by running schema.sql before executing etl.py.

2. Data Files:

- movies.csv and ratings.csv are placed in the same directory as etl.py.

- These files are from the MovieLens small dataset.

3. OMDb API:

- A valid API key is required and stored directly in the script as OMDB_API_KEY.

- API responses may sometimes lack certain fields (e.g., missing directors) — such entries default to "Unknown".

4. Data Integrity:

- Titles in MovieLens may not exactly match OMDb titles; if no match is found, the record is still inserted with missing API fields.

- Duplicate inserts are prevented by using ON DUPLICATE KEY UPDATE logic.

5. Reproducibility:

- The ETL process is idempotent — multiple runs update existing rows instead of creating duplicates.


### The Challenge I Faced is API Data Mismatches, 
some movie titles in the MovieLens dataset did not match exactly with OMDb API records (due to alternate titles, missing years, or spelling variations).

Solution:
Implemented a custom title parsing function (parse_title_and_year) to extract movie titles and release years separately.
If the OMDb API returned "Response": "False", the script handled it gracefully by storing "Unknown" for missing fields like director or plot instead of breaking the ETL process.



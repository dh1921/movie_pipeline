"""

ETL pipeline: MovieLens (movies.csv, ratings.csv) + OMDb enrichment -> MySQL

"""

import os
import re
import time
import json
import pandas as pd
import requests
from sqlalchemy import create_engine, text


# Configuration              

DB_USER = "root"
DB_PASS = "1921"  
DB_HOST = "localhost"
DB_PORT = 3306
DB_NAME = "movie_db"
OMDB_API_KEY = "8f9c2fcb"  

MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"
OMDB_CACHE_FILE = "omdb_cache.json"
SAMPLE_MODE = True


# -----------------------------
# Mysql Database connection
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -----------------------------


def parse_title_and_year(title):
    m = re.search(r"(.+)\s+\((\d{4})\)$", str(title))
    if m:
        return m.group(1).strip(), int(m.group(2))
    return str(title).strip(), None


def parse_genres(genres_str):
    if pd.isna(genres_str):
        return []
    return [g.strip() for g in genres_str.split("|") if g.lower() != "(no genres listed)"]


def compute_decade(year):
    return (year // 10) * 10 if isinstance(year, (int, float)) and not pd.isna(year) else None


def fetch_omdb(title, year=None):
    key = f"{title.lower()}::{year}"
    if key in omdb_cache:
        return omdb_cache[key]

    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = year

    try:
        resp = requests.get("http://www.omdbapi.com/", params=params, timeout=10)
        data = resp.json()
    except Exception as e:
        data = {"Response": "False", "Error": str(e)}

    omdb_cache[key] = data
    save_cache(omdb_cache)
    time.sleep(0.2)
    return data


def load_cache():
    return json.load(open(OMDB_CACHE_FILE)) if os.path.exists(OMDB_CACHE_FILE) else {}


def save_cache(cache):
    with open(OMDB_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


omdb_cache = load_cache()

# -----------------------------
# Extract
# ----------------------------
def extract_data():
    movies = pd.read_csv(MOVIES_CSV)
    ratings = pd.read_csv(RATINGS_CSV)
    if SAMPLE_MODE:
        movies = movies.sample(20, random_state=1)
        ratings = ratings[ratings["movieId"].isin(movies["movieId"])]
    return movies, ratings

# -----------------------------
# Transform
# -----------------------------
def transform_data(movies_df, ratings_df):
    avg_ratings = ratings_df.groupby("movieId")["rating"].mean().reset_index()

    records = []
    for _, row in movies_df.iterrows():
        mid = int(row["movieId"])
        title, year = parse_title_and_year(row["title"])
        genres = parse_genres(row["genres"])

        omdb = fetch_omdb(title, year)
        director = omdb.get("Director") if omdb.get("Response") == "True" else None
        if not director or director.strip() == "N/A":
            director = "Unknown"

        rating_row = avg_ratings.loc[avg_ratings["movieId"] == mid, "rating"]
        rating = round(float(rating_row.iloc[0]), 1) if not rating_row.empty else None

        records.append({
            "movie_id": mid,
            "title": title,
            "year": year,
            "director": director,
            "rating": rating,
            "genres": ", ".join(genres),
        })

    df = pd.DataFrame(records)
    df["director"] = df["director"].fillna("Unknown")
    df["decade"] = df["year"].apply(compute_decade)
    return df

# -----------------------------
# Load
# -----------------------------
def load_data(movies_df, ratings_df):
    with engine.begin() as conn:
        for _, row in movies_df.iterrows():
            conn.execute(text("""
                INSERT INTO movies (movie_id, title, year, director, rating, genres, decade)
                VALUES (:movie_id, :title, :year, :director, :rating, :genres, :decade)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    year = VALUES(year),
                    director = VALUES(director),
                    rating = VALUES(rating),
                    genres = VALUES(genres),
                    decade = VALUES(decade);
            """), row.to_dict())

        
        for _, row in ratings_df.iterrows():
            conn.execute(text("""
                INSERT INTO ratings (userId, movieId, rating, timestamp)
                VALUES (:userId, :movieId, :rating, :timestamp)
                ON DUPLICATE KEY UPDATE
                    rating = VALUES(rating),
                    timestamp = VALUES(timestamp);
            """), row.to_dict())

    print("Data loaded successfully")



def run_etl():
    print(" Extracting data...")
    movies, ratings = extract_data()

    print(" Transforming data...")
    movies_clean = transform_data(movies, ratings)

    print("Loading data to MySQL...")
    load_data(movies_clean, ratings)

    print(" ETL complete.")

if __name__ == "__main__":
    if OMDB_API_KEY.startswith("YOUR") or DB_PASS.startswith("YOUR"):
        print(" Please update your credentials and API key before running.")
    else:
        run_etl()

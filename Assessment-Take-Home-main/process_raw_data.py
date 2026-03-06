"""A script to process book data."""

import pandas as pd
import sqlite3


def get_db_connection():
    '''
    Returns an sqlite3 connection to the authors database
    '''
    conn = sqlite3.connect('data/authors.db')
    return conn


def load_csv(filepath: str, conn):
    '''
    Loads a RAW_DATA .csv file, extracts relevant columns, 
    joins with author data from authors.db, and returns cleaned data.

    Args:
        filepath: Path to a RAW_DATA .csv file

    Returns:
        DataFrame with columns: title, author_name, year, rating, ratings
        - title and author_name are text (object dtype)
        - year, rating, ratings are numeric
    '''
    # Load the raw CSV
    df = pd.read_csv(filepath)

    # Select and rename the relevant columns
    df = df[['book_title', 'author_id',
             'Year released', 'Rating', 'ratings']].copy()
    df.columns = ['title', 'author_id', 'year', 'rating', 'ratings']

    # Load author names from the SQLite database
    authors_df = pd.read_sql_query('SELECT id, name FROM author', conn)

    # Rename the id column to author_id for the merge
    authors_df.columns = ['author_id', 'author_name']

    # Merge with author data to get author names
    df = df.merge(authors_df, on='author_id', how='left')

    # Drop the author_id column (no longer needed)
    df = df.drop('author_id', axis=1)

    # Clean and convert data types
    # Convert rating: remove commas and convert to float
    df['rating'] = df['rating'].str.replace(',', '.').astype(float)

    # Convert year and ratings to numeric
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    # Remove backticks from ratings column before converting to numeric
    df['ratings'] = df['ratings'].str.replace('`', '').astype(float)

    # Remove bracketed content from titles (e.g., "(Paperback)")
    df['title'] = df['title'].str.replace(
        r'\s*\(.*?\)', '', regex=True).str.strip()

    # Reorder columns to match expected output
    df = df[['title', 'author_name', 'year', 'rating', 'ratings']]

    # Drop rows without a title
    df = df.dropna(subset=['title', 'author_name'])

    return df


if __name__ == "__main__":
    conn = get_db_connection()
    processed_data = load_csv('data/RAW_DATA_0.csv', conn)
    print(processed_data.head())

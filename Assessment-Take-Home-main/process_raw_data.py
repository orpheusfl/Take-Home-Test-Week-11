"""A script to process book data."""

import pandas as pd
import sqlite3
import sys


def get_db_connection() -> sqlite3.Connection:
    '''
    Returns an sqlite3 connection to the authors database
    '''
    conn = sqlite3.connect('data/authors.db')
    return conn


def load_csv(filepath: str, conn: sqlite3.Connection) -> pd.DataFrame:
    '''
    Loads a RAW_DATA_<n>.csv file, extracts relevant columns, 
    joins with author data from authors.db, and returns cleaned data.

    Args:
        filepath: Path to a RAW_DATA_<n>.csv file

    Returns:
        DataFrame with columns: title, author_name, year, rating, ratings
        - title and author_name are text (object dtype)
        - year, rating, ratings are numeric
    '''
    # Load csv and rename columns
    df = pd.read_csv(filepath)
    df = df[['book_title', 'author_id',
             'Year released', 'Rating', 'ratings']].copy()
    df.columns = ['title', 'author_id', 'year', 'rating', 'ratings']

    # Load author names from the SQLite database
    authors_df = pd.read_sql_query('SELECT id, name FROM author', conn)
    authors_df.columns = ['author_id', 'author_name']

    # Merge the book data with author names
    df = df.merge(authors_df, on='author_id', how='left')
    # Author ID is no longer needed after merging
    df = df.drop('author_id', axis=1)

    # Converting columns to suitable numeric datatypes
    df['rating'] = df['rating'].str.replace(',', '.').astype(float)
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['ratings'] = df['ratings'].str.replace('`', '').astype(int)

    # Remove bracketed content from titles
    df['title'] = df['title'].str.replace(
        r'\s*\(.*?\)', '', regex=True).str.strip()

    # Drop rows without a title or author name
    df = df.dropna(subset=['title', 'author_name'])

    # Reorder columns to match expected output
    df = df[['title', 'author_name', 'year', 'rating', 'ratings']]

    # Sort by rating in descending order
    df = df.sort_values('rating', ascending=False)

    return df


def main(args: list[str]) -> None:
    '''
    Main CLI function to process raw data files.

    Args:
        args: Command line arguments (should include filepath as first argument)
    '''
    if len(args) != 2:
        print(
            "Program should be run with python3 process_raw_data.py <path to raw data csv>")
        sys.exit(1)

    filepath = args[1]
    conn = get_db_connection()
    processed_data = load_csv(filepath, conn)
    processed_data.to_csv('data/PROCESSED_DATA.csv', index=False)
    print(
        f"Successfully processed {filepath} and saved to data/PROCESSED_DATA.csv")


if __name__ == "__main__":
    main(sys.argv)

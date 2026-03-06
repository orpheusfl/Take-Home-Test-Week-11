"""A script to process book data."""

import pandas as pd
import sqlite3
import sys
import requests
from typing import Optional


def get_db_connection() -> sqlite3.Connection:
    '''
    Returns an sqlite3 connection to the authors database
    '''
    conn = sqlite3.connect('data/authors.db')
    return conn


def lookup_book_by_author_and_year(author_name: str, year: int) -> Optional[str]:
    '''
    Look up a book by author name and year using Open Library API.
    
    Args:
        author_name: Name of the author
        year: Year the book was published
        
    Returns:
        The title of the book if exactly one match found, None otherwise
    '''
    try:
        # Search for books by this author in the specified year
        url = "https://openlibrary.org/search.json"
        params = {
            "author": author_name,
            "first_publish_year": year,
            "limit": 10
        }
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code != 200:
            return None
            
        data = response.json()
        docs = data.get('docs', [])
        
        # Filter to only books published in the exact year
        matching_books = [
            doc for doc in docs 
            if doc.get('first_publish_year') == year and doc.get('title')
        ]
        
        # Return title only if exactly one match
        if len(matching_books) == 1:
            return matching_books[0]['title']
            
        return None
    except (requests.RequestException, KeyError, ValueError):
        return None


def lookup_book_by_title_and_year(title: str, year: int) -> Optional[str]:
    '''
    Look up an author by book title and year using Open Library API.
    
    Args:
        title: Title of the book
        year: Year the book was published
        
    Returns:
        The author name if exactly one match found, None otherwise
    '''
    try:
        # Search for books with this title in the specified year
        url = "https://openlibrary.org/search.json"
        params = {
            "title": title,
            "first_publish_year": year,
            "limit": 10
        }
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code != 200:
            return None
            
        data = response.json()
        docs = data.get('docs', [])
        
        # Filter to only books published in the exact year
        matching_books = [
            doc for doc in docs 
            if doc.get('first_publish_year') == year and doc.get('author_name')
        ]
        
        # Return author name only if exactly one match
        if len(matching_books) == 1:
            authors = matching_books[0].get('author_name', [])
            if authors:
                return authors[0] if isinstance(authors, list) else authors
                
        return None
    except (requests.RequestException, KeyError, ValueError):
        return None


def fill_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Attempt to fill missing titles or author names using Open Library API.
    Rows that still have missing data after API lookup are dropped.
    
    Args:
        df: DataFrame with potential missing titles or author_names
        
    Returns:
        DataFrame with fewer missing values and rows with unfilled missing data dropped
    '''
    # Find rows with missing titles
    missing_title_mask = df['title'].isna()
    for idx in df[missing_title_mask].index:
        author = df.loc[idx, 'author_name']
        year = df.loc[idx, 'year']
        
        # Only try API lookup if we have author and year
        if pd.notna(author) and pd.notna(year):
            title = lookup_book_by_author_and_year(author, int(year))
            if title:
                df.loc[idx, 'title'] = title
                print(f"Found title via API for {author} ({int(year)}): {title}")
    
    # Find rows with missing author names
    missing_author_mask = df['author_name'].isna()
    for idx in df[missing_author_mask].index:
        title = df.loc[idx, 'title']
        year = df.loc[idx, 'year']
        
        # Only try API lookup if we have title and year
        if pd.notna(title) and pd.notna(year):
            author = lookup_book_by_title_and_year(title, int(year))
            if author:
                df.loc[idx, 'author_name'] = author
                print(f"Found author via API for {title} ({int(year)}): {author}")
    
    # Drop rows that still have missing title or author name
    initial_count = len(df)
    df = df.dropna(subset=['title', 'author_name'])
    dropped_count = initial_count - len(df)
    
    if dropped_count > 0:
        print(f"Dropped {dropped_count} rows with unfilled missing data")
    
    return df


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

    # Try to fill missing titles or author names using API lookup
    df = fill_missing_data(df)

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

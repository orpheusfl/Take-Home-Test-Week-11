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


def _call_api(url: str, params: dict) -> Optional[dict]:
    '''
    Helper to make API request and handle errors.

    Args:
        url: API endpoint URL
        params: Query parameters

    Returns:
        Parsed JSON response or None if request failed
    '''
    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code != 200:
            return None
        return response.json()
    except (requests.RequestException, ValueError):
        return None


def _filter_docs_by_year(docs: list, year: int) -> list:
    '''
    Filter API documents to only those published in the specified year.

    Args:
        docs: List of documents from API response
        year: Target publication year

    Returns:
        List of filtered documents
    '''
    return [
        doc for doc in docs
        if doc.get('first_publish_year') == year
    ]


def _get_single_result_or_none(results: list, key: str) -> Optional[str]:
    '''
    Extract a value from results list if exactly one result exists.

    Args:
        results: List of matching results
        key: Key to extract from the result

    Returns:
        The extracted value if results has exactly one entry, None otherwise
    '''
    if len(results) != 1:
        return None

    value = results[0].get(key)

    # Handle cases where value might be a list
    if isinstance(value, list):
        return value[0] if value else None
    return value


def lookup_title_by_author_and_year(author_name: str, year: int) -> Optional[str]:
    '''
    Look up a book title by author name and publication year using Open Library API.

    Args:
        author_name: Name of the author
        year: Year the book was published

    Returns:
        The title if exactly one match found, None otherwise
    '''
    url = "https://openlibrary.org/search.json"
    params = {
        "author": author_name,
        "first_publish_year": year,
        "limit": 10
    }

    data = _call_api(url, params)
    if not data:
        return None

    docs = data.get('docs', [])
    matching_books = _filter_docs_by_year(docs, year)
    return _get_single_result_or_none(matching_books, 'title')


def lookup_author_by_title_and_year(title: str, year: int) -> Optional[str]:
    '''
    Look up an author by book title and publication year using Open Library API.

    Args:
        title: Title of the book
        year: Year the book was published

    Returns:
        The author name if exactly one match found, None otherwise
    '''
    url = "https://openlibrary.org/search.json"
    params = {
        "title": title,
        "first_publish_year": year,
        "limit": 10
    }

    data = _call_api(url, params)
    if not data:
        return None

    docs = data.get('docs', [])
    matching_books = _filter_docs_by_year(docs, year)
    return _get_single_result_or_none(matching_books, 'author_name')


def _load_raw_csv(filepath: str) -> pd.DataFrame:
    '''
    Load raw CSV file and rename columns.

    Args:
        filepath: Path to raw CSV file

    Returns:
        DataFrame with renamed columns
    '''
    df = pd.read_csv(filepath)
    df = df[['book_title', 'author_id',
             'Year released', 'Rating', 'ratings']].copy()
    df.columns = ['title', 'author_id', 'year', 'rating', 'ratings']
    return df


def _load_authors_from_db(conn: sqlite3.Connection) -> pd.DataFrame:
    '''
    Load author data from SQLite database.

    Args:
        conn: SQLite database connection

    Returns:
        DataFrame with author_id and author_name columns
    '''
    authors_df = pd.read_sql_query('SELECT id, name FROM author', conn)
    authors_df.columns = ['author_id', 'author_name']
    return authors_df


def _merge_with_authors(df: pd.DataFrame, authors_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Merge book data with author names from database.

    Args:
        df: Book data DataFrame
        authors_df: Author data DataFrame

    Returns:
        Merged DataFrame without author_id column
    '''
    df = df.merge(authors_df, on='author_id', how='left')
    df = df.drop('author_id', axis=1)
    return df


def _convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Convert columns to appropriate numeric data types.

    Args:
        df: DataFrame with mixed data types

    Returns:
        DataFrame with converted types
    '''
    df['rating'] = df['rating'].str.replace(',', '.').astype(float)
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['ratings'] = df['ratings'].str.replace('`', '').astype(int)
    return df


def _clean_titles(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Remove bracketed content from book titles.

    Args:
        df: DataFrame with titles to clean

    Returns:
        DataFrame with cleaned titles
    '''
    df['title'] = df['title'].str.replace(
        r'\s*\(.*?\)', '', regex=True).str.strip()
    return df


def _fill_missing_titles(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Attempt to fill missing titles using author name and year via API.

    Args:
        df: DataFrame with potential missing titles

    Returns:
        DataFrame with filled titles where possible
    '''
    missing_mask = df['title'].isna()
    for idx in df[missing_mask].index:
        author = df.loc[idx, 'author_name']
        year = df.loc[idx, 'year']

        if pd.notna(author) and pd.notna(year):
            title = lookup_title_by_author_and_year(author, int(year))
            if title:
                df.loc[idx, 'title'] = title
                print(
                    f"Found title via API for {author} ({int(year)}): {title}")

    return df


def _fill_missing_authors(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Attempt to fill missing authors using title and year via API.

    Args:
        df: DataFrame with potential missing authors

    Returns:
        DataFrame with filled authors where possible
    '''
    missing_mask = df['author_name'].isna()
    for idx in df[missing_mask].index:
        title = df.loc[idx, 'title']
        year = df.loc[idx, 'year']

        if pd.notna(title) and pd.notna(year):
            author = lookup_author_by_title_and_year(title, int(year))
            if author:
                df.loc[idx, 'author_name'] = author
                print(
                    f"Found author via API for {title} ({int(year)}): {author}")

    return df


def _drop_incomplete_rows(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Drop rows with missing title or author name.

    Args:
        df: DataFrame to clean

    Returns:
        DataFrame without incomplete rows
    '''
    initial_count = len(df)
    df = df.dropna(subset=['title', 'author_name'])
    dropped_count = initial_count - len(df)

    if dropped_count > 0:
        print(f"Dropped {dropped_count} rows with missing data")

    return df


def _finalize_data(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Reorder columns and sort by rating.

    Args:
        df: Processed DataFrame

    Returns:
        Finalized DataFrame with correct column order and sorting
    '''
    df = df[['title', 'author_name', 'year', 'rating', 'ratings']]
    df = df.sort_values('rating', ascending=False)
    return df


def load_csv(filepath: str, conn: sqlite3.Connection) -> pd.DataFrame:
    '''
    Load a RAW_DATA_<n>.csv file, clean, and enrich with author data.

    Args:
        filepath: Path to a RAW_DATA_<n>.csv file
        conn: SQLite database connection

    Returns:
        Processed DataFrame with columns: title, author_name, year, rating, ratings
    '''
    df = _load_raw_csv(filepath)
    authors_df = _load_authors_from_db(conn)
    df = _merge_with_authors(df, authors_df)
    df = _convert_data_types(df)
    df = _clean_titles(df)
    df = _fill_missing_titles(df)
    df = _fill_missing_authors(df)
    df = _drop_incomplete_rows(df)
    df = _finalize_data(df)
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

"""Produces a chart showing the top 20 words found in book titles, excluding common stop words.
"""

import pandas as pd
import requests
import re
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns

# Cache for stopword API lookups
_stopword_cache = {}


def is_stopword(word: str) -> bool:
    """
    Query the dictionaryapi to check if a word is a stopword.
    Stopwords include: conjunctions, prepositions, articles, pronouns, and determiners.

    Args:
        word: The word to check

    Returns:
        True if the word is a stopword, False otherwise
    """
    word_lower = word.lower().strip()

    # Check cache first
    if word_lower in _stopword_cache:
        return _stopword_cache[word_lower]

    try:
        url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{word_lower}"
        response = requests.get(url, timeout=5)

        # If the word is not found in the API, assume it's not a stopword
        if response.status_code != 200:
            _stopword_cache[word_lower] = False
            return False

        data = response.json()

        # Stopword parts of speech to filter out
        stopword_pos = ['conjunction', 'preposition',
                        'article', 'pronoun', 'determiner']
        for entry in data:
            if 'meanings' in entry:
                for meaning in entry['meanings']:
                    if 'partOfSpeech' in meaning:
                        if meaning['partOfSpeech'].lower() in stopword_pos:
                            _stopword_cache[word_lower] = True
                            return True

        _stopword_cache[word_lower] = False
        return False
    except (requests.RequestException, ValueError, KeyError):
        # If there's an error querying the API, assume it's not a stopword
        _stopword_cache[word_lower] = False
        return False


def get_keywords():
    """Extract top 20 keywords from book titles, excluding stopwords."""
    # Load the processed data
    df = pd.read_csv('data/PROCESSED_DATA.csv')

    # Extract all words from titles
    all_words = []
    for title in df['title'].dropna():
        # Convert to lowercase and split into words
        words = re.findall(r'\b[a-zA-Z]+\b', title.lower())
        all_words.extend(words)

    # Count all word frequencies
    word_counts = Counter(all_words)

    # Get words ordered by frequency (most frequent first)
    words_by_frequency = word_counts.most_common()

    # Find top 20 non-stopwords by going through frequency-ordered list
    top_20_non_stopwords = []
    for word, count in words_by_frequency:
        if not is_stopword(word):
            top_20_non_stopwords.append((word, count))
            if len(top_20_non_stopwords) >= 20:
                break

    # Create a dataframe for seaborn
    words, counts = zip(*top_20_non_stopwords)
    plot_df = pd.DataFrame({'Word': words, 'Frequency': counts})

    # Create the chart using seaborn
    plt.figure(figsize=(12, 7))
    sns.barplot(data=plot_df, y='Word', x='Frequency', orient='h')
    plt.xlabel('Frequency', fontsize=12)
    plt.ylabel('Words', fontsize=12)
    plt.title('Top 20 Keywords in Book Titles (Excluding Stopwords)',
              fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('top_keywords_chart.png', dpi=300, bbox_inches='tight')
    plt.show()

    print("\nTop 20 Keywords (excluding stopwords):")
    for word, count in top_20_non_stopwords:
        print(f"  {word}: {count}")


if __name__ == '__main__':
    get_keywords()

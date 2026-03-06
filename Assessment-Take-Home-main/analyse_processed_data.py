"""A script to analyse book data."""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the processed data
df = pd.read_csv('data/PROCESSED_DATA.csv')

# Create a decade column for books, grouping pre-2000 into one category


def categorize_decade(year):
    decade = (year // 10 * 10)
    if decade < 2000:
        return 'Pre-2000'
    elif decade < 2010:
        return '2000s'
    elif decade < 2020:
        return '2010s'
    else:
        return '2020s'


df['decade_group'] = df['year'].apply(categorize_decade)


def plot_books_by_decade():
    """Create a pie chart showing the proportion of books released in each decade."""
    fig, ax = plt.subplots(figsize=(10, 8))
    decade_counts = df['decade_group'].value_counts()

    # Defining the data to be used
    decade_order = ['Pre-2000', '2000s', '2010s', '2020s']
    decade_counts = decade_counts.reindex(
        [d for d in decade_order if d in decade_counts.index])

    # Creating the chart object
    colors = sns.color_palette('mako', len(decade_counts))
    ax.pie(decade_counts.values, labels=decade_counts.index,
           autopct='%1.1f%%', colors=colors, startangle=90)
    ax.set_title('Proportion of Books Released in Each Decade',
                 fontsize=14, fontweight='bold')

    plt.tight_layout()
    plt.savefig('books_by_decade_pie.png', dpi=300, bbox_inches='tight')
    plt.show()


def plot_top_authors():
    """Create a bar chart showing the total number of ratings for the ten most rated authors."""
    fig, ax = plt.subplots(figsize=(12, 6))

    # Defining the data to be used
    author_ratings = df.groupby('author_name')[
        'ratings'].sum().sort_values(ascending=False).head(10)

    # Creating the chart object
    colors = sns.color_palette('viridis', len(author_ratings))
    ax.barh(range(len(author_ratings)), author_ratings.values, color=colors)
    ax.set_yticks(range(len(author_ratings)))
    ax.set_yticklabels(author_ratings.index)
    ax.set_xlabel('Total Number of Ratings', fontsize=12, fontweight='bold')
    ax.set_ylabel('Author', fontsize=12, fontweight='bold')
    ax.set_title('Top 10 Most Rated Authors', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    # Adding corresponding author names to the end of each bar
    for i, v in enumerate(author_ratings.values):
        ax.text(v, i, f' {v:,.0f}', va='center')

    plt.tight_layout()
    plt.savefig('top_10_authors_ratings.png', dpi=300, bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    plot_books_by_decade()
    plot_top_authors()

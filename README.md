## Book processing files
### Contents:
##### process_raw_data.py
Used to take a csv file containing raw, messy data on books and clean it. The program will detect if books are missing either a title or an author and will use an API call to find books matching the year and author/title (whichever is present). If exactly one result is found, it will be inserted into the data.

The program links to an SQLite database storing details on authors, and merges this with the csv file to produce a dataframe containing book title, author name, release year, average rating, and number of ratings. This is then saved locally in .csv format

##### analyse_processed_data.py
Produces seaborn plots showing the frequencies of releases in each decade, and the number of ratings received by the top 10 most frequently rated authors. Requires data from process_raw_data.py

##### get_keywords.py
Analyses the data produced by process_raw_data.py to produce a bar chart of the top 20 most frequently used words in the processed data. Common stopwords are excluded from this. The differentiation between stopwords and non-stopwords is made using the dictionary API, which allows the program to exclude any word with a definition under one of the following categories: 'conjunction', 'preposition', 'article', 'pronoun', 'determiner'

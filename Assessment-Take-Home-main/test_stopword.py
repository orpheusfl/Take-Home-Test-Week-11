import requests

stopword_cache = {}


def is_stopword(word: str) -> bool:
    word_lower = word.lower().strip()
    if word_lower in stopword_cache:
        return stopword_cache[word_lower]

    try:
        url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{word_lower}"
        response = requests.get(url, timeout=5)

        if response.status_code != 200:
            stopword_cache[word_lower] = False
            return False

        data = response.json()

        stopword_pos = ['conjunction', 'preposition',
                        'article', 'pronoun', 'determiner']

        print(f"\nChecking '{word_lower}':")
        print(f"  Number of entries: {len(data)}")

        for i, entry in enumerate(data):
            print(f"  Entry {i}:")
            if 'meanings' in entry:
                print(f"    Has meanings: {len(entry['meanings'])} meanings")
                for j, meaning in enumerate(entry['meanings']):
                    if 'partOfSpeech' in meaning:
                        pos = meaning['partOfSpeech'].lower()
                        print(f"      Meaning {j}: partOfSpeech = '{pos}'")
                        if pos in stopword_pos:
                            print(f"        -> MATCH!")
                            stopword_cache[word_lower] = True
                            return True

        stopword_cache[word_lower] = False
        return False
    except Exception as e:
        print(f"Error: {e}")
        stopword_cache[word_lower] = False
        return False


# Test it
for test_word in ['of', 'the', 'love', 'and', 'up', 'they']:
    result = is_stopword(test_word)
    print(f"Result for '{test_word}': {result}")

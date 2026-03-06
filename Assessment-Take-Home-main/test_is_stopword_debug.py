import requests


def test_is_stopword(word: str) -> bool:
    try:
        word_lower = word.lower().strip()
        url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{word_lower}"
        response = requests.get(url, timeout=5)

        if response.status_code != 200:
            print(f"  API error: {response.status_code}")
            return False

        data = response.json()

        stopword_pos = ['conjunction', 'preposition',
                        'article', 'pronoun', 'determiner']

        print(f"  Found {len(data)} entry/entries")
        for i, entry in enumerate(data):
            if 'meanings' in entry:
                print(f"    Entry {i} has {len(entry['meanings'])} meanings")
                for j, meaning in enumerate(entry['meanings']):
                    if 'partOfSpeech' in meaning:
                        pos = meaning['partOfSpeech'].lower()
                        print(f"      Meaning {j}: {pos}", end="")
                        if pos in stopword_pos:
                            print(" <- MATCH! Returning True")
                            return True
                        else:
                            print()

        print(f"  No match found, returning False")
        return False
    except Exception as e:
        print(f"  Exception: {e}")
        return False


test_words = ['the', 'of', 'and', 'love']
for word in test_words:
    print(f"\nTesting '{word}':")
    result = test_is_stopword(word)
    print(f"Result: {result}")

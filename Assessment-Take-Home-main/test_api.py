import requests
url = f"https://api.dictionaryapi.dev/api/v2/entries/en/of"
response = requests.get(url, timeout=5).json()
print(response)

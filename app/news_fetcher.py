import requests
from .config import NEWS_API_KEY, NEWS_API_URL

def fetch_news(page: int = 1):
    """Fetch news articles from API."""
    params = {"apiKey": NEWS_API_KEY, "country": "us", "page": page, "pageSize": 100}
    response = requests.get(NEWS_API_URL, params=params)

    if response.status_code == 200:
        return response.json()
    return {"error": response.status_code, "message": response.text}

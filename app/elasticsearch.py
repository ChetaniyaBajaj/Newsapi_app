from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from .config import ELASTICSEARCH_HOST, INDEX_NAME

es = Elasticsearch(ELASTICSEARCH_HOST)

# Ensure index exists
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME)

def article_exists(url: str) -> bool:
    """Check if an article exists in Elasticsearch."""
    try:
        es.get(index=INDEX_NAME, id=url)
        return True
    except NotFoundError:
        return False

def store_news_in_elasticsearch(articles):
    """Store news articles in Elasticsearch."""
    for article in articles:
        source_id = article.get("source", {}).get("id", "Null")
        source_name = article.get("source", {}).get("name", "Unknown")

        doc = {
            "title": article["title"],
            "description": article["description"],
            "url": article["url"],
            "published_at": article["publishedAt"],
            "source": {"id": source_id, "name": source_name},
        }

        if not article_exists(article["url"]):
            es.index(index=INDEX_NAME, id=article["url"], document=doc)
            print(f"New article added: {article['title']}")

from .celery_worker import celery
from .news_fetcher import fetch_news
from .elasticsearch import store_news_in_elasticsearch, es, INDEX_NAME

@celery.task
def fetch_and_store_news():
    """Fetch news and store in Elasticsearch."""
    page = 1
    while True:
        news_data = fetch_news(page=page)
        if "articles" in news_data and news_data["articles"]:
            store_news_in_elasticsearch(news_data["articles"])
            if len(news_data["articles"]) < 100:
                break
            page += 1
        else:
            break

@celery.task
def delete_news_articles_index():
    """Delete the news index from Elasticsearch."""
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"Deleted index: {INDEX_NAME}")
    else:
        print(f"Index {INDEX_NAME} does not exist.")

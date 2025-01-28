from fastapi import FastAPI
from celery import Celery
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import requests

app = FastAPI()

celery = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

celery.conf.update(
    {
        "beat_schedule": {
            "fetch-and-store-news": {
                "task": "main.fetch_and_store_news",
                "schedule": 60.0,
            },
            "delete-all-indices": {
                "task": "main.delete_news_articles_index",
                "schedule": 600.0,
            },
        },
        "timezone": "UTC",
    }
)

es = Elasticsearch("http://localhost:9200")
NEWS_API_KEY = "your_api_key"
NEWS_API_URL = "https://newsapi.org/v2/top-headlines"
index_name = "news_articles"

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)

def fetch_news(page: int = 1):
    params = {"apiKey": NEWS_API_KEY, "country": "us", "page": page, "pageSize": 100}
    response = requests.get(NEWS_API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": response.status_code, "message": response.text}

def article_exists(url: str) -> bool:
    try:
        es.get(index=index_name, id=url)
        return True
    except NotFoundError:
        return False

def store_news_in_elasticsearch(articles):
    for article in articles:
        source_id = article.get("source", {}).get("id", "Null")
        source_name = article.get("source", {}).get("name", "Unknown")

        doc = {
            "title": article["title"],
            "description": article["description"],
            "url": article["url"],
            "published_at": article["publishedAt"],
            "source": {
                "id": source_id,
                "name": source_name
            }
        }

        if not article_exists(article["url"]):
            es.index(index=index_name, id=article["url"], document=doc)
            print(f"New article added: {article['title']}")

@celery.task
def fetch_and_store_news():
    page = 1
    while True:
        news_data = fetch_news(page=page)
        
        if "articles" in news_data and news_data["articles"]:
            articles = news_data["articles"]
            store_news_in_elasticsearch(articles)
            
            if len(articles) < 100:
                break
            page += 1
        else:
            break

@celery.task
def delete_news_articles_index():
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            print(f"Deleted index: {index_name}")
        else:
            print(f"Index {index_name} does not exist.")
    except Exception as e:
        print(f"Error while deleting index {index_name}: {e}")


@app.get("/")
def read_root():
    fetch_and_store_news.apply_async()
    return {"status": "success", "message": "News fetching task triggered"}

@app.get("/delete-indices")
def trigger_delete_indices():
    delete_news_articles_index.apply_async()
    return {"status": "success", "message": "Delete indices task triggered"}

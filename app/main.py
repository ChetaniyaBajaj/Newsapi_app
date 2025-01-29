from fastapi import FastAPI
from .tasks import fetch_and_store_news, delete_news_articles_index

app = FastAPI()

@app.get("/")
def read_root():
    """Trigger news fetching task."""
    fetch_and_store_news.apply_async()
    return {"status": "success", "message": "News fetching task triggered"}

@app.get("/delete-indices")
def trigger_delete_indices():
    """Trigger index deletion task."""
    delete_news_articles_index.apply_async()
    return {"status": "success", "message": "Delete indices task triggered"}

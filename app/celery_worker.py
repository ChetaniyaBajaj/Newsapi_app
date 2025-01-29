from celery import Celery
from .config import REDIS_BROKER, REDIS_BACKEND

celery = Celery("tasks", broker=REDIS_BROKER, backend=REDIS_BACKEND)

celery.conf.update(
    {
        "beat_schedule": {
            "fetch-and-store-news": {
                "task": "app.tasks.fetch_and_store_news",
                "schedule": 60.0,
            },
            "delete-all-indices": {
                "task": "app.tasks.delete_news_articles_index",
                "schedule": 600.0,
            },
        },
        "timezone": "UTC",
    }
)

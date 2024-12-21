from newsapi import NewsApiClient
from kafka import KafkaProducer
import json
import time

# Init NewsAPI client
newsapi = NewsApiClient(api_key='c444a02ff5584ad5abcdbb3ddcf892a7')

# Init Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Define keyword
keywords = 'England'

# Fetch articles
def fetch_articles():
    all_articles = newsapi.get_everything(
        q=keywords,
        language='en',
        page_size=100,
        sort_by='publishedAt'
    )
    return all_articles['articles']

# Send articles to Kafka
def send_to_kafka(articles):
    for article in articles:
        producer.send('news-topic', value=article)
        time.sleep(0.1)
        print(f"Sent article: {article['title']}")

articles = fetch_articles()
send_to_kafka(articles)

from datetime import datetime
import pandas as pd
import feedparser
from google.cloud import storage
import functions_framework
import os

BUCKET_NAME = 'now-news-data-lake'

@functions_framework.http
def extract_newspaper_titles(request):
    
    
    """HTTP Cloud Function to extract titles and load them to GCS."""
    
    #Links to the RSS feeds of various Spanish newspapers
    urls = {
        "elPais" : "https://elpais.com/rss/elpais/portada.xml",
        "elMundo" : "https://e00-elmundo.uecdn.es/elmundo/rss/portada.xml",
        "ABC" : "https://www.abc.es/rss/atom/portada/",
        "laVanguardia" : "https://www.lavanguardia.com/rss/home.xml",
        "expansion" : "https://e01-expansion.uecdn.es/rss/portada.xml"
    }

    titles = []
    dates = []
    newspapers = []

    date = datetime.now().strftime("%Y-%m-%d")

    i = 0
    for media, url in urls.items():
        feed = feedparser.parse(url)
        for entry in feed.entries:
            print(i)
            i= i+1
            titles.append(entry.title)
            dates.append(date)
            newspapers.append(media)

    df = pd.DataFrame(
        {'date': dates,
         'title': titles,
         'newspaper': newspapers            
        }
    )

    name_csv_export = str(date) + '_newspaper_titles.csv'
    df.to_csv(name_csv_export, index = False, sep = ';', encoding='utf-8-sig')
    
    # Upload to Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f'now-news-bronze/{name_csv_export}')
    blob.upload_from_filename(name_csv_export)
    os.remove(name_csv_export)

    
    return f"File {name_csv_export} saved to GCS bucket {BUCKET_NAME}."


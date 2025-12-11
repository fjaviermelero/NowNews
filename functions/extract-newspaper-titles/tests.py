from datetime import datetime
import feedparser
import pandas as pd

urls = {
    "elPais": "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada",
    "elMundo": "https://e00-elmundo.uecdn.es/elmundo/rss/espana.xml",
    "elMundo_inter": "https://e00-elmundo.uecdn.es/elmundo/rss/internacional.xml",
    "elConfidencial": "https://rss.elconfidencial.com/espana/",
    "elConfidencial_inter": "https://rss.elconfidencial.com/mundo/",
    "ABC": "https://www.abc.es/rss/atom/portada/",
    "ABC_inter": "https://www.abc.es/rss/2.0/internacional/",
    "ABC_ultima_hora": "https://www.abc.es/rss/2.0/ultima-hora/",
    "laVanguardia": "https://www.lavanguardia.com/rss/home.xml",
    "expansion": "https://e01-expansion.uecdn.es/rss/portada.xml",
}

titles = []
dates = []
newspapers = []

date = datetime.now().strftime("%Y-%m-%d")

i = 0
for media, url in urls.items():
    feed = feedparser.parse(url)
    
    if media in ('elMundo', 'elMundo_inter'):
        media = 'elMundo'
        
    if media in ('ABC', 'ABC_inter', 'ABC_ultima_hora'):
        media = 'ABC'    
        
    if media in ('elConfidencial', 'elConfidencial_inter'):
        media = 'elConfidencial'            
    
    for entry in feed.entries:
        print(i)
        print(media)
        i += 1
        titles.append(entry.title)
        dates.append(date)
        newspapers.append(media)

df = pd.DataFrame(
    {
        "date": dates,
        "title": titles,
        "newspaper": newspapers,
    }
)

name_csv_export = f"{date}_newspaper_titles.csv"
df.to_csv(name_csv_export, index=False, sep=";", encoding="utf-8-sig")

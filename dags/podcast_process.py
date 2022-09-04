import xmltodict
from airflow import DAG
# import providers
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from datetime import datetime
import os
import requests

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "./episodes"
FRAME_RATE = 16000


def _get_eps(ti):
    data = requests.get(PODCAST_URL)
    feed = xmltodict.parse(data.text)
    episodes = feed["rss"]["channel"]["item"]
    print(f"Found {len(episodes)} episodes.")
    ti.xcom_push(key='episodes', value= episodes)

def _load_eps(ti):
    hook = PostgresHook(postgres_conn_id='postgres')
    old_eps = hook.get_pandas_df('SELECT * FROM podcast;')
    new_eps = []
    episodes = ti.xcom_pull(key='episodes', task_ids='extract_eps')

    for eps in episodes:
        if eps['link'] not in old_eps['link'].values:
            print([eps['link'], eps['title'], eps['pubDate'], eps['description']])
            filename = f"{eps['link'].split('/')[-1]}.mp3"
            new_eps.append([eps['link'], eps['title'], eps['pubDate'], eps['description'], filename])

    hook.insert_rows(table='podcast', rows=new_eps, target_fields=["link", "title", "published", "description", "filename"])

def _download_episodes(ti):
        audio_files = []
        episodes = ti.xcom_pull(key='episodes', task_ids='extract_eps')
        for episode in episodes:
            name_end = episode["link"].split('/')[-1]
            filename = f"{name_end}.mp3"
            audio_path = os.path.join(EPISODE_FOLDER, filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                with open(EPISODE_FOLDER, "wb+") as f:
                    f.write(audio.content)
            audio_files.append({
                "link": episode["link"],
                "filename": filename
            })

with DAG('podcast_processing', start_date= datetime(2022,9,4), schedule_interval='@daily', catchup=False) as dag:
    create_table= PostgresOperator(task_id='create_table', postgres_conn_id='postgres', sql='''
        CREATE TABLE IF NOT EXISTS podcast(
            link TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            filename TEXT NOT NULL,
            published TEXT NOT NULL,
            description TEXT NOT NULL
        );
    ''')
    extract_eps = PythonOperator(task_id='extract_eps', python_callable= _get_eps)
    load_eps = PythonOperator(task_id='load_eps', python_callable=_load_eps)
    download_eps = PythonOperator(task_id='download_eps', python_callable = _download_episodes)

create_table >> extract_eps >> [load_eps, download_eps]
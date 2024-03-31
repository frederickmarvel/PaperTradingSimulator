import pandas as pd
import requests
import numpy as np
import json
import pymysql
import os
from dotenv import load_dotenv
from logging import getLogger

load_dotenv()
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

logger = getLogger(__name__) 

conn = pymysql.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME'),
)
cursor = conn.cursor()


def insertData():
    xbx_url = "https://cdi-website-three.vercel.app/indices/api/v1/trend-indicator?symbol=xbx&history=true"
    try:
        response = requests.get(xbx_url)
        response.raise_for_status()  
        data = response.json()['data']
        for item in data:
            try:
                cursor.execute('''
                    INSERT INTO coindesk_data (timestamp, index_level, trend_score) VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    index_level=VALUES(index_level),
                    trend_score=VALUES(trend_score)
                ''', (item['timestamp'], item['index_level'], item['trend_score']))
                logger.info(f"Successfully inserted data for timestamp: {item['timestamp']}")
            except pymysql.Error as err:
                logger.error(f"Error inserting data: {err}")
    except requests.exceptions.RequestException as err:
        logger.error(f"Error during API request: {err}")

    conn.commit()


insertData()
conn.close()
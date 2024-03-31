import pandas as pd
import requests
import numpy as np
import json
import pymysql
from pymysql import connect, err
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

# Global Variables - Mungkin ga kepake
initialBalance = 10000
assetQuantity = 0
assetValue = 0
cashValue = 0

def create_connection():
    try:
        conn = connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
        )
        return conn
    except err.MySQLError as e:
        print(f"Error connecting to MySQL Platform: {e}")
        return None

def getValue(timestamp):
    with create_connection() as conn:
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM coindesk_data WHERE timestamp = %s', (timestamp,))
                    return cursor.fetchone()
            except err.MySQLError as e:
                logger.error(f"Database query failed: {e}")
    return (timestamp, 0, 0, 0)

# print(getValue(1514840400))

def getBalanceAsset(timestamp):
    cursor.execute('SELECT * FROM balanceAsset WHERE timestamp = %s', (timestamp,))
    row = cursor.fetchone()
    conn.close()
    return row


def currentTrend(timestamp):
    request = getValue(timestamp)
    return request[3]

def btcPrice(timestamp):
    request = getValue(timestamp)
    return request[2]

# print(currentTrend(1514840400))
# print(btcPrice(1514840400))
def rebalancePortfolio(target_btc_ratio, timestamp, usdtBalance, btcBalance):
    value = getValue(timestamp)
    if value is None or len(value) < 3:
        print(f"No data available for timestamp {timestamp}. Skipping.")
        return  # Skip this timestamp

    current_btc_price = value[2]
    total_value = usdtBalance + (int(btcBalance) * int(current_btc_price))  

    target_btc_value = total_value * target_btc_ratio
    target_usdt_value = total_value * (1 - target_btc_ratio)

    if target_btc_value > (btcBalance * current_btc_price):
        print(f"Buy BTC: {(target_btc_value - (btcBalance * current_btc_price)) / current_btc_price:.2f} units")
    elif target_btc_value < (btcBalance * current_btc_price):
        print(f"Sell BTC: {((btcBalance * current_btc_price) - target_btc_value) / current_btc_price:.2f} units")  # Corrected braces

    if target_usdt_value > usdtBalance:
        print(f"Sell BTC to get USDT: {(target_usdt_value - usdtBalance) / current_btc_price:.2f} units")
    elif target_usdt_value < usdtBalance:
        print(f"Buy USDT: {target_usdt_value - usdtBalance:.2f} units")
    btcBalance = target_btc_value / current_btc_price
    usdtBalance = target_usdt_value
    btcValue = btcBalance *current_btc_price
    finalValue = usdtBalance + btcValue

    # Update the database
    with create_connection() as conn:
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        INSERT INTO balanceAsset (timestamp, usdtBalance, bitcoinBalance, bitcoinValue, assetValue)
                        VALUES (%s, %s, %s, %s, %s)
                        ''', (timestamp, usdtBalance, btcBalance, btcValue, finalValue))
                    conn.commit()
            except err.MySQLError as e:
                logger.error(f"Failed to insert rebalanced portfolio data: {e}")

rebalancePortfolio(0.5, 1514840400, 100000, 0)
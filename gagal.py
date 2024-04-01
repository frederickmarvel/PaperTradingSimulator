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
    with create_connection() as conn:
        if conn is not None:
            try:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM balanceAsset WHERE timestamp = %s', (timestamp,))
                    row = cursor.fetchone()
                    return row
            except err.MySQLError as e:
                logger.error(f"Database query failed: {e}")
                return None
        else:
            logger.error("Failed to create database connection.")
            return None

# def currentTrend(timestamp):
#     request = getValue(timestamp)
#     return request[3]

# def btcPrice(timestamp):
#     request = getValue(timestamp)
#     return request[2]

# print(currentTrend(1514840400))
# print(btcPrice(1514840400))
def rebalancePortfolio(target_btc_ratio, timestamp, usdtBalance, btcBalance):
    value = getValue(timestamp)
    if value is None or len(value) < 3:
        print(f"No data available for timestamp {timestamp}. Skipping.")
        return  # Skip this timestamp

    current_btc_price = value[2]
    total_value = float(usdtBalance) + (float(btcBalance) * float(current_btc_price))  

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

# rebalancePortfolio(0.5, 1514840400, 100000, 0)
# updateBitcoinValue(1514926800, 3.1514926800, 14840.6)
def strategy1():
    lastTimestamp = 1514840400
    value = getBalanceAsset(lastTimestamp)
    if value is None:
        logger.error("No initial balance asset data found. Exiting strategy.")
        return

    lastUsdtBalance = value[2]
    lastBitcoinBalance = value[3]
    currentTimestamp = lastTimestamp + 86400
    
    for i in range(currentTimestamp, 1711656000 + 1, 86400):
        prevValue = getValue(i-86400)
        currentValue = getValue(i)

        # Ensure we have valid data before proceeding
        if prevValue is None or currentValue is None:
            logger.error(f"Data for timestamp {i} or {i-86400} is missing. Skipping this iteration.")
            continue

        lastTrend = prevValue[3]
        currentTrendValue = currentValue[3]

        if lastTrend == currentTrendValue:
            last_data = getBalanceAsset(i - 86400)
            if last_data:
                last_usdtBalance = last_data[2]
                last_bitcoinBalance = last_data[3]
                currentBitcoinPrice = currentValue[2]
                new_bitcoinValue = float(last_bitcoinBalance) * float(currentBitcoinPrice)
                currentAssetValue = float(new_bitcoinValue) + float(last_usdtBalance)
                
                # Database update logic here...
        else:
            btc_bal = getBalanceAsset(i - 86400)[3] if getBalanceAsset(i - 86400) else 0
            usdt_bal = getBalanceAsset(i - 86400)[2] if getBalanceAsset(i - 86400) else 0

            # Now rebalance based on the trend
            if currentTrendValue in [0, -0.5, 0.5]:
                rebalancePortfolio(0.5, i, usdt_bal, btc_bal)
            elif currentTrendValue == 1:
                rebalancePortfolio(1.0, i, usdt_bal, btc_bal)
            elif currentTrendValue == -1:
                rebalancePortfolio(0, i, usdt_bal, btc_bal)
# strategy1()
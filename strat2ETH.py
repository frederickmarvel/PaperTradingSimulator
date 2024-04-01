import os
import pymysql
from pymysql import connect, err
from dotenv import load_dotenv
from logging import getLogger
import time
# Load environment variables
load_dotenv()
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

logger = getLogger(__name__)

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


def getValue(id):
    with create_connection() as conn:
        if conn is not None:
            try:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM coindesk_data2 WHERE id = %s', (id,))
                    row = cursor.fetchone()
                    return row
            except err.MySQLError as e:
                logger.error(f"Database query failed: {e}")
                return None
        else:
            logger.error("Failed to create database connection.")
            return None    


def getBalanceAsset(id):
    with create_connection() as conn:
        if conn is not None:
            try:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM balanceAssetETH2 WHERE id = %s', (id,))
                    row = cursor.fetchone()
                    return row
            except err.MySQLError as e:
                logger.error(f"Database query failed: {e}")
                return None
        else:
            logger.error("Failed to create database connection.")
            return None
        
def rebalancePortfolio(id,target_btc_ratio, timestamp, usdtBalance, btcBalance):
    value = getValue(id)
    if value is None or len(value) < 4:
        print(f"No data available for id {id}. Skipping.")
        return
    
    currentBTCPrice = value[2]
    total_value = usdtBalance + (btcBalance * currentBTCPrice)
    target_btc_value = total_value * target_btc_ratio
    target_usdt_value = total_value * (1 - target_btc_ratio)

    if target_btc_value > (btcBalance * currentBTCPrice):
        print(f"Buy BTC: {(target_btc_value - (btcBalance * currentBTCPrice)) / currentBTCPrice:.2f} units")
    elif target_btc_value < (btcBalance * currentBTCPrice):
        print(f"Sell BTC: {((btcBalance * currentBTCPrice) - target_btc_value) / currentBTCPrice:.2f} units")  # Corrected braces

    if target_usdt_value > usdtBalance:
        print(f"Sell BTC to get USDT: {(target_usdt_value - usdtBalance) / currentBTCPrice:.2f} units")
    elif target_usdt_value < usdtBalance:
        print(f"Buy USDT: {target_usdt_value - usdtBalance:.2f} units")    
    btcBalance = target_btc_value / currentBTCPrice
    usdtBalance = target_usdt_value
    btcValue = btcBalance *currentBTCPrice
    finalValue = usdtBalance + btcValue
     # Update the database
    with create_connection() as conn:
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        INSERT INTO balanceAssetETH2 (timestamp, usdtBalance, ethereumBalance, ethereumValue, assetValue)
                        VALUES (%s, %s, %s, %s, %s)
                        ''', (timestamp, usdtBalance, btcBalance, btcValue, finalValue))
                    conn.commit()
            except err.MySQLError as e:
                logger.error(f"Failed to insert rebalanced portfolio data: {e}")   
    

def updateBitcoinValue(id):
    value = getValue(id)
    if value is None or len(value) < 4:
        print(f"No data available for id {id}. Skipping.")
        return
    balance = getBalanceAsset(id-1)
    if balance is None or len(balance) < 6:
        print(f"No data available for id {id}. Skipping.")
        return
    currentBTCPrice = value[2]
    currentTimestamp = value[1]
    lastUSDTAmount = float(balance[2])
    lastBTCAmount = float(balance[3])
    currentBTCValue = lastBTCAmount * currentBTCPrice
    totalAmount = currentBTCValue + lastUSDTAmount
    with create_connection() as conn:
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        INSERT INTO balanceAssetETH2 (timestamp, usdtBalance, ethereumBalance, ethereumValue, assetValue)
                        VALUES (%s, %s, %s, %s, %s)
                        ''', (currentTimestamp, lastUSDTAmount, lastBTCAmount, currentBTCValue, totalAmount))
                    conn.commit()
            except err.MySQLError as e:
                logger.error(f"Failed to insert rebalanced portfolio data: {e}")
    



def strategy1():
    for i in range(3,2283-1,1):
        valueNow = getValue(i)
        valueThen = getValue(i-1)
        currentTrend = valueNow[3]
        lastTrend = valueThen[3]
        if currentTrend == lastTrend:
            updateBitcoinValue(i)
        else:
            balanceLast = getBalanceAsset(i-1)
            usdtBalance = float(balanceLast[2])
            bitcoinBalance = float(balanceLast[3])
            currTimestamp = valueNow[1]
            if currentTrend == 0:
                rebalancePortfolio(i,0.5,currTimestamp,usdtBalance,bitcoinBalance)
            elif currentTrend == 0.5:
                rebalancePortfolio(i,0.75,currTimestamp,usdtBalance,bitcoinBalance)
            elif currentTrend == -0.5:
                rebalancePortfolio(i,0.25,currTimestamp,usdtBalance,bitcoinBalance)
            elif currentTrend == 1:
                rebalancePortfolio(i,1,currTimestamp,usdtBalance,bitcoinBalance)
            elif currentTrend == -1:
                rebalancePortfolio(i,0,currTimestamp,usdtBalance,bitcoinBalance)
        time.sleep(0.1)
rebalancePortfolio(1,0.5, 1514840400, 100000, 0)                
updateBitcoinValue(2)
strategy1()

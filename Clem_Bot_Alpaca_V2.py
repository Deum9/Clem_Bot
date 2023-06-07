import asyncio
import logging
import math
import smtplib
import time
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from telegram import Bot
import pandas as pd
import ta
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading import TradingClient, MarketOrderRequest, OrderSide, TimeInForce
from dateutil.relativedelta import relativedelta

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50000)
pd.set_option('display.width', 10000)


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')  # , filename='Log_Bot_Alpaca.log')
    logger = logging.getLogger()

    API_KEY = 'PKLYQ5IN21W1YS4CJ6GB'
    API_SECRET = 'RGaZhoKS4NzZQJyPn5ZMty82WJh1ycTM6vigqHpL'
    BASE_URL = 'https://paper-api.alpaca.markets'

    SYMBOL = "ETH/USD"
    TIMEFRAME = TimeFrame.Hour
    START_DATE = datetime.now() - relativedelta(days=8.45)
    TODAY = date.today()
    PAPER_BOOL = True

    SMTP_SERVER = "smtp-mail.outlook.com"
    SMTP_PORT = 587
    SMTP_USER = "arthur.demarest@outlook.com"
    SMTP_PASSWORD = "99Arthur99"

    FROM_ADDRESS = "Arthur Demarest <arthur.demarest@outlook.com>"
    TO_ADDRESS = "arthur.demarest@outlook.com"

    logger.info('Connecting to Alpaca Crypto Historical API')

    Client_Hist_Data = CryptoHistoricalDataClient(API_KEY, API_SECRET)
    request_params = CryptoBarsRequest(symbol_or_symbols=[SYMBOL],
                                       timeframe=TIMEFRAME,
                                       start=START_DATE)

    MSG = 'Downloading "' + SYMBOL + '" Historical data from ' + str(START_DATE.date()) + ' to ' + str(TODAY)
    logger.info(MSG)

    df = Client_Hist_Data.get_crypto_bars(request_params).df
    df = df.reset_index()
    df = df.set_index("timestamp")
    df = df.drop(['symbol', 'vwap'], axis=1)
    # logger.info(df)

    # EMA
    df['EMA1'] = ta.trend.ema_indicator(close=df['close'], window=21)
    df['EMA2'] = ta.trend.ema_indicator(close=df['close'], window=50)
    df['EMA3'] = ta.trend.ema_indicator(close=df['close'], window=100)
    df['EMA4'] = ta.trend.ema_indicator(close=df['close'], window=200)

    # RSI
    df['RSI'] = ta.momentum.RSIIndicator(close=df['close'], window=14).rsi()

    # Volume Oscillator
    df['Volume Oscillator'] = ta.momentum.PercentageVolumeOscillator(volume=df['volume'], window_slow=28,
                                                                     window_fast=14,
                                                                     window_sign=9).pvo()

    List_Columns = df.columns.to_list()
    List_Close = List_Columns.index('close')
    List_Oscillator = List_Columns.index('Volume Oscillator')

    Delta_24 = []
    Oscillator_24 = []
    Ratio_Delta_Volume = []

    for i in range(0, len(df)):
        if i > 23:
            Oscillator_24.append(
                ((df.iloc[i, List_Oscillator] - df.iloc[i - 24, List_Oscillator]) / abs(
                    df.iloc[i - 24, List_Oscillator])))
            Delta_24.append((df.iloc[i, List_Close] / df.iloc[i - 24, List_Close]) - 1)
        else:
            Oscillator_24.append(0)
            Delta_24.append(0)

    for i in range(0, len(df)):
        if abs(Delta_24[i]) > 0.01:
            Ratio_Delta_Volume.append(Oscillator_24[i] / Delta_24[i])
        else:
            Ratio_Delta_Volume.append(0)

    df['Delta Oscillator 24h'] = Oscillator_24
    df['Delta Spot 24h'] = Delta_24
    df['Ratio Delta Volume'] = Ratio_Delta_Volume

    Trading_Client = TradingClient(API_KEY, API_SECRET, paper=PAPER_BOOL)

    ACCOUNT = Trading_Client.get_account()

    USD_BALANCE = math.trunc(float(ACCOUNT.cash) * 100) / 100
    EQUITY = round(float(ACCOUNT.equity), 2)
    ALL_POSITION = Trading_Client.get_all_positions()

    if ALL_POSITION:
        OPEN_POSITION = Trading_Client.get_open_position(SYMBOL.replace('/', ''))
        CRYPTO_POSITION = True
        CRYPTO_SIDE = str(OPEN_POSITION.side).split('.')[-1]
        CRYPTO_QTY = OPEN_POSITION.qty_available
        CRYPTO_CURRENT_PRICE = round(float(OPEN_POSITION.current_price), 2)
        CRYPTO_MARKET_VALUE = round(float(OPEN_POSITION.market_value), 2)

        logger.info(SYMBOL)
        logger.info('USD Balance : ' + str(USD_BALANCE))
        logger.info('Crypto Position : ' + CRYPTO_SIDE)
        logger.info('Quantity : ' + str(CRYPTO_QTY))
        logger.info('Current Price : ' + str(CRYPTO_CURRENT_PRICE))
        logger.info('Market Value : ' + str(CRYPTO_MARKET_VALUE))
        logger.info('Total Equity : ' + str(EQUITY))

    else:
        CRYPTO_POSITION = False
        CRYPTO_SIDE = None
        logger.info(SYMBOL)
        logger.info('USD Balance : ' + str(USD_BALANCE))
        logger.info('Crypto Position : ' + str(CRYPTO_SIDE))
        logger.info('Quantity : ' + str(0))
        logger.info('Current Price : ' + str(df['close'][-1]))
        logger.info('Total Equity : ' + str(EQUITY))

    def longCondition(row):
        global CONDITION
        if (row['EMA1'] > row['EMA2'] > row['EMA3'] > row['EMA4']
                and row['RSI'] < 70
                and (row['Delta 24h'] < 0 and row['Ratio Delta Volume'] < -10)):
            CONDITION = 'LONG Condition REACHED'
            logger.info(CONDITION)
            return True
        else:
            return False

    def closelongCondition(row):
        global CONDITION
        if row['EMA4'] > row['EMA1'] or row['RSI'] >= 90:
            CONDITION = 'Exit LONG Condition REACHED'
            logger.info(CONDITION)
            return True
        else:
            return False

    def shortCondition(row):
        global CONDITION
        if (row['EMA4'] > row['EMA3'] > row['EMA2'] > row['EMA1']
                and row['RSI'] > 40
                and (row['Delta 24h'] > 0 and row['Ratio Delta Volume'] < -10)):
            CONDITION = 'SHORT Condition REACHED'
            logger.info(CONDITION)
            return True
        else:
            return False

    def closeshortCondition(row):
        global CONDITION
        if row['RSI'] <= 25 or row['EMA1'] > row['EMA4']:
            CONDITION = 'Exit SHORT Condition REACHED'
            logger.info(CONDITION)
            return True
        else:
            return False

    if longCondition(df.iloc[-1]) and not CRYPTO_POSITION:
        Market_Order_Data_Long = MarketOrderRequest(symbol=SYMBOL.replace('/', ''),
                                                    notional=USD_BALANCE,
                                                    side=OrderSide.BUY,
                                                    time_in_force=TimeInForce.GTC)

        LONG_ORDER = Trading_Client.submit_order(order_data=Market_Order_Data_Long)
        ORDER_MESSAGE = 'BUY Order Submited'
        logger.info(ORDER_MESSAGE)
        logger.info(LONG_ORDER)

    elif closelongCondition(df.iloc[-1]) and CRYPTO_POSITION and CRYPTO_SIDE == "LONG":
        Market_Order_Data_Exit_Long = MarketOrderRequest(symbol=SYMBOL.replace('/', ''),
                                                         qty=CRYPTO_QTY,
                                                         side=OrderSide.SELL,
                                                         time_in_force=TimeInForce.GTC, )
        EXIT_LONG_ORDER = Trading_Client.submit_order(order_data=Market_Order_Data_Exit_Long)
        ORDER_MESSAGE = 'SELL (Exit Buy) Order Submited'
        logger.info(ORDER_MESSAGE)
        logger.info(EXIT_LONG_ORDER)

    if shortCondition(df.iloc[-1]) and not CRYPTO_POSITION:
        Market_Order_Data_Short = MarketOrderRequest(symbol=SYMBOL.replace('/', ''),
                                                     notional=USD_BALANCE,
                                                     side=OrderSide.SELL,
                                                     time_in_force=TimeInForce.GTC, )
        SHORT_ORDER = Trading_Client.submit_order(order_data=Market_Order_Data_Short)
        ORDER_MESSAGE = 'SELL Order Submited'
        logger.info(ORDER_MESSAGE)
        logger.info(SHORT_ORDER)

    elif closeshortCondition(df.iloc[-1]) and CRYPTO_POSITION and CRYPTO_SIDE == "SHORT":
        Market_Order_Data_Exit_Short = MarketOrderRequest(symbol=SYMBOL.replace('/', ''),
                                                          qty=CRYPTO_QTY,
                                                          side=OrderSide.BUY,
                                                          time_in_force=TimeInForce.GTC, )
        EXIT_SHORT_ORDER = Trading_Client.submit_order(order_data=Market_Order_Data_Exit_Short)
        ORDER_MESSAGE = 'BUY (Exit Sell) Order Submited'
        logger.info(ORDER_MESSAGE)
        logger.info(EXIT_SHORT_ORDER)

    else:
        ORDER_MESSAGE = "No Order Placed"
        logger.info(ORDER_MESSAGE)

    List_Columns = df.columns.to_list()
    Col_EMA1 = List_Columns.index('EMA1')
    Col_EMA2 = List_Columns.index('EMA2')
    Col_EMA3 = List_Columns.index('EMA3')
    Col_EMA4 = List_Columns.index('EMA4')
    Col_RSI = List_Columns.index('RSI')
    Col_Delta_Osc_24 = List_Columns.index('Delta Oscillator 24h')
    Col_Delta_Spot_24 = List_Columns.index('Delta Spot 24h')

    df['Delta Oscillator 24h'] = Oscillator_24
    df['Delta Spot 24h'] = Delta_24
    df['Ratio Delta Volume'] = Ratio_Delta_Volume

    msg = MIMEMultipart()
    msg["From"] = FROM_ADDRESS
    msg["To"] = TO_ADDRESS
    msg["Subject"] = "Bot Notification"

    if ALL_POSITION:
        TEXT_DATA = 'USD Balance : ' + str(USD_BALANCE) + "\n" + \
                    'Crypto Position : ' + CRYPTO_SIDE + "\n" + \
                    'Quantity : ' + str(CRYPTO_QTY) + "\n" + \
                    'Current Price : ' + str(CRYPTO_CURRENT_PRICE) + "\n" + \
                    'Market Value : ' + str(CRYPTO_MARKET_VALUE) + "\n" + \
                    'Total Equity : ' + str(EQUITY) + "\n" + \
                    '________________________________________'
    else:
        TEXT_DATA = 'USD Balance : ' + str(USD_BALANCE) + "\n" + \
                    'Crypto Position : ' + str(CRYPTO_SIDE) + "\n" + \
                    'Quantity : ' + str(0) + "\n" + \
                    'Current Price : ' + str(round(df['close'][-1], 2)) + "\n" + \
                    'Total Equity : ' + str(EQUITY) + "\n" + \
                    '________________________________________'

    INDICATOR = "EMA21 = " + str(df.iloc[-1, Col_EMA1]) + "\n" + \
                "EMA50 = " + str(df.iloc[-1, Col_EMA2]) + "\n" + \
                "EMA100 = " + str(df.iloc[-1, Col_EMA3]) + "\n" + \
                "EMA200 = " + str(df.iloc[-1, Col_EMA4]) + "\n" + \
                "RSI = " + str(df.iloc[-1, Col_RSI]) + "\n" + \
                "Delta Oscillator 24h = " + str(df.iloc[-1, Col_Delta_Osc_24]) + "\n" + \
                "Delta Spot 24h = " + str(df.iloc[-1, Col_Delta_Spot_24])

    MAIL_TEXT = SYMBOL + "\n\n" + \
                TEXT_DATA + "\n" + \
                CONDITION + "\n" + \
                ORDER_MESSAGE + "\n" + \
                '________________________________________'+ "\n\n" + \
                INDICATOR

    msg.attach(MIMEText(MAIL_TEXT, "plain"))

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASSWORD)
    server.send_message(msg)
    server.quit()

    logger.info("Mail sent successfully")

    logger.info("________________________________________________________________")

while True:
    main()
    time.sleep(3600)

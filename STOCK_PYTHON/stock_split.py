import requests
import csv
import numpy as np
import pandas as pd
import time
import os
from bs4 import BeautifulSoup

start_time = time.time()

path_ajust = os.getcwd() + '//ADJ_STOCK_CSV//'
path = os.getcwd() + '//STOCK_CSV//'
filename = os.listdir(path)
filenum = len(filename)


#URLリストを作成

for i in range(0, filenum):
    print(filename[i])
    filepath = path + filename[i]
    adjfile_path = path_ajust + filename[i]

    ohlcv = pd.read_csv(filepath,
                        index_col=0,
                        parse_dates=True).dropna()

    Open = ohlcv['Open'].values
    High = ohlcv['High'].values
    Low = ohlcv['Low'].values
    Close = ohlcv['Close'].values
    Volume = ohlcv['Volume'].values
    Adj_Close = ohlcv['Adj Close'].values

    df_split = Close / Adj_Close

    Open = pd.Series(Open / df_split, index=ohlcv.index.values)
    High = pd.Series(High / df_split, index=ohlcv.index.values)
    Low = pd.Series(Low / df_split, index=ohlcv.index.values)
    Close = pd.Series(Close / df_split, index=ohlcv.index.values)
    Volume = pd.Series(Volume * df_split, index=ohlcv.index.values)

    df = pd.concat([Open, High, Low, Close, Volume], axis=1)
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df.to_csv(adjfile_path)

process_time = time.time() - start_time
print(process_time)
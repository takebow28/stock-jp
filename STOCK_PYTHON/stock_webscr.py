import requests
import pandas as pd
import time
import os
from bs4 import BeautifulSoup

start_time = time.time()

headers = {"User-Agent":
           "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0"}

path = os.getcwd() + '//STOCK_CSV//'

codelist = pd.read_csv('codelist.csv', usecols=[1])
codelist = codelist['コード'].values
length = len(codelist)
leng_year = 0

urllist = []
tickerlist = []
stock_url_list = []

url = 'https://kabuoji3.com/stock/'

# URLリストを作成

for i in range(0, length):
    urllist.append(url + str(codelist[i]) + '/')

for i in range(0, length):

    # 対象ページへアクセス
    print(urllist[i])
    filepass = path + str(codelist[i]) + '.csv'
    res = requests.get(urllist[i], headers=headers)
    content = res.content

    # スクレイピングしたページデータを整形する
    soup = BeautifulSoup(content, 'html.parser')

    stock_url_list = []
    for j in range(2018, 2021):
        target_url = urllist[i] + str(j) + '/'
        # リンク要素を取得する
        link = soup.find("a", {"href": target_url})

        if bool(link):
            stock_url_list.append(link.get('href'))

    leng_year = len(stock_url_list)

    for k in range(0, leng_year):
        res = requests.get(stock_url_list[k], headers=headers)
        content = res.content
        if k == 0:
            dflist = pd.read_html(stock_url_list[k],
                                  header=0,
                                  index_col=0,
                                  parse_dates=True)
            df_0 = dflist[0]
            df = df_0

        elif(k == 0 and k != leng_year-1):
            dflist = pd.read_html(stock_url_list[k],
                                  header=0,
                                  index_col=0,
                                  parse_dates=True)
            df_0 = dflist[0]
            df = df_0

            df.to_csv(filepass, header=None)
            df = pd.read_csv(filepass,
                             names=('Date',
                                    'Open',
                                    'High',
                                    'Low',
                                    'Close',
                                    'Volume',
                                    'Adj Close'),
                             index_col=0,
                             parse_dates=True)
            df.to_csv(filepass)

        elif(k != 0 and k != leng_year-1):
            dflist = pd.read_html(stock_url_list[k],
                                  header=0,
                                  index_col=0,
                                  parse_dates=True)
            df_1 = dflist[0]
            df = pd.concat([df, df_1])

        elif(k == leng_year - 1):
            dflist = pd.read_html(stock_url_list[k],
                                  header=0,
                                  index_col=0,
                                  parse_dates=True)
            df_1 = dflist[0]
            df = pd.concat([df, df_1])

            df.to_csv(filepass, header=None)
            df = pd.read_csv(filepass,
                             names=('Date',
                                    'Open',
                                    'High',
                                    'Low',
                                    'Close',
                                    'Volume',
                                    'Adj Close'),
                             index_col=0, parse_dates=True)
            df.to_csv(filepass)

process_time = time.time() - start_time
print(process_time)

import requests
import pandas as pd
import time
import datetime
import os
from bs4 import BeautifulSoup

start_time = time.time()

#  現在時刻取得
now_time = datetime.datetime.now((datetime.timezone(datetime.timedelta(hours=9))))
hour = now_time.hour
mint = now_time.minute

#  ザラバ中判定
def stock_market_close():

    def pm16():
        return hour > 15

    def pm1520():
        return (hour > 14 and mint > 20)

    return pm16() or pm1520()


#  ザラバ中は株価取得しない
if stock_market_close():

    headers = {"User-Agent":
               "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0"}

    currentpath = os.getcwd()
    path = currentpath + '//STOCK_CSV//'

    path_adj = currentpath + '//ADJ_STOCK_CSV//'
    filename = os.listdir(path_adj)
    filenum = len(filename)

    urllist = []
    tickerlist = []
    stock_url_list = []

    url = 'https://kabutan.jp/stock/?code='
    kabuoji_url = 'https://kabuoji3.com/stock/'

    # URLリストを作成

    for i in range(0, filenum):
        code = filename[i].rstrip('.csv')
        urllist.append(url + str(code))

    # ファイルリストに従って順次実行
    for i in range(0, filenum):

        code = filename[i].rstrip('.csv')
        print(code)

        # 対象ページへアクセス
        target_url = urllist[i]
        res = requests.get(target_url, headers=headers)
        content = res.content
        soup = BeautifulSoup(content, 'html.parser')

        #  前営業日の日付取得
        two_last_day = soup.findAll('dd', {'class': "floatr"})
        list_length = len(two_last_day)

        #  前営業日を取得できた銘柄のみ実行
        if list_length != 0:

            #  前営業日を計算
            two_last_day = two_last_day[0].find('time')
            two_last_day = two_last_day.get('datetime')
            two_last_day = datetime.datetime.strptime(two_last_day, '%Y-%m-%d')

            #  最新の営業日を取得
            data = soup.findAll('h2')
            lastday = data[1].find('time')
            lastday = lastday.get('datetime')

            #  データ取得
            dflist = pd.read_html(target_url, header=None, index_col=0)

            df_ohlc = dflist[3]
            df_volume = dflist[4]
            contract_num = df_volume.loc['約定回数', 1].rstrip(' 回').replace(',', '')
            contract_num = int(contract_num)
            lastday = datetime.datetime.strptime(lastday, '%Y-%m-%d')

            new_day = lastday

            #  約定回数が0でない場合データ更新
            if contract_num != 0:
                new_open = float(df_ohlc.loc['始値', 1])
                new_high = float(df_ohlc.loc['高値', 1])
                new_low = float(df_ohlc.loc['安値', 1])
                new_close = float(df_ohlc.loc['終値', 1])
                new_volume = df_volume.loc['出来高', 1].rstrip(' 株').replace(',', '')
                new_volume = float(new_volume)

                df_new = pd.DataFrame({'Open': new_open,
                                    'High': new_high,
                                    'Low': new_low,
                                    'Close': new_close,
                                    'Volume': new_volume},
                                    index=[new_day])

                filepath = path_adj + filename[i]

                #  ローカル呼び出し
                df_temp = pd.read_csv(filepath, index_col=0, parse_dates=True)

                df_temp_open = df_temp['Open']
                df_temp_date = df_temp['Open'].index

                df_date = df_temp['Open'].index
                finaldate = df_date[-1]

                if finaldate == two_last_day:
                    print('Data update is possible')
                    if finaldate != lastday:
                        df = pd.concat([df_temp, df_new])
                        df.to_csv(filepath)
                        print('update_success')

                    else:
                        print('The data was up to date')

                elif finaldate == lastday:
                    print('The data was up to date')

                else:
                    print('Data update is unpossible')
                    print('Please update data to yesterday_state')

            else:
                print('Today_Volume is 0')

        else:
            print('There is no data on kabutan')

        time.sleep(0.3)

else:
    print('Tokyo stock market is not close.')

process_time = time.time() - start_time
print(process_time)

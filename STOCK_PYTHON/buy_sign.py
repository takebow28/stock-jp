
import pandas as pd
import os
import time
import datetime

tradenumber = 0
count = 0
test = 0

# CSVファイルカウント
currentpath = os.getcwd()
fileoutpath = currentpath + '//BUY//'
path = currentpath + '//ADJ_STOCK_CSV//'
filename = os.listdir(path)
filenum = len(filename)
codelistpath = currentpath + '//codelist.csv'
num = 0
today = datetime.date.today()
print(today)
today = today.strftime('%Y%m%d')

# ピラミッディング有効無効処理
outputfilename_detail = 'BBbreak_sign_jpstock_' + today

df_codelist = pd.read_csv(codelistpath, usecols=[1, 2, 3, 5, 7],
                          index_col=0, header=None, skiprows=1)

# CSV出力用データフレーム作成

TradeDetailData = pd.DataFrame({'code': [0],
                                'name': ['name'],
                                'Market': ['Tokyo'],
                                '33Sector': ['Sector'],
                                '17Sector': ['Sector'],
                                'Entrydate': [0],
                                'ClosePrice': [0],
                                '350_UP': [0],
                                'Vol': [0],
                                'Vol50': [0],
                                'Vol50min': [0]})


# 基準テクニカル期間
ATR_LENGTH = 20
SMA_LENGTH = 350

start = 0
end = 0

start_time = time.time()

for j in range(0, filenum):
    filepath = path + filename[j]
    code = filename[j].rstrip('.csv')
    print(code)

# データ読み込み処理
    ohlcv = pd.read_csv(filepath,
                        usecols=[0, 1, 2, 3, 4, 5],
                        index_col=0,
                        parse_dates=True).dropna()

    Open = ohlcv['Open'].values
    High = ohlcv['High'].values
    Low = ohlcv['Low'].values
    Close = ohlcv['Close'].values
    Vol = ohlcv['Volume'].values
    Date_ohlcv = ohlcv['Open'].index
    Open_pd = ohlcv['Open']
    High_pd = ohlcv['High']
    Low_pd = ohlcv['Low']
    Close_pd = ohlcv['Close']
    Vol_pd = ohlcv['Volume']
    datanum = len(Close)

    def atr(df, ema_period1, ma_period, ma_period2):
        tr = pd.Series(0.0, index=df.index.values)
        High = df['High']
        Low = df['Low']
        Close = df['Close']
        length = len(df)
        for i in range(1, length):
            curhigh = High[i]
            prevdayclose = Close[i-1]
            curlow = Low[i]
            highclose = (curhigh - prevdayclose)
            closelow = (prevdayclose - curlow)
            highlow = (curhigh - curlow)
            pricerange = [highclose, closelow, highlow]
            maxrange = max(pricerange)
            tr[i] = maxrange
        atr1 = pd.Series.ewm(tr, span=ema_period1).mean().values
        atr2 = tr.rolling(ma_period).mean().values
        atr3 = tr.rolling(ma_period2).mean().values

        return atr1, atr2, atr3

# テクニカル指標作成
    (atr, atr2, atr3) = atr(ohlcv, 20, 50, 5)

    sma = ((Close_pd.rolling(SMA_LENGTH).mean()) * 0.9).values

    up_pd = ((Close_pd.rolling(SMA_LENGTH).mean()) +
             (Close_pd.rolling(SMA_LENGTH).std() * 2))

    up = up_pd.values

    sup = ((Close_pd.rolling(SMA_LENGTH).mean()) +
           (Close_pd.rolling(SMA_LENGTH).std() * 2.5)).values

    ema300 = (pd.Series.ewm(Close_pd, span=300).mean()).values

    ema25_pd = pd.Series.ewm(Close_pd, span=25).mean()
    ema25 = ema25_pd.values

    momentum = ((ema25_pd - up_pd).rolling(20)).min().values

    vol50 = (Vol_pd.rolling(50).mean()).values
    vol50_hf = (vol50 / 2)
    vol_min = (Vol_pd.rolling(50).min()).values
    vol25_max = (Vol_pd.rolling(25).max()).values
    vol25 = Vol_pd.rolling(25).mean()
    vol25_hf = vol25 / 2
    vol25_min = Vol_pd.rolling(25).min()

    high250 = (High_pd.rolling(250).max()).values
    high60 = (High_pd.rolling(60).max()).values
    high25 = (High_pd.rolling(25).max()).values
    low25 = (Low_pd.rolling(25).min()).values
    close_diff = (Close_pd.pct_change()).values


# 売買関数作成
    # Entry条件
    def buy_entry(i):
        def up_break(i):
            return ((Close[i] > up[i]) and (Close[i-1] < up[i-1])) or  ((High[i] > up[i]) and (Close[i-1] < up[i-1]))

        def trend_filter(i):
            return ema25[i] > ema300[i]

        return up_break(i) and trend_filter(i)

# 判定
    if(buy_entry(-1)):
        print(code)

        TradeDetailData.loc[num, 'code'] = code
        code = int(code)
        TradeDetailData.loc[num, 'name'] = df_codelist.loc[code, 2]
        TradeDetailData.loc[num, 'Market'] = df_codelist.loc[code, 3]
        TradeDetailData.loc[num, '33Sector'] = df_codelist.loc[code, 5]
        TradeDetailData.loc[num, '17Sector'] = df_codelist.loc[code, 7]
        TradeDetailData.loc[num, 'Entrydate'] = Date_ohlcv[-1].strftime('%F')
        TradeDetailData.loc[num, 'ClosePrice'] = Close[-1]
        TradeDetailData.loc[num, 'atr20'] = round(atr[-1] , 3)
        TradeDetailData.loc[num, 'atr20'] = round(atr[-1] / Close[-1], 3)
        TradeDetailData.loc[num, '350_UP'] = round(up[-1], 3)
        TradeDetailData.loc[num, 'Vol'] = Vol[-1]
        TradeDetailData.loc[num, 'Vol50'] = vol50[-1]
        TradeDetailData.loc[num, 'Vol50min'] = vol_min[-1]

        num = num + 1

# Summary処理
if num != 0:
    TradeDetailData.to_csv(fileoutpath + outputfilename_detail + '.csv')
else:
    print('There are no stocks reaching buy point')

process_time = time.time() - start_time
print(process_time)

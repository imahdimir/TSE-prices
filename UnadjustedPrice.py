"""

    """

import requests
import json
import pandas as pd
from githubdata import GitHubDataRepo
from tqdm import tqdm
import time
from bs4 import BeautifulSoup
import datetime
import jalali_pandas
from persiantools.jdatetime import JalaliDate

def number_corrector(df,i):
    k = df.iloc[: , i:i+1]
    k = k.replace('1',"01")
    k = k.replace('2' , "02")
    k = k.replace('3' , "03")
    k = k.replace('4' , "04")
    k = k.replace('5' , "05")
    k = k.replace('6' , "06")
    k = k.replace('7' , "07")
    k = k.replace('8' , "08")
    k = k.replace('9' , "09")
    df.iloc[: , i:i+1] = k
    return df

def date_corrector(df):
    df["JDate"] = pd.to_datetime(df["JDate"])
    df["JDate"] = df["JDate"].jalali.to_jalali()
    df['year'] = df["JDate"].jalali.year.astype(str)
    df['month'] = df["JDate"].jalali.month.astype(str)
    df = number_corrector(df , 9)
    df['day'] = df["JDate"].jalali.day.astype(str)
    df = number_corrector(df , 10)
    df['JDate'] = df['year'] + df['month'] + df['day']
    df = df.iloc[: , :8]
    return df

def add_shareout(df):
    Shareout = pd.read_excel("Shareout.xlsx")
    Shareout['JDate'] = Shareout['JDate'].astype(str)
    df['JDate'] = df['JDate'].astype(str)
    df.dtypes
    df1 = df.merge(Shareout , on = ['JDate' , "Ticker"] , how = 'outer')
    df1 = df1.sort_values(by = ['Ticker' , 'JDate'] , ascending = False)
    df1['Shareout'] = df1.groupby(["Ticker"])['Shareout1'].fillna(method = 'ffill')
    df1 = df1.sort_values(by = ['Ticker' , 'JDate'])
    df1 = df1.iloc[: , :-1]
    # df1 = df1.dropna()
    # df1['MarketCap'] = df1['Shareout'] * df1['Close']
    return df1

def adjuster_price(df):
    df['AdjustedClose'] = df['MarketCap']/df['Shareout'].iloc[-1]
    return df

def df_maker_url1(i,t):
    id = stock_id.iloc[i , 0]
    url = f"http://members.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={id}&Top=999999&A=0"
    r = requests.get(url, timeout=15, headers = {
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
            } )
    time.sleep(t)
    df = pd.DataFrame(r.text.split(';') , columns = [0])
    df = df[0].str.split('@' , expand = True)
    df.columns = ["JDate" , "Max" , "Min" , "Close" , "Last" , "First" ,
                  "Yestrtday" , "Value" , "Volume" , "Count"]
    df = df[["JDate" , "Max" , "Min" , "First" , "Last" , "Volume" , "Close"]]
    df["Ticker"] = stock_id.iloc[i , 1]
    return df

def df_maker_url2(i,t):
    id = stock_id.iloc[i , 0]
    url = f"https://members.tsetmc.com/tsev2/chart/data/Financial.aspx?i={id}&t=ph&a=0"
    r = requests.get(url, timeout=15, headers = {
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
            } )
    time.sleep(t)
    df = pd.DataFrame(r.text.split(';') , columns = [0])
    df = df[0].str.split(',' , expand = True)
    df.columns = ["JDate" , "Max" , "Min" , "First" , "Last" , "Volume" ,
                  "Close"]
    df["Ticker"] = stock_id.iloc[i , 1]
    return df

def df_maker_url3(i,t):
    id = stock_id.iloc[i , 0]
    url = f"https://tsetmc.com/tsev2/data/Export-txt.aspx?t=i&a=1&b=0&i={id}"
    r = requests.get(url, timeout=15, headers = {
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
            } )
    time.sleep(t)
    df = pd.DataFrame(r.text.split(';') , columns = [0])
    df = df[0].str.split(',' , expand = True)
    df.columns = ["JDate" , "Max" , "Min" , "First" , "Last" , "Volume" ,
                  "Close"]
    df["Ticker"] = stock_id.iloc[i , 1]
    return df

##
class GDUrl :
    with open('E:\RA_AliMarashi\pycharm\IndIns\d-FirmTicker-Industry-SubIndustry\gdu1.json' , 'r') as f :
        gj = json.load(f)

    src = gj['src']
    src0 = gj['src0']


gu = GDUrl()
##

def main() :
    pass

    ##
    gds = GitHubDataRepo(gu.src)
    ##
    gds.overwriting_clone()
    ##
    stock_id = gds.read_data()
    df = pd.read_parquet(
        r"E:\RA_AliMarashi\pycharm\IndIns\Data\mergerdallData_cleaned.parquet").sort_values(
        by = ["date"])

    stock_id2 = df[['stock_id' , 'name']].drop_duplicates(keep = "last").sort_values('name')
    stock_id2.columns = ['TSETMC_ID' , 'FirmTicker']
    stock_id2['TSETMC_ID'] = stock_id2['TSETMC_ID'].astype('int64')
    stock_id.dtypes
    stock_id1 = stock_id.append(stock_id2)
    stock_id = stock_id1.reset_index(drop = True)
    stock_id = stock_id.sort_values(by = ['FirmTicker'])
    # stock_id['TSETMC_ID'] = stock_id['TSETMC_ID'].astype(str)
    stock_id = stock_id.drop_duplicates(keep = 'first').sort_values('FirmTicker')
    #
    stock_id = stock_id.reset_index(drop = True)
stock_id.to_excel('stock_id.xlsx' , index = False)
##
stock_id = pd.read_excel(r'E:\RA_AliMarashi\pycharm\TSE-prices\stock_id.xlsx')

##
i = -10
stock_id.iloc[i,0]
k = 0
list = []
for i in tqdm(range(len(stock_id))):
    try:
        df = df_maker_url1(i,0.05)
        list.append(df)
    except:
        try:
            df = df_maker_url2(i , 0.05)
            list.append(df)
            print('ali1')
        except:
            try:
                df = df_maker_url1(i , 0.5)
                list.append(df)
                print('ali2')
            except:
                k = k +1
                print(k)
                pass
df = pd.concat(list , axis = 0 , ignore_index = False)
df = df.dropna()
df = date_corrector(df)
df.to_parquet('UnAdjustedPrice.parquet' , index = False)
df.Ticker.nunique()
##
UnAdjustedPrice = pd.read_parquet("UnAdjustedPrice.parquet")
df = UnAdjustedPrice

df['Close'] = df['Close'].astype("float")
df = add_shareout(df)
# df = df.groupby('Ticker').apply(adjuster_price)
# df = df.dropna()
df.to_parquet('UnAdjustedPrice_shareout.parquet' , index = False)


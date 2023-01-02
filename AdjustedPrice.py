"""

    """

import requests
import json
import pandas as pd
from githubdata import GitHubDataRepo
from tqdm import tqdm
import time
import datetime
import jalali_pandas

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
    df["JDate"] = pd.to_datetime(df["JDate"] , format = '%Y%m%d')
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
    df.dtypes
    df1 = df.merge(Shareout , on = ['JDate' , "Ticker"] , how = 'outer')
    df1 = df1.sort_values(by = ['Ticker' , 'JDate'] , ascending = False)
    df1['Shareout'] = df1.groupby(["Ticker"])['Shareout1'].fillna(method = 'ffill')
    df1 = df1.sort_values(by = ['Ticker' , 'JDate'])
    df1 = df1.iloc[: , :-1]
    df1['MarketCap'] = df1['Shareout'] * df1['Close']
    return df1
def adjuster_price(df):
    return df['MarketCap']/df['Shareout'].iloc[-1]

def df_maker_url(i , t) :
    id = stock_id.iloc[i , 0]
    url = f"https://members.tsetmc.com/tsev2/chart/data/Financial.aspx?i={id}&t=ph&a=1"
    r = requests.get(url)
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

##
#len(stock_id)
i = 0
list = []
for i in tqdm(range(len(stock_id))):
    try:
        df = df_maker_url(i , 0.01)
        list.append(df)
    except:
        try :
            df = df_maker_url(i , 0.5)
            list.append(df)
        except :
            print(df.shape)
            pass
df = pd.concat(list , axis = 0 , ignore_index = False)
df.to_parquet('AdjustedPrice.parquet' , index = False)
df.Ticker.nunique()
##
AdjustedPrice = pd.read_parquet("AdjustedPrice.parquet")
df = AdjustedPrice
df = df.dropna()
df = date_corrector(df)
df['Close'] = df['Close'].astype("float")
df = add_shareout(df)
df = df.dropna()
df.to_parquet('AdjustedPrice.parquet' , index = False)
# df["AdjustedClose"] = 1
df["AdjustedClose"] = df.groupby('Ticker').apply(adjuster_price)
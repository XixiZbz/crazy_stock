import json
import akshare as ak
import arrow
from pip import main
import pymongo
from multiprocessing.dummy import Pool as ThreadPool

pool = ThreadPool(10)
myclient = pymongo.MongoClient('localhost', 27017)

def get_history_data(input_date):
    code = input_date.get("代码","")
    name = input_date.get("名称","")
    collection_history  = myclient.stock.history
    today = arrow.now().format("YYYYMMDD")
    try:
       data =  ak.stock_zh_a_hist(symbol=code,start_date=today,end_date=today)
    except Exception as why:
        print(f"{why},{code},{name}")
        return
    data['gmt_create'] = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['gmt_modified'] =arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['代码'] =code 
    data['名称'] = name
    data = json.loads(data.T.to_json()).values()
    try:
         collection_history.insert_many(data)
    except Exception as why:
        print(why)
        return 
def main():
    myclient = pymongo.MongoClient('localhost', 27017)
    collection_daily = myclient.stock.daily
    is_not_inits = collection_daily.find()
    collection_history  = myclient.stock.history
    column = [("代码",-1),("名称",-1),("日期",-1)]
    collection_history.create_index(column,unique=True,name='唯一键')
    pool.map(get_history_data,is_not_inits)

if __name__ == '__main__':
    main()
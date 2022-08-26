import json
import akshare as ak
import arrow
import pymongo
from multiprocessing.dummy import Pool as ThreadPool

myclient = pymongo.MongoClient('localhost', 27017)
collection_daily = myclient.stock.daily
is_not_inits = collection_daily.find({'init':0})
collection_history  = myclient.stock.history
column = [("代码",-1),("名称",-1),("日期",-1)]
collection_history.create_index(column,unique=True,name='唯一键')
def main(is_not_init):
    code = is_not_init.get("代码","")
    name = is_not_init.get("名称","")
    try:
        data = ak.stock_zh_a_hist(symbol=code)
    except Exception as why:
        print(f"{why},{code},{name}")
        return
    data['gmt_create'] = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['gmt_modified'] =arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['代码'] =code 
    data['名称'] = name
    data = json.loads(data.T.to_json()).values()
    for d in data:
        try:
            collection_history.insert_one(d)
        except pymongo.errors.DuplicateKeyError:
            print(f"{d['代码']} {d['日期']} is duplicate")
            continue
    collection_daily.update_one({'代码':code},{'$set':{'init':1}})
if __name__ == '__main__':
    pool = ThreadPool(1)
    pool.map(main,is_not_inits)
   
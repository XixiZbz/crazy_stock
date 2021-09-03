import json
import akshare as ak
import arrow
import pymongo

myclient = pymongo.MongoClient('localhost', 27017)
collection_daily = myclient.stock.daily
is_not_inits = collection_daily.find({'init':0})
collection_history  = myclient.stock.history
column = [("代码",-1),("名称",-1),("日期",-1)]
collection_history.create_index(column,unique=True,name='唯一键')
for is_not_init in is_not_inits:
    code = is_not_init.get("代码","")
    name = is_not_init.get("名称","")
    try:
        data = ak.stock_zh_a_hist(symbol=code)
    except Exception as why:
        print(f"{why},{code},{name}")
        continue
    data['gmt_create'] = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['gmt_modified'] =arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['代码'] =code 
    data['名称'] = name
    data = json.loads(data.T.to_json()).values()
    try:
        collection_history.insert_many(data)
    except Exception as why:
        print(why)
        continue
    collection_daily.update_one({"代码":code},{"$set":{"init":1}},upsert=True)
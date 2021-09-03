import akshare as ak
import pymongo
import json
import arrow
myclient = pymongo.MongoClient('localhost', 27017)
collection = myclient.stock.daily
# 获取所有code代码，
stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
column = list(stock_zh_a_spot_em_df)
column = [(c,pymongo.DESCENDING) for c in column[1:3]]
collection.create_index(column,unique=True,name='唯一键')
datas = json.loads(stock_zh_a_spot_em_df.T.to_json()).values()
for data in datas:
    s_num,code,name,*_  = data.values()
    update_date = {key:value for key,value in data.items() if key not in ['序号','代码',"名称"]}
    is_exit = collection.find_one({"代码":code})
    data['gmt_create'] = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['gmt_modified'] =arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['init'] = 0
    update_date["gmt_modified"] = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    data['update_date'] = arrow.now().date().__str__()
    update_date["update_date"] = arrow.now().date().__str__()
    if is_exit:
        collection.update_one({"代码":code},{"$set":update_date},upsert=True,bypass_document_validation=False)
    else:
        collection.insert_one(data)

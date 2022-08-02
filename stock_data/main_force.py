import akshare as ak
import pymongo
import arrow
myclient = pymongo.MongoClient('localhost', 27017)
# collection_main_force = myclient.stock.main_force
column = [("代码",-1),("日期",-1)]
myclient.stock.main_force.create_index(column,unique=True,name='唯一键')
today = arrow.now().format("YYYY-MM-DD")
tem_data = myclient.stock.history.find({"代码":{"$regex":"(^60\d*|^00\d*)"},"日期":f"{today}"},{'代码':1,'名称':1})
def sync_main_force(tem): 
    print(tem['代码'])
    code = tem['代码']
   
    name = tem['名称']
    if code[0] == '6':
        market = 'sh'
    else:
        market = 'sz'
    stock_individual_fund_flow_df = ak.stock_individual_fund_flow(
        stock=f"{code}", market=market)
    is_today_data = stock_individual_fund_flow_df.iloc[-1]['日期']
    if is_today_data == arrow.now().format("YYYY-MM-DD"):
        data = {}
        main_force_rate =stock_individual_fund_flow_df.iloc[-1]['主力净流入-净占比']
        data['主力占比']=main_force_rate
        data['日期']=is_today_data
        data['代码']=code
        data['名称']=name
        try:
            myclient.stock.main_force.insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            return
    else:
        return
    
if __name__ == '__main__':
    from multiprocessing.dummy import Pool as ThreadPool
    pool = ThreadPool(10)
    pool.map(sync_main_force,tem_data)
from copy import deepcopy
import akshare as ak
import pymongo
import arrow
myclient = pymongo.MongoClient('localhost', 27017)
def storage_every_deal(tem):
    print(tem['代码'])
    code = tem['代码']
    if code[0] == '6':
        market_code = f'sh{code}'
    else:
        market_code = f'sz{code}'
    try:
        stock_zh_a_tick_tx_js_df = ak.stock_zh_a_tick_tx_js(symbol=market_code)
    except Exception as e:
        print(e)
        return
    stock_zh_a_tick_tx_js_df['代码'] = code
    stock_zh_a_tick_tx_js_df['名称'] = tem['名称']
    if arrow.now().hour < 16:
        stock_zh_a_tick_tx_js_df['日期'] = arrow.now().shift(days=-1).format("YYYY-MM-DD")
    else:
        stock_zh_a_tick_tx_js_df['日期'] = arrow.now().format("YYYY-MM-DD")
    try:
        myclient.stock.every_deal.insert_many(stock_zh_a_tick_tx_js_df.to_dict('records'))
    except pymongo.errors.BulkWriteError:
        return 

if __name__ == '__main__':
    from multiprocessing.dummy import Pool as ThreadPool
    today = arrow.now().format("YYYY-MM-DD")
    column = [("代码",-1),("日期",-1),("成交时间",-1)]
    myclient.stock.every_deal.create_index(column,unique=True,name='唯一键')
    tem_data = myclient.stock.history.find({"代码":{"$regex":"(^60\d*|^00\d*)"},"日期":f"{today}"},{'代码':1,'名称':1})

    pool = ThreadPool(20)
    pool.map(storage_every_deal,tem_data)
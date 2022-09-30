from copy import deepcopy
from turtle import hideturtle
import arrow
import pymongo
import pandas as pd
"""
收盘获利
主力占比 
盈利情况

换手率、
今日涨跌、
最新价格，
名称，
代码，
"""
today = arrow.now().format("YYYY-MM-DD")
# today = "2022-07-27"
myclient = pymongo.MongoClient('localhost', 27017)
column = [("代码",-1),("日期",-1)]
myclient.stock.aggregation.create_index(column,unique=True,name='唯一键')
base_data =myclient.stock.history.find({"日期":today,"代码":{"$regex":"(^60\d*|^00\d*)"}},
{'代码':1,"收盘":1,"最新价格":1,"名称":1,"成交量":1,"换手率":1,"日期":1})
for tem in base_data:
    code = tem['代码']
    close_price = tem['收盘']
    tem_data = deepcopy(tem)
    main_force_data = myclient.stock.main_force.find({"代码":code,'日期':today},{'主力占比':1})
    for main_force_tem in main_force_data:
        tem_data['主力占比'] = main_force_tem['主力占比']
    # every_deal = myclient.stock.every_deal.find({"代码":code,'日期':today},{''})
    every_deal_win = myclient.stock.every_deal.aggregate([{
    "$match":   {
        "代码": code,
        "成交价格": {
            "$gt": close_price
        },
        "日期": today
    },
    }])
    every_deal_win_data = list(every_deal_win)
    if not every_deal_win_data:
        win_sum = 0
    else:   
        try:
            win_sum = pd.DataFrame(every_deal_win_data)['成交量'].sum()
        except Exception as why:
            print(why)

    every_deal_all = myclient.stock.every_deal.find({"代码":code,'日期':today},{})
    every_deal_all_data = list(every_deal_all)
    try:
        all_sum = pd.DataFrame(every_deal_all_data)['成交量'].sum()
    except Exception as e:
        all_sum = 1
        print(e)

    tem_data['收盘获益'] = round(win_sum/all_sum,4)
    tem_data['代码'] = code
    try:
        myclient.stock.aggregation.insert_one(tem_data)
    except pymongo.errors.DuplicateKeyError:
        continue



    
    
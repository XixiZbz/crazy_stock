import akshare as ak
import pymongo
import json
import arrow
myclient = pymongo.MongoClient('localhost', 27017)
collection = myclient.stock.profit
stock_profit_sheet_by_quarterly_em_df = ak.stock_profit_sheet_by_quarterly_em(symbol="SH600519")
column = list(stock_profit_sheet_by_quarterly_em_df)
column = [(c,pymongo.DESCENDING) for c in column[1:3]]
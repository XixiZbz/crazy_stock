from pprint import pprint
import pymongo
class DailySync:
    @classmethod
    def aggregation(self):
      myclient = pymongo.MongoClient('localhost', 27017)
      collection_aggregation = myclient.stock.aggregation
      collection_daily = myclient.stock.daily
      a = collection_daily.find({},{'代码':1,
          '名称':1,
          '最新价':1,
          '涨跌幅':1,
          '换手率':1})
      for x in a:
        try:
          collection_aggregation.insert_one(x)
        except pymongo.errors.DuplicateKeyError:
          continue
      
    @classmethod
    def main_force(self):
      myclient = pymongo.MongoClient('localhost', 27017)
      collection_main_force = myclient.stock.main_force
      collection_daily = myclient.stock.daily
      a = collection_daily.find({},{'代码':1,
          '名称':1})
      for x in a:
        try:
          collection_main_force.insert_one(x)
        except pymongo.errors.DuplicateKeyError:
          continue 
if __name__ == "__main__":
  DailySync.main_force()
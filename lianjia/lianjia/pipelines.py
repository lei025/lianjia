# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
# from scrapy.conf import settings
from lianjia.settings import MONGO_HOST, MONGO_PORT

class LianjiaPipeline(object):
    def process_item(self, item, spider):
        return item


class LianjiaVillageSavePipeline(object):
    def __init__(self):
        pass

    def process_item(self, item, spider):
        if spider.name == 'lianjia_spider':
            # client = pymongo.MongoClient(host=settings['MONGO_HOST'], port=settings['MONGO_PORT'])
            client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
            db = client['house']
            coll = db[item.collection]
            coll.insert(dict(item))

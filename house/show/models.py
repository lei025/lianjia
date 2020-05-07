from django.db import models

# Create your models here.
from mongoengine import *

connect('house', host='127.0.0.1', port=27017)



# class lianjia_village(Document):
#     _id = StringField()
#     id = StringField()
#     name = StringField()
#     address = StringField()
#     longitude = StringField()
#     latitude = StringField()
#     zone = StringField()
#     year = StringField()
#     build_type = StringField()
#     property_costs = StringField()
#     property_company = StringField()
#     developers = StringField()
#     buildings = StringField()
#     total_house = StringField()
#     采集时间 = StringField()
#
#     meta = {'collection':'lianjia_village'}


class lianjia_House(Document):
    _id = StringField()
    房屋Id = StringField()
    售价 = StringField()
    成交价 = StringField()
    标题 = StringField()
    小区 = StringField()
    小区ID = StringField()
    房屋户型 = StringField()
    所在楼层 = StringField()
    建筑类型 = StringField()
    房屋朝向 = StringField()
    装修情况 = StringField()
    建筑结构 = StringField()
    梯户比例 = StringField()
    产权年限 = StringField()
    配备电梯 = StringField()
    交易权属 = StringField()
    挂牌时间 = StringField()
    房屋年限 = StringField()
    房屋用途 = StringField()
    产权所属 = StringField()
    套内面积 = StringField()
    建筑面积 = StringField()
    户型结构 = StringField()
    抵押信息 = StringField()
    上次交易 = StringField()
    房本备件 = StringField()
    状态 = StringField()
    成交时间 = StringField()
    采集时间 = StringField()


    meta = {'collection':'lianjia_House'}

#
# for i in lianjia_House.objects[:10]:
#     print(i)




# class student(Document):
#     name = StringField()
#     age = StringField()
#     sex = StringField()
#
#     meta = {'collection': 'student'}

#
# for i in student.objects:
#     print(i.name)

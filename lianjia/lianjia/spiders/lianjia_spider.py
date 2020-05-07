# -*- coding: utf-8 -*-
import json
import re
import time
from datetime import datetime

import scrapy
import pymongo

from lianjia.items import LianjiaVillageItem,LianjiaHouseItem
from lianjia.settings import MONGO_HOST, MONGO_PORT

from scrapy_redis.spiders import RedisSpider


# class LianjiaSpiderSpider(scrapy.Spider):
class LianjiaSpiderSpider(RedisSpider):
    name = 'lianjia_spider'
    # allowed_domains = ['lianjia.com']
    # start_urls = ['https://wh.lianjia.com/']
    redis_key = 'lianjia'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.base_url = 'https://wh.lianjia.com'
        self.client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)


    def start_requests(self):
        request_url = 'https://wh.lianjia.com/xiaoqu/'
        yield scrapy.Request(url=request_url, callback=self.parse_district_links)

    def parse_district_links(self, response):
        """提取地区链接"""
        links = response.xpath("//div[@data-role='ershoufang']//a/@href").extract()
        for link in links:
            url = self.base_url + link
            yield scrapy.Request(url=url, callback=self.parse_bizcircle_links)

    def parse_bizcircle_links(self, response):
        """提取商圈链接"""
        links = response.xpath("//div[@data-role='ershoufang']/div[2]/a/@href").extract()
        for link in links:
            url = self.base_url + link
            yield scrapy.Request(url=url, callback=self.parse_village_list, meta={"ref": url})

    def parse_village_list(self, response):
        """提取小区链接"""

        links = response.xpath("//ul[@class='listContent']/li/a/@href").extract()
        for link in links:
            village_id = link.replace(self.base_url + '/xiaoqu/', '').replace('/', '')
            db = self.client['house']
            coll = db['lianjia_village']
            village = coll.find_one({'id': village_id})

            if village is None:
                # count = response.meta['count'] + 1
                yield scrapy.Request(url=link, callback=self.parse_village_detail)
            else:
                # 小区房源 https://wh.lianjia.com/ershoufang/c3620038190566370/
                url = self.base_url + "/ershoufang/c" + village_id + "/"
                yield scrapy.Request(url=url, callback=self.parse_house_list, meta={"ref": url})
                # 成交房源
                url = self.base_url + "/chengjiao/c" + village_id + "/"
                yield scrapy.Request(url=url, callback=self.parse_chouse_list, meta={"ref": url})

       # page
        page_data = response.xpath("//div[@class='page-box house-lst-page-box']/@page-data").extract_first()
        page_data = json.loads(page_data)
        if page_data['curPage'] < page_data['totalPage']:
            url = response.meta["ref"] + 'pg' + str(page_data['curPage'] + 1)
            yield scrapy.Request(url=url, callback=self.parse_village_list, meta=response.meta)


    def parse_village_detail(self, response):
        """提取小区详情"""
        count = 0
        village_url = response.url
        zone = response.xpath('//div[@class="xiaoquDetailbreadCrumbs"]/div[@class="fl l-txt"]/a/text()').extract()
        latitude = 0
        longitude = 0
        try:
            html = response.body.decode().replace('\r', '')
            local = html[html.find('resblockPosition:'):html.find('resblockName') - 1]
            m = re.search('(\d.*\d),(\d.*\d)', local)
            longitude = m.group(1)
            latitude = m.group(2)
        except Exception:
            pass

        item = LianjiaVillageItem()
        item['id'] = village_url.replace(self.base_url + '/xiaoqu/', '').replace('/', '')
        item['name'] = response.xpath('//h1[@class="detailTitle"]/text()').extract_first()
        item['address'] = response.xpath("//div[@class='detailHeader fl']/div[@class='detailDesc']/text()").extract_first()
        item['latitude'] = latitude
        item['longitude'] = longitude
        item['zone'] = ','.join(zone)
        item['year'] = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem'][1]/span[@class='xiaoquInfoContent']/text()").extract_first()
        item['build_type'] = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem'][2]/span[@class='xiaoquInfoContent']/text()").extract_first()
        item['property_costs'] = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem'][3]/span[@class='xiaoquInfoContent']/text()").extract_first()
        item['property_company'] = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem'][4]/span[@class='xiaoquInfoContent']/text()").extract_first()
        item['developers'] = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem'][5]/span[@class='xiaoquInfoContent']/text()").extract_first()
        item['buildings'] = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem'][6]/span[@class='xiaoquInfoContent']/text()").extract_first()
        item['total_house'] = response.xpath("//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem'][7]/span[@class='xiaoquInfoContent']/text()").extract_first()
        item['采集时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        # print(count,'\t',item['name'])
        print(item['name'])
        yield item
        # "//div[@class='xiaoquInfo']/div[@class='xiaoquInfoItem']/span[@class='xiaoquInfoContent']/text()"
        # 小区房源 https://wh.lianjia.com/ershoufang/c3620038190566370/
        url = self.base_url + "/ershoufang/c" + item['id'] + "/"
        yield scrapy.Request(url=url, callback=self.parse_house_list, meta={"ref": url})
        # 成交房源
        url = self.base_url + "/chengjiao/c" + item['id'] + "/"
        yield scrapy.Request(url=url, callback=self.parse_chouse_list, meta={"ref": url})

    def parse_house_list(self, response):
        """提取房源链接"""
        # 链家有时小区查询不到数据
        total = response.xpath("//div[@class='resultDes clear']/h2[@class='total fl']/span/text()").extract_first()
        total = int(total)
        if total > 0:
            # 提取房源链接
            links = response.xpath("//ul[@class='sellListContent']/li/div/div[@class='title']/a/@href").extract()
            for link in links:
                yield scrapy.Request(url=link, callback=self.parse_house_detail)
            # 链接分页
            page_data = response.xpath("//div[@class='page-box house-lst-page-box']/@page-data").extract_first()
            page_data = json.loads(page_data)
            if page_data['curPage'] == 1 and page_data['totalPage'] > 1:
                price = response.url.replace(self.base_url + '/ershoufang/', '')
                for x in range(2, page_data['totalPage'] + 1, 1):
                    url = self.base_url + '/ershoufang/' + 'pg' + str(x) + price
                    yield scrapy.Request(url=url, callback=self.parse_house_list)

    def parse_house_detail(self, response):
        """提取房源信息"""

        item = LianjiaHouseItem()
        item['房屋Id'] = response.url.replace(self.base_url + '/ershoufang/', '').replace('.html', '')
        item['标题'] = response.xpath("//div[@class='title']/h1[@class='main']/text()").extract_first()
        item['售价'] = response.xpath("//div[@class='content']/div[@class='price ']/span[@class='total']/text()").extract_first()
        item['小区'] = response.xpath("//div[@class='aroundInfo']/div[@class='communityName']/a/text()").extract_first()
        item['小区ID'] = response.xpath("//div[@class='aroundInfo']/div[@class='communityName']/a/@href").extract_first().replace('/xiaoqu/', '').replace('/', '')
        item['房屋户型'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[1]/text()").extract_first()
        item['所在楼层'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[2]/text()").extract_first()
        item['建筑面积'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[3]/text()").extract_first()
        item['户型结构'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[4]/text()").extract_first()
        item['套内面积'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[5]/text()").extract_first()
        item['建筑类型'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[6]/text()").extract_first()
        item['房屋朝向'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[7]/text()").extract_first()
        item['建筑结构'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[8]/text()").extract_first()
        item['装修情况'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[9]/text()").extract_first()
        item['梯户比例'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[10]/text()").extract_first()
        item['配备电梯'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[11]/text()").extract_first()
        item['产权年限'] = response.xpath("//div[@class='introContent']/div[@class='base']//li[12]/text()").extract_first()
        item['挂牌时间'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[1]/span[2]/text()").extract_first()
        item['交易权属'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[2]/span[2]/text()").extract_first()
        item['上次交易'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[3]/span[2]/text()").extract_first()
        item['房屋用途'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[4]/span[2]/text()").extract_first()
        item['房屋年限'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[5]/span[2]/text()").extract_first()
        item['产权所属'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[6]/span[2]/text()").extract_first()
        item['抵押信息'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[7]/span[2]/text()").extract_first()
        item['房本备件'] = response.xpath("//div[@class='introContent']/div[@class='transaction']//li[8]/span[2]/text()").extract_first()
        item['状态'] = '在售'
        item['采集时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        yield item

    def parse_chouse_list(self, response):
        """提取成交房源链接"""
        # 链家有时小区查询不到数据
        total = response.xpath("//div[@class='resultDes clear']/div[@class='total fl']/span/text()").extract_first()
        total = int(total)
        if total > 0:
            # 提取房源链接
            links = response.xpath("//ul[@class='listContent']/li/div[@class='info']/div[@class='title']/a/@href").extract()
            for link in links:
                yield scrapy.Request(url=link, callback=self.parse_chouse_detail)
            # 链接分页
            page_data = response.xpath("//div[@class='page-box house-lst-page-box']/@page-data").extract_first()
            page_data = json.loads(page_data)
            if page_data['curPage'] == 1 and page_data['totalPage'] > 1:
                price = response.url.replace(self.base_url + '/chengjiao/', '')
                for x in range(2, page_data['totalPage'] + 1, 1):
                    url = self.base_url + '/chengjiao/' + 'pg' + str(x) + price
                    yield scrapy.Request(url=url, callback=self.parse_chouse_list)

    def parse_chouse_detail(self, response):
        """提取成交房源信息"""
        house_id = response.url.replace(self.base_url + '/chengjiao/', '').replace('.html', '')
        db = self.client['house']
        coll = db['lianjia_House']
        house = coll.find_one({'房屋Id': house_id, '状态': '成交'})

        if house is None:
            house = coll.find_one({'房屋Id': house_id})
            item = LianjiaHouseItem()
            item['房屋Id'] = house_id
            item['售价'] = response.xpath("//section[@class='wrapper']//div[@class='msg']/span[1]/label/text()").extract_first()
            item['成交价'] = response.xpath("//section[@class='wrapper']//div[@class='price']/span[@class='dealTotalPrice']/i/text()").extract_first()

            if house is None:
                item['标题'] = response.xpath("//div[@class='house-title']/div[@class='wrapper']/text()").extract_first()
                item['小区'] = response.xpath("//section[@class='wrapper']/div[@class='deal-bread']/a[5]/text()").extract_first().replace('二手房成交', '')
                item['小区ID'] = response.xpath("//div[@class='house-title']/@data-lj_action_housedel_id").extract_first()
                item['房屋户型'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[1]/text()").extract_first()
                item['所在楼层'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[2]/text()").extract_first()
                item['建筑面积'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[3]/text()").extract_first()
                item['户型结构'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[4]/text()").extract_first()
                item['套内面积'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[5]/text()").extract_first()
                item['建筑类型'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[6]/text()").extract_first()
                item['房屋朝向'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[7]/text()").extract_first()
                item['装修情况'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[9]/text()").extract_first()
                item['建筑结构'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[10]/text()").extract_first()
                item['梯户比例'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[12]/text()").extract_first()
                item['产权年限'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[13]/text()").extract_first()
                item['配备电梯'] = response.xpath("//div[@id='introduction']//div[@class='base']/div[@class='content']/ul/li[14]/text()").extract_first()
                item['交易权属'] = response.xpath("//div[@id='introduction']//div[@class='transaction']/div[@class='content']/ul/li[2]/text()").extract_first()
                item['挂牌时间'] = response.xpath("//div[@id='introduction']//div[@class='transaction']/div[@class='content']/ul/li[3]/text()").extract_first()
                item['房屋用途'] = response.xpath("//div[@id='introduction']//div[@class='transaction']/div[@class='content']/ul/li[4]/text()").extract_first()
                item['房屋年限'] = response.xpath("//div[@id='introduction']//div[@class='transaction']/div[@class='content']/ul/li[5]/text()").extract_first()
                item['产权所属'] = response.xpath("//div[@id='introduction']//div[@class='transaction']/div[@class='content']/ul/li[6]/text()").extract_first()
            else:
                item['标题'] = house['标题']
                item['小区'] = house['小区']
                item['小区ID'] = house['小区ID']
                item['房屋户型'] = house['房屋户型']
                item['所在楼层'] = house['所在楼层']
                item['建筑面积'] = house['建筑面积']
                item['户型结构'] = house['户型结构']
                item['套内面积'] = house['套内面积']
                item['建筑类型'] = house['建筑类型']
                item['房屋朝向'] = house['房屋朝向']
                item['建筑结构'] = house['建筑结构']
                item['装修情况'] = house['装修情况']
                item['梯户比例'] = house['梯户比例']
                item['配备电梯'] = house['配备电梯']
                item['产权年限'] = house['产权年限']
                item['挂牌时间'] = house['挂牌时间']
                item['交易权属'] = house['交易权属']
                item['上次交易'] = house['上次交易']
                item['房屋用途'] = house['房屋用途']
                item['房屋年限'] = house['房屋年限']
                item['产权所属'] = house['产权所属']
                item['抵押信息'] = house['抵押信息']
                item['房本备件'] = house['房本备件']

            item['状态'] = '成交'
            item['成交时间'] = datetime.strptime(response.xpath("//div[4]/div/span/text()").extract_first().replace(' 成交', ''), '%Y.%m.%d').strftime('%Y-%m-%d')
            item['采集时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # item['成交时间'] = datetime.strptime(sel.css('.house-title div span::text').extract_first().replace(' 成交', ''), '%Y.%m.%d').strftime('%Y-%m-%d')

            yield item





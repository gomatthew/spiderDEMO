# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst
from hashlib import md5
from elasticsearch_dsl.connections import connections
from models.es_types import BeikeType

es = connections.create_connection(BeikeType._doc_type.using)


# def gen_sugesst(index, info_tuple):
# 	# 根据传入字符串生成搜索建议
# 	used_words = set()
# 	sugests = []
# 	for text, weight in info_tuple:
# 		if text:
# 			# 调用es的analys接口分析字符串
# 			word = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter': ["lowercase"]}, body=text)
# 			analyzed_words = set([r["token"] for r in word if len(r["token"]) > 1])
# 			used_words = analyzed_words - used_words
# 		else:
# 			new_words = set()
# 		if new_words:
# 			sugests.append({"input": list(new_words), "weight": weight})
# 	return sugests


def get_md5(value):
	if isinstance(value, str):
		value = value.encode("utf-8")
	m = md5()
	m.update(value)
	return m.hexdigest()


class DataLoader(ItemLoader):
	'''
	写自定义逻辑
	'''
	default_output_processor = TakeFirst()


# 转换平方米
def convert_square(value):
	return value.split("㎡")[0]


# 去空格
def convert_strip(value):
	return value.strip()


def convert_date(value):
	from datetime import date
	try:
		if isinstance(value, str):
			value = value.strip()
			if value == "暂无数据":
				return str(datetime.now())
		else:
			value = value[0].strip()
		year, others = value.split("年")
		month, others = others.split("月")
		day = others.split("日")[0]
		return date(int(year), int(month), int(day))
	except Exception as e:
		return date(2020,12,10)


def convert_city(value):
	if isinstance(value, str):
		return value.replace("房产", "").strip()
	else:
		pass


class BeiKeItem(scrapy.Item):
	city = scrapy.Field(output_processor=MapCompose(convert_city))  # 城市
	title = scrapy.Field()  # 标题
	url = scrapy.Field()  # URL
	url_id = scrapy.Field()  # URL_ID
	describe = scrapy.Field(input_processor=MapCompose(convert_strip))  # 房源描述
	total_price = scrapy.Field()  # 总价
	unit_price = scrapy.Field()  # 单价
	house_type = scrapy.Field()  # 户型
	community = scrapy.Field()  # 小区
	area = scrapy.Field()  # 详细坐标
	district = scrapy.Field()  # 行政区
	market_time = scrapy.Field(output_processor=convert_date)  # 挂牌时间
	last_market_time = scrapy.Field(output_processor=convert_date)  # 上次成交时间
	square = scrapy.Field(input_processor=MapCompose(convert_square))  # 面积
	floor = scrapy.Field()  # 楼层
	direction = scrapy.Field()  # 朝向
	createtime = scrapy.Field()  # 爬取时间
	location = scrapy.Field()  # 详细位置

	# traffic=scrapy.Field() #交通
	# arround=scrapy.Field() # 周边配套
	# core_future =scrapy.Field()# 核心卖点

	def get_insert_sql(self):
		insert_sql = '''
        INSERT INTO beike (city,title,url,url_id,describe_info,total_price,unit_price,house_type,community,area,district,market_time,last_market_time,square,floor,direction,createtime,location)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE createtime=VALUES(createtime)
        '''
		params = (
			self.get("city", ""),
			self.get("title", ""),
			self.get("url", ""),
			self.get("url_id", ""),
			self.get("describe", ""),
			self.get("total_price", ""),
			self.get("unit_price", ""),
			self.get("house_type", ""),
			self.get("community", ""),
			self.get("area", ""),
			self.get("district", ""),
			self.get("market_time", ""),
			self.get("last_market_time", ""),
			self.get("square", ""),
			self.get("floor", ""),
			self.get("direction", ""),
			self.get("createtime", ""),
			self.get("location"),
		)

		return insert_sql, params


def convertLianjia_city(value):
	if isinstance(value, str):
		return value.replace("房产网", "")


def join_Lianjia_area(value):
	if isinstance(value, list):
		return "".join(value)


class LianjiaItem(scrapy.Item):
	city = scrapy.Field(output_processor=MapCompose(convertLianjia_city))  # 城市
	title = scrapy.Field()  # 标题
	url = scrapy.Field()  # URL
	url_id = scrapy.Field()  # URL_ID
	total_price = scrapy.Field()  # 总价
	unit_price = scrapy.Field()  # 单价
	house_type = scrapy.Field()  # 户型
	community = scrapy.Field()  # 小区
	area = scrapy.Field(input_processor=join_Lianjia_area)  # 详细坐标
	district = scrapy.Field()  # 行政区
	market_time = scrapy.Field()  # 挂牌时间
	last_market_time = scrapy.Field(output_processor=convert_date)  # 上次成交时间
	square = scrapy.Field(input_processor=MapCompose(convert_square))  # 面积
	floor = scrapy.Field()  # 楼层
	direction = scrapy.Field()  # 朝向
	createtime = scrapy.Field()  # 爬取时间

	location = scrapy.Field()  # 详细位置

	def get_insert_sql(self):
		insert_sql = '''
        INSERT INTO lianjia (city,title,url,url_id,total_price,unit_price,house_type,community,area,district,market_time,last_market_time,square,floor,direction,createtime,location)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE createtime=VALUES(createtime)
        '''
		params = (
			self.get("city", ""),
			self.get("title", ""),
			self.get("url", ""),
			self.get("url_id", ""),
			self.get("total_price", ""),
			self.get("unit_price", ""),
			self.get("house_type", ""),
			self.get("community", ""),
			self.get("area", ""),
			self.get("district", ""),
			self.get("market_time", ""),
			self.get("last_market_time", ""),
			self.get("square", ""),
			self.get("floor", ""),
			self.get("direction", ""),
			self.get("createtime", ""),
			self.get("location"),
		)

		return insert_sql, params


# def save_to_es(self):
# 	beike = BeikeType()
# 	beike.city = self["city"]
# 	beike.title = self["title"]
# 	beike.url = self["url"]
# 	beike.meta.id = self["url_id"]
# 	beike.describe = self["describe"]
# 	beike.total_price = self["total_price"]
# 	beike.unit_price = self["unit_price"]
# 	beike.house_type = self["house_type"]
# 	beike.community = self["community"]
# 	beike.area = self["area"]
# 	beike.district = self["district"]
# 	beike.market_time = self["market_time"]
# 	beike.last_market_time = self["last_market_time"]
# 	beike.square = self["square"]
# 	beike.floor = self["floor"]
# 	beike.direction = self["direction"]
# 	beike.createtime = self["createtime"]
# 	beike.suggest = gen_sugesst(BeikeType._doc_type.index, ((beike.title, 10), (beike.describe, 7)))
# 	beike.save()
# 	return


if __name__ == '__main__':
	from datetime import date

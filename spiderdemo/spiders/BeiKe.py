# -*- coding: utf-8 -*-
import json
import scrapy
from datetime import datetime
from urllib import parse
from scrapy import Selector
from scrapy.http import Request
from spiderdemo.items import BeiKeItem, DataLoader, get_md5
from scrapy import Selector


class BeikeSpider(scrapy.Spider):
	name = 'BeiKe'
	allowed_domains = ['ke.com']
	start_urls = ['https://sz.ke.com/ershoufang/pg1/']

	def parse(self, response):
		base_url = "https://sz.ke.com/ershoufang/pg{}/"
		res = response.xpath('//div[@data-component="list"]/ul//li/a/@href').extract()
		nodes = res[:-7]
		for i in nodes:
			if not i.endswith("html"):
				res.remove(i)
		for node in nodes:
			yield Request(url=node, callback=self.parse_detail)

		page_number = response.xpath("//*[@class='page-box house-lst-page-box']//@page-data").extract_first()
		page_info = json.loads(page_number)
		cur_page = page_info["curPage"]
		total_page = page_info["totalPage"]
		if cur_page != total_page:
			yield Request(base_url.format(cur_page + 1))

	def parse_detail(self, response):
		city = response.xpath("//*[@class='fl l-txt']/a[1]/text()").extract_first().replace("房产", "")
		area = response.xpath('//div[@class="areaName"]/span/a[2]/text()').extract_first()
		district = response.xpath('//div[@class="areaName"]/span/a/text()').extract_first()
		community = response.xpath('//div[@class="communityName"]/a[1]/text()').extract_first()
		item_loader = DataLoader(item=BeiKeItem(), response=response)
		item_loader.add_value("city", "上海")
		item_loader.add_xpath("title", "//title/text()")
		item_loader.add_value("url", response.url)
		item_loader.add_value("url_id", get_md5(response.url))
		item_loader.add_xpath("total_price", '//div[@class="price "]/span[@class="total"]/text()')
		item_loader.add_xpath("unit_price", '//div[@class="unitPrice"]/span/text()')
		item_loader.add_xpath("house_type", '//div[@class="room"]/div/text()')
		item_loader.add_xpath("community", '//div[@class="communityName"]/a[1]/text()')
		item_loader.add_xpath("district", '//div[@class="areaName"]/span/a/text()')
		item_loader.add_xpath("area", '//div[@class="areaName"]/span/a[2]/text()')
		item_loader.add_xpath("market_time", '//div[@class="transaction"]//li[1]/text()')
		item_loader.add_xpath("last_market_time", '//div[@class="transaction"]//li[3]/text()')
		item_loader.add_xpath("square", '//div[@class="base"]/div[2]//li[3]/text()')
		item_loader.add_xpath("floor", '//div[@class="base"]/div[2]/ul/li[2]/text()')
		item_loader.add_xpath("direction", '//div[@class="base"]/div[2]/ul/li[6]/text()')
		item_loader.add_xpath("describe", '//div[@class="introContent showbasemore"]/div[2]/div[2]/text()')
		item_loader.add_value("createtime", str(datetime.now()))
		item_loader.add_value("location",area + district + community)
		data = item_loader.load_item()

		yield data
		# item_loader.add_xpath("traffic", "")
		# item_loader.add_xpath("arround", "")
		# item_loader.add_xpath("core_future", "")


if __name__ == '__main__':
	# from spiderdemo.items import html
	#
	# response = Selector(text=html)
	# res = response.xpath('//div[@class="introContent showbasemore"]/div[2]/div[2]/text()').extract()

	# print(res)
	print(datetime.now())
	# print(response.css(".sellListContent").extract())
	# print(response.xpath('//ul[@class="sellListContent"]').extract())

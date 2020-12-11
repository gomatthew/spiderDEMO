import json
import scrapy
from datetime import datetime
from scrapy.http import Request
from spiderdemo.items import LianjiaItem, DataLoader,get_md5


class LianjiaSpider(scrapy.Spider):
	name = 'Lianjia'
	allowed_domains = ['lianjia.com']
	start_urls = ['https://sz.lianjia.com/ershoufang/pg1/']

	def parse(self, response):
		pages = json.loads(response.xpath('//div[@class="page-box house-lst-page-box"]/@page-data').extract_first())
		next_page = 'https://sz.lianjia.com/ershoufang/pg{}/'.format(pages["curPage"]+1)
		# 提取详情页节点
		nodes = response.xpath('//ul[@class="sellListContent"]/li//div[@class="title"]/a/@href').extract()
		for node in nodes:
			yield Request(url=node, callback=self.parse_detail)
		yield Request(url=next_page, callback=self.parse)

	def parse_detail(self, response):
		item_loader = DataLoader(item=LianjiaItem(), response=response)
		city = response.xpath("//div[@class='intro clear']//a[1]/text()").extract_first().replace("房产网","")
		area = response.xpath("//div[@class='areaName']/span[2]/a/text()").extract_first()
		area = "".join(area)
		community= response.xpath("//div[@class='communityName']/a[1]/text()").extract_first()
		item_loader.add_value("city", city)
		item_loader.add_xpath("title", "//div[@class='title']/h1/@title")
		item_loader.add_value("url", response.url)
		item_loader.add_value("url_id",get_md5(response.url) )
		item_loader.add_xpath("total_price","//div[@class='price ']/span/text()" )
		item_loader.add_xpath("unit_price", "//span[@class='unitPriceValue']/text()")
		item_loader.add_xpath("house_type","//div[@class='introContent']//li[1]/text()" )
		item_loader.add_xpath("community","//div[@class='communityName']/a[1]/text()" )
		item_loader.add_xpath("area", "//div[@class='areaName']/span[2]/a/text()")
		item_loader.add_xpath("district","//div[@class='areaName']/span[2]/a/text()" )
		item_loader.add_xpath("market_time", "//div[@class='introContent']/div[@class='transaction']//span[2]/text()")
		item_loader.add_xpath("last_market_time",'//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li[3]/span[2]/text()' )
		item_loader.add_xpath("square",'//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[3]/text()' )
		item_loader.add_xpath("floor",'//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[2]/text()' )
		item_loader.add_xpath("direction",'//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[7]/text()' )
		item_loader.add_value("createtime", str(datetime.now()))
		item_loader.add_value("location",area+community)
		data = item_loader.load_item()
		yield data
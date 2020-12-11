# -*- coding: utf-8 -*-
import MySQLdb
import MySQLdb.cursors
from twisted.enterprise import adbapi
# from models.es_types import BeikeType


class MySQLTwistPipline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset="utf8",
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        # 使用Twisted 将插入MySQSL变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)
        return item

    def handle_error(self, failure, item, spider):
        # 进行数据异常处理，目前打印在公屏
        print(failure)

    def do_insert(self, cursor, item):
        # 执行SQL语句
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)


class ElasticsearchPipeline(object):
    # 将数据写入es中
    def process_item(self, item, spider):
        # 将item转化为es
        item.save_to_es()
        return item


if __name__ == '__main__':
    pass

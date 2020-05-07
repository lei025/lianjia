# -*- coding: utf-8 -*-
# @Time    : 20-2-27 下午5:57
# @Author  : MaoLei
# @Email   : maolei025@qq.com
# @File    : run.py.py
# @Software: PyCharm


from scrapy import cmdline

cmdline.execute("scrapy crawl lianjia_spider".split())
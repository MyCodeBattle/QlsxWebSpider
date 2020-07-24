import scrapy
import time
import arrow
import os
from lxml import etree
from selenium import webdriver
from QlsxWebScan.autodownload import GoGoGo
import pandas as pd
from threading import Lock


class QlsxWebSpider(scrapy.Spider):
    l = Lock()

    name = 'QlsxWebSpider'
    url = 'http://www.zjzwfw.gov.cn/zjservice/item/detail/index.do?localInnerCode={}'

    #模板header
    __HDADERS_TEMPLATE = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'Cookie': 'acw_sc__v2={};'
    }
    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'Cookie': 'acw_sc__v2={};'
    }
    __acw = ''
    __wait = arrow.get('2019-12-05 12:00:00')


    def start_requests(self):
        crawlFilename = '{}/事项表/0723市本级许可.xls'.format(os.getcwd())
        df = pd.read_excel(os.path.join(os.getcwd(), crawlFilename), sheet_name='Sheet1')
        for ic in df['权力内部编码']:
            ## -------------------------------------------------#
            # if ic != 'cef8e360-d18d-4928-a977-1778e1fb58a5':
            #     continue

            ##------------------------------------------------#


            yield scrapy.Request(self.url.format(ic), headers=self.HEADERS, method='GET', callback=self.parse, meta={'innerCode': ic}, dont_filter=True)

    def __refresh(self):
        try:
            self.l.acquire()
            if self.__wait and (arrow.now() - self.__wait).seconds < 100:
                print('间隔太短，不取了')
                return
            print('开始刷新')
            r = GoGoGo.GoGoGo()
            self.__acw = r.getAcwscv2()
            self.HEADERS['Cookie'] = self.__HDADERS_TEMPLATE['Cookie'].format(self.__acw)
            r.close()
            print('拿到acw={}'.format(self.__acw))
            self.__wait = arrow.now()
        finally:
            self.l.release()

    def __getMaterials(self, response, ic):
        # 判断是否分类材料，能的话需要重新请求
        et = etree.HTML(response)
        isMaterialSplit = et.xpath('//*[@id="sbcl"]//div[@class="apply_material"]')
        if isMaterialSplit:
            # 拿到impleCode
            impleCode = et.xpath('//*[@id="impleCode"]/@value')[0]
            totalMaterialValues = ';'.join(et.xpath('//div[@class="apply_material"]//li//@value'))
            yield scrapy.Request('http://www.zjzwfw.gov.cn/zjservice/item/detail/searchMateriel.do?linkedStr={}&impleCode={}'.format(totalMaterialValues, impleCode), callback=self.materialParse, headers=self.HEADERS, meta={'ic': ic, 'reqValues': totalMaterialValues, 'impleCode': impleCode})

    def materialParse(self, response):
        ic = response.meta['ic']
        reqValues = response.meta['reqValues']
        DIR = '{}/数据/{}_material'.format(os.getcwd(), ic)
        if '申请材料' not in response.text:
            self.__refresh()
            yield scrapy.Request('http://www.zjzwfw.gov.cn/zjservice/item/detail/searchMateriel.do?linkedStr={}&impleCode={}'.format(reqValues, response.meta['impleCode']), callback=self.materialParse, headers=self.HEADERS, meta=response.meta, dont_filter=True)

        with open(DIR, 'w', encoding='utf-8') as fp:
            fp.write(response.text)

    def parse(self, response):
        ic = response.meta['innerCode']
        DIR = '{}/数据/{}'.format(os.getcwd(), ic)
        if '事项名称' not in response.text:
            self.__refresh()
            yield scrapy.Request(self.url.format(ic), dont_filter=True, headers=self.HEADERS, method='GET', callback=self.parse, meta={'innerCode': ic})


        # yield from self.__getMaterials(response.text, ic)
        #

        with open(DIR, 'w', encoding='utf-8') as fp:
            fp.write(response.text)

from selenium import webdriver
import time
import os

class GoGoGo:

    __chrome_options = webdriver.ChromeOptions()
    __chrome_options.add_argument('--headless')
    __chrome_options.add_argument('--no-sandbox')
    __chrome_options.add_argument('--disable-gpu')
    __chrome_options.add_argument('--disable-dev-shm-usage')
    __browser = None

    def __init__(self):
        self.__browser = webdriver.Chrome(os.path.join(os.getcwd(), 'chromedriver'), options=self.__chrome_options)

    def __waitElement(self, xpath):
        while True:
            try:
                return self.__browser.find_element_by_xpath(xpath)
            except:
                time.sleep(0.5)

    def getAcwscv2(self):
        url = 'http://www.zjzwfw.gov.cn/zjservice/item/detail/index.do?localInnerCode=3745047b-b63f-4bd7-9236-9c6dcfef8be1&webId=28'
        self.__browser.get(url)
        while not self.__browser.get_cookie('acw_sc__v2'):
            time.sleep(0.5)

        return self.__browser.get_cookie('acw_sc__v2')['value']

    def close(self):
        self.__browser.close()

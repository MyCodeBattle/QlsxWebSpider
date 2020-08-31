from lxml import etree
import re
from typing import List
import os
from functools import reduce
import pandas as pd
import arrow
import traceback
from tqdm import tqdm


class AnalyseData:
    # 要分析的事项列表xls
    __analyseFilename = '../../事项表/0831total.xls'
    with open('部门编码地区映射', 'r', encoding='utf-8') as fp:
        __areaList = fp.readlines()

    def regionMap(self, code: str):

        for c in self.__areaList:
            tmp = c.split()
            if code.startswith(tmp[1].strip()):
                return tmp[0]

    def run(self):
        df = pd.read_excel(self.__analyseFilename, sheet_name='Sheet1', dtype=str).fillna('')
        df['区县'] = df['组织编码（即部门编码）'].apply(lambda e: self.regionMap(e))
        res1 = []
        with tqdm(total=df.shape[0], ncols=200) as pbar:
            for idx, row in df.iterrows():
                try:
                    with open('{}/../../数据/{}'.format(os.getcwd(), row['权力内部编码']), 'r', encoding='utf-8') as fp:
                        tmp = fp.read()
                        tmp = tmp.replace('<br>', '\n')
                        et = etree.HTML(tmp)
                        baseInfo = self.produce(et, row)
                        res1.append(baseInfo)
                except Exception as e:
                    pass
                pbar.update(1)

            ddf = pd.DataFrame(res1)
            w = pd.ExcelWriter('test.xls')
            ddf.to_excel(w, '事项信息', index=False)
            w.save()
            w.close()

    def joinStrip(self, lis):
        return ''.join([x.strip() for x in lis])

    def produce(self, et, row):

        jbxx = list(filter(lambda x: x not in ['事项信息', '办事信息', '结果信息'], map(lambda x: x.strip(), et.xpath('//div[@class="jbxx_tables"]//td/div/text()'))))
        newJbxx = []
        for i in range(0, len(jbxx) - 1):
            if jbxx[i] != jbxx[i + 1]:
                newJbxx.append(jbxx[i])
        jbxx = newJbxx

        #组装基本信息
        jbxxDic = {'内部编码': row['权力内部编码'], '部门名称': row['部门名称'], '部门编码': row['组织编码（即部门编码）'], '事项名称': row['权力名称'], '权力基本码': row['权力基本码'], '区县': row['区县']}

        for i in range(0, len(jbxx), 2):
            # print('{} {}\n'.format(jbxx[i], jbxx[i+1]))
            if jbxx[i] not in jbxxDic and jbxx[(i + 1) % len(jbxx)] not in jbxxDic:
                jbxxDic[jbxx[i]] = jbxx[(i + 1) % len(jbxx)]

        if et.xpath('//a[contains(text(),"样本下载")]'):
            jbxxDic['审批结果样本'] = '有样本'
        # 法定办结时限和承诺办结期限
        totalCompleteTime = et.xpath('//table[@id="table1"]//div[contains(text(), "法定办结时限")]/../..//span/text()')
        lowComplete = totalCompleteTime[0].strip()
        curComplete = totalCompleteTime[1].strip()
        jbxxDic['法定办结时限'] = lowComplete
        jbxxDic['承诺办结时限'] = curComplete
        jbxxDic['承诺期限数字'] = 0 if '即办' in curComplete else int(re.findall(r'\d+', curComplete)[0])

        # 具体地址
        address = ''.join([k.strip() for k in et.xpath('//span[contains(text(), "具体地址")]/..//span[@class="Cons"]/text()')])
        jbxxDic['具体地址'] = address

        # 工作时间
        workTime = ''.join([k.strip() for k in et.xpath('//span[contains(text(), "办理时间")]/..//span[@class="Cons"]/text()')])
        jbxxDic['工作时间'] = workTime

        # 联系方式
        phone = ''.join([k.strip() for k in et.xpath('//span[contains(text(), "联系电话")]/..//span[@class="Cons"]/text()')])
        jbxxDic['联系方式'] = phone

        # 有表格的 办理环节，如果空就是不是表格形式
        applyLink = list(filter(lambda x: x.strip(), [''.join(x.split()) for x in et.xpath('//div[@class="bllc_con"]//td[1]//text()')]))
        # print(rowspan)
        jbxxDic['办理环节'] = ''.join(applyLink)
        jbxxDic['办理环节数'] = len(applyLink) - 1
        # print(applyLink)
        
        #表格里的时间和承诺时间对比
        tableSum = self.__getTableSum(et)
        jbxxDic['表格流程时间和'] = tableSum


        # 是否收费
        needMoney = ''.join(et.xpath('//div[@class="sfsf"]//div[@class="sfyjCon"]//text()')).strip()
        # print(needMoney)
        jbxxDic['是否收费'] = needMoney

        # 收费依据
        chargeTicket = ''.join(et.xpath('//div[@class="sfyj"]//div[@class="sfyjCon"]//text()')).strip()
        # print(chargeTicket)
        jbxxDic['收费依据'] = chargeTicket

        # 是否支持网上支付
        webCharge = ''.join(et.xpath('//div[@class="sfzccwszf"]//div[@class="sfyjCon"]//text()')).strip()
        jbxxDic['是否支持网上支付'] = webCharge

        # 收费项目
        chargeItems = reduce(lambda x, y: x + y, map(lambda x: x.strip(), et.xpath('//div[@class="sfbz_tables"]//span[@class="sfxmmc_cons clearfix"]//text()')), '')
        jbxxDic['收费项目'] = chargeItems
        # 常见问题
        faq = reduce(lambda x, y: x + y, map(lambda x: x.strip(), et.xpath('//div[@class="cjwt_table"]//text()')), '')
        # print(faq)
        jbxxDic['常见问题'] = faq

        # 咨询电话
        askPhone = ''.join(et.xpath('//div[@class="zxfs"]//p[@class="zxdh clearfix"]/span[@class="zxfsCon"]//text()')).strip()
        jbxxDic['咨询电话'] = askPhone

        # print(askPhone)

        # 咨询地址
        askAddress = ''.join(et.xpath('//div[@class="zxfs"]//p[@class="zxdz clearfix"]/span[@class="zxfsCon"]//text()')).strip()
        jbxxDic['咨询地址'] = askAddress

        # 投诉电话
        complainPhone = ''.join(et.xpath('//div[@class="jdtsfs"]//p[@class="zxdh clearfix"]/span[@class="jdtsfsCon"]//text()')).strip()
        # print(complainPhone)
        jbxxDic['投诉电话'] = complainPhone

        # 投诉地址
        complainAddress = ''.join(et.xpath('//div[@class="jdtsfs"]//p[@class="zxdz clearfix"]/span[@class="jdtsfsCon"]//text()')).strip()
        jbxxDic['投诉地址'] = complainAddress

        #咨询网址
        askLink = ''.join(et.xpath('//*[@id="zxfs"]/p[3]/a/@href'))
        jbxxDic['咨询网址'] = askLink

        #投诉网址
        complainLink = ''.join(et.xpath('//*[@id="jdtsfs"]/p[3]/a/@href'))
        jbxxDic['投诉网址'] = complainLink

        return jbxxDic

    def analyse(self):
        df = pd.read_excel('test.xls', sheet_name='事项信息', dtype=str).fillna('')
        # df[['省级法律依据', '国家法律依据', '工作时间', '审批结果名称']] = df[['省级法律依据', '国家法律依据', '工作时间', '审批结果名称']].fillna('').astype(str)
        df = df.drop(labels=df[df['事项名称'] == '无'].index)
        totRes = []
        totalErrorDetails = []
        errorDf = pd.DataFrame()    #一个一个error的df

        for index, row in df.iterrows():

            error = ''
            errorList = []
            idx = 1
            # 承诺办结时间为即办，办件类型必须为即办件，事项审查类型为即审即办
            if row['承诺办结时限'] == '即办':
                if row['办件类型'] != '即办件' or row['事项审查类型'] != '即审即办':
                    error += '{}. 承诺办结时间为即办，办件类型必须为即办件，事项审查类型为即审即办\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '办结时间和办件类型不对应', 'ERROR_DESCRIPTION': ' 承诺办结时间为即办，办件类型必须为即办件，事项审查类型为即审即办'})

            # 承诺办结时间非即办，办件类型必须为承诺件，事项审查类型为先审后批
            elif row['承诺办结时限'] != '即办':
                if row['办件类型'] != '承诺件' or row['事项审查类型'] != '前审后批':
                    error += '{}. 承诺办结时间非即办，办件类型必须为承诺件，事项审查类型为前审后批\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '办结时间和办件类型不对应', 'ERROR_DESCRIPTION': '承诺办结时间非即办，办件类型必须为承诺件，事项审查类型为前审后批'})


            if '批复' in row['审批结果名称'] or '批文' in row['审批结果名称']:
                if row['审批结果类型'] != '审批办结':
                    error += '{}. 如审批结果名称为XX批复则审批结果类型为审批办结\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '审批结果名称和审批结果类型不对应', 'ERROR_DESCRIPTION': '如审批结果名称为XX批复则审批结果类型为审批办结'})

            # 2、如审批结果名称为XX证则审批结果类型为出证办结
            elif (row['审批结果名称'].endswith('证') or '证书' in row['审批结果名称']) and \
                    '凭证' not in row['审批结果名称'] and '证明' not in row['审批结果名称']:
                if row['审批结果类型'] != '出证办结':
                    error += '{}. 如审批结果名称为XX证则审批结果类型为出证办结\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '审批结果名称和审批结果类型不对应', 'ERROR_DESCRIPTION': '如审批结果名称为XX证则审批结果类型为出证办结'})

            # 2、如审批结果名称为XX文则审批结果类型为出文办结
            elif row['审批结果名称'].endswith('文') or ('文' in row['审批结果名称'] and '批文' not in row['审批结果名称']):
                if row['审批结果类型'] != '出文办结':
                    error += '{}. 如审批结果名称为XX文则审批结果类型为出文办结\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '审批结果名称和审批结果类型不对应', 'ERROR_DESCRIPTION': '如审批结果名称为XX文则审批结果类型为出文办结'})

            elif row['审批结果名称'] and row['审批结果名称'] != '无' and (not row['审批结果类型'] or row['审批结果类型'] == '无'):
                error += '{}. 有审批结果一定要有审批结果类型\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '无审批结果类型', 'ERROR_DESCRIPTION': '有审批结果一定要有审批结果类型'})

            # 到办事现场次数非0次事项需填写原因说明
            if row['到办事现场次数'] == '':
                error += '{}. 需填写到现场次数\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '无到现场次数', 'ERROR_DESCRIPTION': '需填写到现场次数'})
            elif row['到办事现场次数'] != '0次':
                if not row['必须现场办理原因说明'] or row['必须现场办理原因说明'] == '无需到现场办理':
                    error += '{}. 到办事现场次数非0次事项需填写原因说明\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '需要跑现场无说明', 'ERROR_DESCRIPTION': '到办事现场次数非0次事项需填写原因说明'})

            # 跑0次事项除非有特殊原因外不必填写原因说明
            elif row['到办事现场次数'] == '0次':
                if (row['必须现场办理原因说明'] != '无' or row['必须现场办理原因说明'] != '') \
                        and ('现场' in row['必须现场办理原因说明'] and '无需现场' not in row['必须现场办理原因说明'] and row['必须现场办理原因说明'] != '无需到现场办理'):
                    error += '{}. 跑0次事项除非有特殊原因外不必填写原因说明\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '跑零次事项不应有到现场办理原因说明', 'ERROR_DESCRIPTION': '跑0次事项除非有特殊原因外不必填写原因说明'})

            # 到现场办事次数为0次事项并且不支持网办事项，办理形式需支持快递收件
            if row['到办事现场次数'] == '0次':
                if row['是否网办'] != '是' and '邮寄申请' not in row['办理形式']:
                    error += '{}. 到现场办事次数为0次事项并且不支持网办事项，办理形式需支持快递收件\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '跑零次不支持网办事项需支持快递收件', 'ERROR_DESCRIPTION': '到现场办事次数为0次事项并且不支持网办事项，办理形式需支持快递收件'})

            # 网办深度四级到现场办事次数应为0次
            if row['网上办理深度'] == '全程网办（Ⅳ级）':
                if row['到办事现场次数'] != '0次':
                    error += '{}. 网办深度四级到现场办事次数应为0次\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '网办深度四级不应跑现场', 'ERROR_DESCRIPTION': '网办深度四级到现场办事次数应为0次'})

            # 是否收费为是，则需填写收费依据、是否支持网上支付、收费项目名称、收费标准
            # if row['是否收费'] == '是':
            #     if not row['收费依据'] or not row['是否支持网上支付'] or not row['收费项目']:
            #         error += '{}. 是否收费为是，则需填写收费依据、是否支持网上支付、收费项目名称、收费标准\n'.format(idx)
            #         idx += 1

            # 咨询电话和投诉电话不同且必须有，带区号0577
            if not row['咨询电话'] or not row['投诉电话'] or row['咨询电话'] == row['投诉电话'] or ('0577' not in row['咨询电话'] or '0577' not in row['投诉电话']):
                error += '{}. 咨询电话和投诉电话不同且必须有，带区号0577\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '咨询电话和投诉电话相同或没有或没有带0577', 'ERROR_DESCRIPTION': '咨询电话和投诉电话不同且必须有，带区号0577'})

            # 工作时间要包括「夏、冬、工作日」
            # keyWords = ['夏', '冬', '工作日']
            keyWords = ['工作日']
            if not reduce(lambda a, b: a & b, map(lambda key: key in row['工作时间'], keyWords)) and '全天' not in row['工作时间'] and '24小时' not in row['工作时间'] and '节假日' not in row['工作时间']:
                error += '{}. 工作时间必须包含「工作日」关键字。参考模板：工作日，夏季：上午8：30-12:00，下午2:30-5:30；春、秋、冬季：上午8:30-12:00，下午2:00-5:00\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '工作时间不规范', 'ERROR_DESCRIPTION': '工作时间必须包含「工作日」关键字。参考模板：工作日，夏季：上午8：30-12:00，下午2:30-5:30；春、秋、冬季：上午8:30-12:00，下午2:00-5:00'})


            #办理地址要精确到窗口/门牌号
            #按括号为分界split，判断各个部分有没有
            if not self.__isAddressAccurate(row['具体地址']):
                error += '{}. 办理地址要精确到窗口/门牌号\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '办理地址不精确', 'ERROR_DESCRIPTION': '办理地址要精确到窗口/门牌号'})

            # 咨询和投诉地址不为空且不相等
            # if pd.isnull(row['咨询地址']) or pd.isnull(row['投诉地址']) or row['咨询地址'] == row['投诉地址']:
            #     error += '{}. 咨询和投诉地址不为空且不相等\n'.format(idx)
            #     idx += 1
            if not row['咨询地址'] or not row['投诉地址'] or row['咨询地址'] == '无' or row['投诉地址'] == '无':
                error += '{}. 咨询和投诉地址为空\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '咨询地址或投诉地址为空', 'ERROR_DESCRIPTION': '咨询和投诉地址为空'})

            # 服务对象需与主题分类一致。例如法人事项主题分类为法人，必须有法人主题，且自然人主题应为空。
            objects = row['服务对象'].split('/')
            res = 0
            if '法人' in row['服务对象'] or '其他组织' in row['服务对象']:
                res ^= 1
            if '个人' in objects:
                res ^= 1
            if row['自然人主题分类'] not in ['无', '不涉及']:
                res ^= 1
            if row['法人主题分类'] not in ['无', '不涉及']:
                res ^= 1
            if res != 0:
                error += '{}. 服务对象需与主题分类一致。例如法人事项主题分类为法人，必须有法人主题，且自然人主题应为空。\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '服务对象和主题分类不一致', 'ERROR_DESCRIPTION': '服务对象需与主题分类一致。例如法人事项主题分类为法人，必须有法人主题，且自然人主题应为空'})

            # 许可类事项一定要有一个常见问题
            if row['事项类型'] == '行政许可' and '暂无常见问题' in row['常见问题']:
                error += '{}. 许可类事项一定要有一个常见问题\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '许可类事项“常见问题”栏目无内容', 'ERROR_DESCRIPTION': '许可类事项一定要有一个常见问题'})

            # 联系方式不为空且和投诉电话不相等，并且带0577
            if not row['联系方式'] or row['联系方式'] == row['投诉电话'] or '0577' not in row['联系方式']:
                error += '{}. 联系方式不为空且和投诉电话不相等，并且带区号0577\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '联系电话为空或者和投诉电话相等', 'ERROR_DESCRIPTION': '联系方式不为空且和投诉电话不相等，并且带区号0577'})

            # 许可类事项环节可能存在异常（必须为表格且包含受理、审核、审批、办结、送达等环节）
            if row['办理环节数'] == -1 and row['事项类型'] == '行政许可':
                error += '{}. 许可类事项办理流程必须为表格\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '办理流程不是表格形式', 'ERROR_DESCRIPTION': '许可类事项办理流程必须为表格'})

            #表格流程时间之和等于承诺期限，许可专用
            if int(row['表格流程时间和']) > int(row['承诺期限数字']) and row['事项类型'] == '行政许可':
                error += '{}. 表格流程时间之和（受理、审核、审批、办结）不等于承诺期限\n'.format(idx)
                idx += 1
                errorList.append({'ERROR_CODE': '表格流程时间之和（受理、审核、审批、办结）不等于承诺期限', 'ERROR_DESCRIPTION': '表格流程时间之和（受理、审核、审批、办结）不等于承诺期限'})

            # 如果有审批结果就要有样本
            if row['审批结果名称'] != '无':
                if row['审批结果样本'] != '有样本':
                    error += '{}. 如果有审批结果就要有样本\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '无审批结果样本', 'ERROR_DESCRIPTION': '如果有审批结果就要有样本'})

            # 如果不收费，不能支持网上支付
            if row['是否收费'] == '不收费':
                if row['是否支持网上支付'] == '支持':
                    error += '{}. 如果不收费，不能支持网上支付\n'.format(idx)
                    idx += 1
                    errorList.append({'ERROR_CODE': '不收费但支持网上支付', 'ERROR_DESCRIPTION': '如果不收费，不能支持网上支付'})



            error = error.strip()  #去除最后的换行
            totRes.append(error)
            errorDf = errorDf.append(self.__splitErrors(errorList, row), ignore_index=True)



        df['错误情况'] = pd.Series(totRes)

        df['事项地址'] = 'http://www.zjzwfw.gov.cn/zjservice/item/detail/index.do?localInnerCode=' + df['内部编码']
        dep = df.pop('部门名称')
        df.insert(0, '部门名称', dep)
        # 得到一个总的表
        df.to_excel('total.xls', index=False)
        singleDf = df.groupby('区县')
        for name, d in singleDf:
            d.to_excel('{}{}错误情况.xls'.format(arrow.now().format('MMDD'), name), index=False)

        errorDf.to_excel('errordf.xls', index=False)

    def analyseStatics(self):
        df = pd.read_excel('total.xls')

    def __getTableSum(self, html):
        rows = len(html.xpath('//div[@class="bllc_con"]//tr'))

        keywords = ['受理', '审核', '审查', '核准', '决定', '办结', '审批', '制证', '签发']
        sum = 0
        for i in range(1, rows + 1):
            procedure = ''.join(html.xpath('//div[@class="bllc_con"]//tr[{}]/td[1]//text()'.format(i)))
            selected = reduce(lambda a, b: a | b, [k in procedure for k in keywords], False)
            if selected:
                curTime = ''.join(html.xpath('//div[@class="bllc_con"]//tr[{}]/td[2]//text()'.format(i)))
                workDays = re.findall(r'\d\.*\d*', curTime)
                if '不包含在承诺办结时限内' in curTime or '不属于市级承诺时间范围' in curTime:
                    continue
                if '即办' in curTime:
                    continue
                if '包含' in curTime:
                    continue
                elif workDays:
                    sum += float(workDays[0])

        return int(sum)

    def __isAddressAccurate(self, row):
        dealAddress: List[str] = row.replace('（', '#').replace('）', '#').split('#')
        for add in dealAddress:
            if not add:
                continue
            if '窗口' in add or '室' in add or '专区' in add or '中台' in add:
                return True
            if re.search(r'[A-Z].*区.*\d', add):
                return True
            if add[-1].isdigit():
                return True

        return False

    def __splitErrors(self, lis:list, row:pd.Series):
        appendLis = []
        for e in lis:
            appendLis.append({'AREA': row['区县'], 'DEPARTMENT': row['部门名称'], 'QL_BASIC_CODE': row['权力基本码'], 'MATTER_NAME': row['事项名称'], 'ERROR_CODE': e['ERROR_CODE'], 'ERROR_DESCRIPTION': e['ERROR_DESCRIPTION']})
        return pd.DataFrame(appendLis)

a = AnalyseData()
a.run()
a.analyse()

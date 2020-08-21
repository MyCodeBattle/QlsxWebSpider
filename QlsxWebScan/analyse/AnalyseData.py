from lxml import etree
import re
from typing import List
import os
from functools import reduce
import pandas as pd
import arrow
import traceback
from tqdm import tqdm


class AnalyseDaya:
    # 要分析的事项列表xls
    __analyseFilename = '../../事项表/0819全市许可.xls'
    with open('部门编码地区映射', 'r', encoding='utf-8') as fp:
        __areaList = fp.readlines()

    def regionMap(self, code: str):

        for c in self.__areaList:
            tmp = c.split()
            if code.startswith(tmp[1].strip()):
                return tmp[0]

    def run(self):
        df = pd.read_excel(self.__analyseFilename, sheet_name='Sheet1')['权力内部编码']
        res1 = []
        res2 = []
        for ic in tqdm(df, ncols=100):
            # if ic != '65e48c90-908f-4599-87a3-8af319da67f0':
            #     continue
            try:
                with open('{}/../../数据/{}'.format(os.getcwd(), ic), 'r', encoding='utf-8') as fp:
                    tmp = fp.read()
                    tmp = tmp.replace('<br>', '\n')
                    # print(tmp)
                    et = etree.HTML(tmp)
                    baseInfo, materialInfo = self.produce(et, ic)
                    # print(materialInfo)
                    res1.append(baseInfo)
                    res2 += materialInfo
            except Exception as e:

                traceback.print_exc()
                print('{}有问题'.format(ic))


        ddf = pd.DataFrame(res1)
        # ddf2 = pd.DataFrame(res2)
        w = pd.ExcelWriter('test.xls')
        ddf.to_excel(w, '事项信息', index=False)
        # ddf2.to_excel(w, '材料信息', index=False)
        w.save()
        w.close()

    def joinStrip(self, lis):
        return ''.join([x.strip() for x in lis])

    def materialProduce(self, et, ic, isMaterialSplit):
        template = {'材料名称': 'clmc_con', '来源渠道': 'lyqd_con', '材料形式': 'clxs_con', '纸质材料份数': 'zzclfs_con', '材料必要性': 'clbyx_con', '材料下载': 'zlxz_con', '备注': 'bz_con'}
        l = template
        if isMaterialSplit:
            l = dict(zip(template.keys(), map(lambda v: v + 's', template.values())))

        # 材料名称，list
        materialName = et.xpath('//*[@id="sbcl"]//*[@class="{}"]/p/text()'.format(l['材料名称']))

        # 来源渠道
        fromWhere = et.xpath('//span[@class="{}"]//p/text()'.format(l['来源渠道']))

        # 材料形式
        materialForm = et.xpath('//span[@class="{}"]/text()'.format(l['材料形式']))

        # 纸质材料份数
        paperNumber = et.xpath('//span[@class="{}"]//text()'.format(l['纸质材料份数']))

        # 材料必要性
        necessity = et.xpath('//span[@class="{}"]//text()'.format(l['材料必要性']))

        # 材料下载
        # downloads = et.xpath('//span[@class="{}"]//a/@href'.format(l['材料下载']))

        # 材料类型
        materialKind = et.xpath('//span[@class="cllx_con"]//text()')

        # 纸质材料规格
        materialScala = et.xpath('//span[@class="zzclgg_con"]//text()')

        # 备注
        notes = et.xpath('//span[@class="{}"]/p/text()'.format(l['备注']))

        res = []
        # print(materialForm)
        # print(paperNumber)
        # print(necessity)
        # print(materialKind)
        # print(materialScala)
        # print(notes)
        for i in range(len(materialName)):
            # 组装起来
            dic = {'材料名称': materialName[i], '来源渠道': fromWhere[i], '材料形式': materialForm[i], '纸质材料份数': paperNumber[i], '材料必要性': necessity[i], '材料类型': materialKind[i], '纸质材料规格': materialScala[i], '备注': notes[i], '权力内部编码': ic}
            res.append(dic)
        return res

    def produce(self, et, ic):

        jbxx = list(filter(lambda x: x not in ['事项信息', '办事信息', '结果信息'], map(lambda x: x.strip(), et.xpath('//div[@class="jbxx_tables"]//td/div/text()'))))
        newJbxx = []
        for i in range(0, len(jbxx) - 1):
            if jbxx[i] != jbxx[i + 1]:
                newJbxx.append(jbxx[i])
        jbxx = newJbxx
        # print(jbxx)

        jbxxDic = {'内部编码': ic}
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

        impleCode = et.xpath('//*[@id="impleCode"]/@value')[0]
        if impleCode == 'ff8080815e01f0b9015e0389183c0f904331400515002':
            print(ic)
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
        rowspan = et.xpath('//div[@class="bllc_con"]//td[1]/@rowspan')
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

        # 国家法律依据
        # countryLow = self.joinStrip(et.xpath('//*[contains(text(), "国家法律依据")]/following-sibling::*//text()'))
        # jbxxDic['国家法律依据'] = countryLow

        # 省级法律依据
        # provincelow = self.joinStrip(et.xpath('//*[contains(text(), "省级法律依据")]/following-sibling::*//text()'))
        # jbxxDic['省级法律依据'] = provincelow

        isMaterialSplit = len(et.xpath('//*[@id="sbcl"]//div[@class="apply_material"]')) != 0

        # 如果材料分情形，读取情形源码
        materialInfo = {}
        # if isMaterialSplit:
        #     try:
        #         with open('{}/../../数据/{}_material'.format(os.getcwd(), ic)) as fp:
        #             et = etree.HTML(fp.read())
        #
        #     except:
        #         print('{}没有材料\n', ic)
        #
        # materialInfo = self.materialProduce(et, ic, isMaterialSplit)
        return jbxxDic, materialInfo

    def analyse(self):
        df = pd.read_excel('test.xls', sheet_name='事项信息').fillna('')
        # df[['省级法律依据', '国家法律依据', '工作时间', '审批结果名称']] = df[['省级法律依据', '国家法律依据', '工作时间', '审批结果名称']].fillna('').astype(str)
        totRes = []
        for index, row in df.iterrows():
            if row['事项名称'] == '无':
                continue

            error = ''
            idx = 1
            # 承诺办结时间为即办，办件类型必须为即办件，事项审查类型为即审即办
            if row['承诺办结时限'] == '即办':
                if row['办件类型'] != '即办件' or row['事项审查类型'] != '即审即办':
                    error += '{}. 承诺办结时间为即办，办件类型必须为即办件，事项审查类型为即审即办\n'.format(idx)
                    idx += 1
            # 承诺办结时间非即办，办件类型必须为承诺件，事项审查类型为先审后批
            elif row['承诺办结时限'] != '即办':
                if row['办件类型'] != '承诺件' or row['事项审查类型'] != '前审后批':
                    error += '{}. 承诺办结时间非即办，办件类型必须为承诺件，事项审查类型为前审后批\n'.format(idx)
                    idx += 1
            # # 如审批结果名称为XX证则审批结果类型为出证办结
            # if row['审批结果名称'].endswith('证'):
            #     if row['审批结果类型'] != '出证办结':
            #         error += '{}. 如审批结果名称为XX证则审批结果类型为出证办结\n'.format(idx)
            #         idx += 1
            # # 4.如审批结果名称为XX文则审批结果类型为出文办结
            # if row['审批结果名称'].endswith('文'):
            #     if row['审批结果类型'] != '出文办结':
            #         error += '{}. 如审批结果名称为XX文则审批结果类型为出文办结\n'.format(idx)
            #         idx += 1
            # # 如审批结果名称为XX批复则审批结果类型为审批办结
            # if row['审批结果名称'].endswith('批复'):
            #     if row['审批结果类型'] != '审批办结':
            #         error += '{}. 如审批结果名称为XX批复则审批结果类型为审批办结\n'.format(idx)
            #         idx += 1
            # modify by wencj start
            # 2、如审批结果名称为XX批复则审批结果类型为审批办结
            if '批复' in row['审批结果名称'] or '批文' in row['审批结果名称']:
                if row['审批结果类型'] != '审批办结':
                    error += '{}. 如审批结果名称为XX批复则审批结果类型为审批办结\n'.format(idx)
                    idx += 1
            # 2、如审批结果名称为XX证则审批结果类型为出证办结
            elif (row['审批结果名称'].endswith('证') or '证书' in row['审批结果名称']) and \
                    '凭证' not in row['审批结果名称'] and '证明' not in row['审批结果名称']:
                if row['审批结果类型'] != '出证办结':
                    error += '{}. 如审批结果名称为XX证则审批结果类型为出证办结\n'.format(idx)
                    idx += 1

            # 2、如审批结果名称为XX文则审批结果类型为出文办结
            elif row['审批结果名称'].endswith('文') or ('文' in row['审批结果名称'] and '批文' not in row['审批结果名称']):
                if row['审批结果类型'] != '出文办结':
                    error += '{}. 如审批结果名称为XX文则审批结果类型为出文办结\n'.format(idx)
                    idx += 1
            elif row['审批结果名称'] and row['审批结果名称'] != '无' and (not row['审批结果类型'] or row['审批结果类型'] == '无'):
                error += '{}. 有审批结果一定要有审批结果类型\n'.format(idx)
                idx += 1
            # modify by wencj end
            # 到办事现场次数非0次事项需填写原因说明
            if row['到办事现场次数'] == '':
                error += '{}. 需填写到现场次数\n'.format(idx)
                idx += 1
            elif row['到办事现场次数'] != '0次':
                if not row['必须现场办理原因说明']:
                    error += '{}. 到办事现场次数非0次事项需填写原因说明\n'.format(idx)
                    idx += 1
            # 跑0次事项除非有特殊原因外不必填写原因说明
            elif row['到办事现场次数'] == '0次':
                if (row['必须现场办理原因说明'] != '无' or row['必须现场办理原因说明'] != '') \
                        and ('现场' in row['必须现场办理原因说明'] and '无需现场' not in row['必须现场办理原因说明'] and row['必须现场办理原因说明'] != '无需到现场办理'):
                    error += '{}. 跑0次事项除非有特殊原因外不必填写原因说明\n'.format(idx)
                    idx += 1

            # 到现场办事次数为0次事项并且不支持网办事项，办理形式需支持快递收件
            if row['到办事现场次数'] == '0次':
                if row['是否网办'] != '是' and '邮寄申请' not in row['办理形式']:
                    error += '{}. 到现场办事次数为0次事项并且不支持网办事项，办理形式需支持快递收件\n'.format(idx)
                    idx += 1

            # 网办深度四级到现场办事次数应为0次
            if row['网上办理深度'] == '全程网办（Ⅳ级）':
                if row['到办事现场次数'] != '0次':
                    error += '{}. 网办深度四级到现场办事次数应为0次\n'.format(idx)
                    idx += 1

            # 是否收费为是，则需填写收费依据、是否支持网上支付、收费项目名称、收费标准
            if row['是否收费'] == '是':
                if not row['收费依据'] or not row['是否支持网上支付'] or not row['收费项目']:
                    error += '{}. 是否收费为是，则需填写收费依据、是否支持网上支付、收费项目名称、收费标准\n'.format(idx)
                    idx += 1

            # 咨询电话和投诉电话不同且必须有，带区号0577
            if not row['咨询电话'] or not row['投诉电话'] or row['咨询电话'] == row['投诉电话'] or ('0577' not in row['咨询电话'] or '0577' not in row['投诉电话']):
                error += '{}. 咨询电话和投诉电话不同且必须有，带区号0577\n'.format(idx)
                idx += 1

            # 工作时间要包括「夏、冬、工作日」
            # keyWords = ['夏', '冬', '工作日']
            keyWords = ['工作日']
            if not reduce(lambda a, b: a & b, map(lambda key: key in row['工作时间'], keyWords)) and '全天' not in row['工作时间'] and '24小时' not in row['工作时间']:
                error += '{}. 工作时间必须包含「工作日」关键字。参考模板：工作日，夏季：上午8：30-12:00，下午2:30-5:30；春、秋、冬季：上午8:30-12:00，下午2:00-5:00\n'.format(idx)
                idx += 1


            #办理地址要精确到窗口/门牌号
            #按括号为分界split，判断各个部分有没有
            if not self.__isAddressAccurate(row['具体地址']):
                error += '{}. 办理地址要精确到窗口/门牌号\n'.format(idx)
                idx += 1

            # 咨询和投诉地址不为空且不相等
            # if pd.isnull(row['咨询地址']) or pd.isnull(row['投诉地址']) or row['咨询地址'] == row['投诉地址']:
            #     error += '{}. 咨询和投诉地址不为空且不相等\n'.format(idx)
            #     idx += 1
            if not row['咨询地址'] or not row['投诉地址'] or row['咨询地址'] == '无' or row['投诉地址'] == '无':
                error += '{}. 咨询和投诉地址不为空\n'.format(idx)
                idx += 1
            # else:
            #     if not ('窗口' in row['咨询地址'] or '号' in row['咨询地址'] or '室' in row['咨询地址'] or '幢' in row['咨询地址']
            #             or '楼' in row['咨询地址'] or '办公室' in row['咨询地址']):
            #         error += '{}. 咨询地址需精确到门牌号或窗口号\n'.format(idx)
            #         idx += 1
            #     if not ('窗口' in row['投诉地址'] or '号' in row['投诉地址'] or '室' in row['投诉地址'] or '幢' in row['投诉地址']
            #             or '楼' in row['投诉地址'] or '办公室' in row['投诉地址']):
            #         error += '{}. 投诉地址需精确到门牌号或窗口号\n'.format(idx)
            #         idx += 1

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

            # 许可类事项一定要有一个常见问题
            if row['事项类型'] == '行政许可' and '暂无常见问题' in row['常见问题']:
                error += '{}. 许可类事项一定要有一个常见问题\n'.format(idx)
                idx += 1

            # 联系方式不为空且和投诉电话不相等，并且带0577
            if not row['联系方式'] or row['联系方式'] == row['投诉电话'] or '0577' not in row['联系方式']:
                error += '{}. 联系方式不为空且和投诉电话不相等，并且带区号0577\n'.format(idx)
                idx += 1

            # 许可类事项环节可能存在异常（必须为表格且包含受理、审核、审批、办结、送达等环节）
            if row['办理环节数'] == -1 and row['事项类型'] == '行政许可':
                error += '{}. 许可类事项办理流程必须为表格\n'.format(idx)
                idx += 1

            #表格流程时间之和等于承诺期限，许可专用
            if row['表格流程时间和'] > row['承诺期限数字'] and row['事项类型'] == '行政许可':
                error += '{}. 表格流程时间之和（受理、审核、审批、办结）不等于承诺期限\n'.format(idx)
                idx += 1

            # 如果有审批结果就要有样本
            if row['审批结果名称'] != '无':
                if row['审批结果样本'] != '有样本':
                    error += '{}. 如果有审批结果就要有样本\n'.format(idx)
                    idx += 1
            # 如果不收费，不能支持网上支付
            if row['是否收费'] == '不收费':
                if row['是否支持网上支付'] == '支持':
                    error += '{}. 如果不收费，不能支持网上支付\n'.format(idx)
                    idx += 1

            # # 国家法律依据不能出现浙江省
            # countryLow = row['国家法律依据']
            # # lis = re.compile(r'《.*?》').findall(countryLow)
            # if countryLow and countryLow not in ['无', '无相关法律依据', '无特定法律依据']:
            #     # if reduce(lambda res1, res2: res1 | res2, map(lambda x: '浙江省' in x, lis), False):
            #     if '浙江省' in countryLow:
            #         error += '{}. 国家法律依据出现省级法律\n'.format(idx)
            #         idx += 1
            #
            # # 省级法律依据一定要出现浙江省
            # provinceLow = row['省级法律依据']
            # if provinceLow and provinceLow not in ['无', '无相关法律依据', '无特定法律依据']:
            #     lis = re.compile(r'《.*?》').findall(provinceLow)
            #     if reduce(lambda res1, res2: res1 & res2, map(lambda x: '浙江省' not in x, lis), True):
            #     # if '中华人民共和国' in provinceLow or '部令' in provinceLow or '总局令' in provinceLow \
            #     #         or '国务院' in provinceLow or '主席令' in provinceLow and '浙江省实施' not in provinceLow:
            #         error += '{}. 省级法律依据一定要有浙江省\n'.format(idx)
            #         idx += 1

            error = error.strip()  #去除最后的换行
            totRes.append(error)

        df['错误情况'] = pd.Series(totRes)
        oldDf = pd.read_excel(self.__analyseFilename, sheet_name='Sheet1', usecols=['权力内部编码', '部门名称', '权力基本码', '组织编码（即部门编码）'], dtype={'组织编码（即部门编码）': str})
        oldDf['地区'] = oldDf['组织编码（即部门编码）'].apply(lambda e: self.regionMap(e))
        df: pd.DataFrame = pd.merge(df, oldDf, left_on='内部编码', right_on='权力内部编码').drop(columns='内部编码')
        df['事项地址'] = 'http://www.zjzwfw.gov.cn/zjservice/item/detail/index.do?localInnerCode=' + df['权力内部编码']
        dep = df.pop('部门名称')
        df.insert(0, '部门名称', dep)
        # 得到一个总的表
        df.to_excel('total.xls', index=False)
        singleDf = df.groupby('地区')
        for name, d in singleDf:
            d.to_excel('{}{}错误情况.xls'.format(arrow.now().format('MMDD'), name), index=False)

        # self.__generateTotalExcel(df)

    def analyseStatics(self):
        df = pd.read_excel('total.xls')

    def __getTableSum(self, html):
        rows = len(html.xpath('//div[@class="bllc_con"]//tr'))

        firstInclude = False
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

a = AnalyseDaya()
a.run()
a.analyse()

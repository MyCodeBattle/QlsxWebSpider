import pandas as pd
import arrow
from IPython.display import display


class QlsxAnalyse:

    def __init__(self):
        with open('部门编码地区映射', 'r', encoding='utf-8') as fp:
            self.__areaList = fp.readlines()

        self.__targets = ['事项总数', '网上可办事项数', '网上可办率', '掌上可办事项数', '掌上可办率', '即办事项数', '即办率', '法定期限总和', '承诺期限总和' '承诺时限压缩比', '跑零次事项数', '跑零次率']

        self.__whiteDf = pd.read_excel('不宜跑零次.xlsx')
    def __regionMap(self, code: str):

        for c in self.__areaList:
            tmp = c.split()
            if code.startswith(tmp[1].strip()):
                return tmp[0]

    def __clean(self, df: pd.DataFrame):

        # 处理月、年
        tempDf = pd.DataFrame()
        for idx, row in df[['法定期限', '承诺期限']].iterrows():
            row = row.replace(r'工作日|天|即办', '', regex=True).replace('', '0').replace('无期限', '99999')

            if '月' in row['法定期限']:
                row = row.replace('月', '', regex=True)
                row['法定期限'] = float(row['法定期限'])*22

            elif '年' in row['法定期限']:
                row = row.replace('年', '', regex=True)
                row['法定期限'] = float(row['法定期限'])*250

            row = row.astype('float')
            tempDf = tempDf.append(row)
        tempDf = tempDf.rename(columns={'法定期限': '法定数字', '承诺期限': '承诺数字'})
        return pd.concat([df, tempDf], axis=1)

    def __calculate(self, df: pd.DataFrame):
        baseNum = df.shape[0]

        # 网办，总的比例减去不适宜网办的比例（办理地址为空且不是不适宜网办）
        wangbanNum = baseNum - df[(df['在线申报地址'] == '') & (df['不适宜开展网上办事'] != '是')].shape[0]
        wangban = wangbanNum / baseNum
        # 掌办同理
        zhangbanNum = baseNum - df[(df['移动端网上办理地址'] == '') & (df['不适宜开展网上办事'] != '是')].shape[0]
        zhangban = zhangbanNum / baseNum

        # 即办
        jibanNum = df[df['承诺数字'] == 0].shape[0]
        jiban = df[df['承诺数字'] == 0].shape[0] / baseNum

        # 压缩比
        yasuobiDf = df[(df['法定期限'] != '无期限') & (df['承诺期限'] != '无期限')]
        lawTotal = yasuobiDf['法定数字'].sum()
        actuallyTotal = yasuobiDf['承诺数字'].sum()
        yasuobi = (lawTotal - actuallyTotal) / lawTotal

        # 跑零次
        paolingciNum = df[df['办事者到办事地点最少次数'] == '0'].shape[0]
        allowLis = df[(df['办事者到办事地点最少次数'] != '0') & (df['权力基本码'].str.startsWith(self.__whiteDf['权力基本码']))]
        try:
            paolingci = df[df['办事者到办事地点最少次数'] == '0'].shape[0] / (baseNum - allowLis.shape[0])
        except Exception as e:
            paolingci = 1

        return {'网上可办率': wangban, '掌上可办率': zhangban, '即办率': jiban, '承诺时限压缩比': yasuobi, '跑零次率': paolingci, '网上可办事项数': wangbanNum, '掌上可办事项数': zhangbanNum, '即办事项数': jibanNum, '跑零次事项数': paolingciNum, '事项总数': baseNum, '法定期限总和': lawTotal, '承诺期限总和': actuallyTotal}

    def run(self):

        df = self.__clean(pd.read_excel('totalQlsx.xls', dtype=str).fillna(''))
        df = df[df['权力基本码'].str.contains('许可')] #依申请事项
        df.to_excel('检测数据.xls', index=False)
        df['区县'] = df['组织编码（即部门编码）'].apply(lambda e: self.__regionMap(e))

        totalDf = pd.DataFrame(columns=['区县'] + self.__targets) #全部汇总表

        #先把汇总的搞进去
        wenzhouTarget = self.__calculate(df)
        wenzhouTarget['区县'] = '汇总'
        totalDf = totalDf.append(wenzhouTarget, ignore_index=True)

        writer = pd.ExcelWriter('{}全市许可政务服务指标.xls'.format(arrow.now().strftime('%m%d')))

        quxianDfs = df.groupby('区县')

        for quxian, quxianDf in quxianDfs:
            quxianTotal = pd.DataFrame(columns=['部门名称'] + self.__targets)

            target = self.__calculate(quxianDf)
            target['区县'] = quxian
            totalDf = totalDf.append(target, ignore_index=True)

            for dept, deptDf in quxianDf.groupby('部门名称'):
                deptTarget = self.__calculate(deptDf)
                deptTarget['部门名称'] = dept
                quxianTotal = quxianTotal.append(deptTarget, ignore_index=True)

            quxianTotal.to_excel(writer, sheet_name=quxian, index=False)

        totalDf.to_excel(writer, sheet_name='汇总', index=False)
        writer.save()

    def dataHighlight(self, val:pd.Series):
        styleDict = 'font-family: 仿宋_GB2312; font-size: 14;'
        print(val.name)
        if val.name == '即办率': #76.1%
            colors = ['color: red' if v < 0.761 else 'color: black' for v in val]
            return colors
        if val.name == '承诺时限压缩比': #92.5%
            colors = ['color: red' if v < 0.925 else 'color: black' for v in val]


        return ['color: yellow' for v in val]

    def highlight(self):
        dfs = pd.read_excel('1015全市依申请政务服务指标.xls', sheet_name='汇总')
        dfs.style.format('{.2f%}')
        # s = dfs.style.format({'即办率': '{:.2%}', '承诺时限压缩比': '{:.2%}'})
        # s = s.apply(self.dataHighlight)
        display(dfs)
        dfs.to_excel('123.xlsx', index=False)



a = QlsxAnalyse()
# a.run()
a.highlight()

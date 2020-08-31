import pandas as pd
import arrow
import re

df:pd.DataFrame = pd.read_excel('total.xls', dtype=str).fillna('')

#统计汇总的看看
row = df.iloc[0, :]
print(row['错误情况'].split('\n'))

df['错误数量'] = df['错误情况'].apply(lambda s: 0 if not s else len(s.split('\n')))

g1 = df.groupby('区县')

newDf = pd.DataFrame()
for area, sht in g1:
    newDf = newDf.append({'区县': area, '事项总数': sht.shape[0], '正确事项数': sht[sht['错误数量'] == 0].shape[0]}, ignore_index=True)

writer = pd.ExcelWriter('{}部门排名.xls'.format(arrow.now().strftime('%m%d')))

for area, sht in g1:
    quxian = pd.DataFrame()
    for dept, depSht in sht.groupby('部门名称'):
        quxian = quxian.append({'部门': dept, '事项总数': depSht.shape[0], '正确事项数': depSht[depSht['错误数量'] == 0].shape[0]}, ignore_index=True)

    quxian['准确率'] = quxian['正确事项数'] / quxian['事项总数']
    quxian['排名'] = quxian['准确率'].rank(method='min', ascending=False)
    quxian = quxian.sort_values(by='排名')
    quxian.to_excel(writer, sheet_name=area, index=False)

newDf['准确率'] = newDf['正确事项数'] / newDf['事项总数']
newDf.to_excel(writer, '汇总', index=False)
writer.save()
import requests
import xlrd
import csv
dls = "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO-Historical-Data-Annual.xlsx"
resp = requests.get(dls)

output = open('/home/oracle/logs/dref/test.xlsx', 'wb')
output.write(resp.content)
output.close()
wb = xlrd.open_workbook('/home/oracle/logs/dref/test.xlsx')
sh = wb.sheet_by_index(0)
your_csv_file = open('/home/oracle/logs/dref/test.csv', 'w')
wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)
print(sh.nrows)
for rownum in range(sh.nrows):
    wr.writerow(sh.row_values(rownum))

your_csv_file.close()

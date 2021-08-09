import sys
from datetime import datetime
import time
import requests
import urllib.request as urllib2
import pandas as pd
import csv
from bs4 import BeautifulSoup
import lxml.html as lh
import cx_Oracle
import os
#Set Connection to Oracle
connection = cx_Oracle.connect('<User>/<Password>@<Host>/<SID>')
cur=connection.cursor()
now = datetime.now()

# Get/Set Input parameters
file_name = '/home/oracle/logs/dref/' + str(sys.argv[1]) + '_' + now.strftime("%Y%m%d%H%M%S") + '.dbg'
url = str(sys.argv[2])
try:
    tableIdx = int(sys.argv[3])
except:
    print('failed to set tableIdx with value:' )
    tableIdx = 0
stg_table = str(sys.argv[4])
final_table = str(sys.argv[5])
active_flag = int(sys.argv[6])
if len(sys.argv) > 7:
   param = str(sys.argv[7])
else:
   param = ''
   

# Identify the HTML table that need to extracted
print(url)
tables = pd.read_html(url)
print('number of table : ' + str(len(tables)))
#Set Data Frame
df = tables[tableIdx]
# Debug file
f = open(file_name, "a")
# Columns
f.write("COLUMNS:" + str(df.columns)+"\n")
# Number of columns
f.write("NUMBER OF COLUMNS:" + str(len(df.columns))+"\n")

#Prepare Create Table Statement
createTableStr = 'create table ' + stg_table + '(' 
for colIdx in range(1, len(df.columns)+1):
    if colIdx == len(df.columns):
       createTableStr += 'var'+str(colIdx)+' VARCHAR2(100))'
    else:
       createTableStr += 'var'+str(colIdx)+' VARCHAR2(100), '
f.write(createTableStr+"\n")
f.flush()


if active_flag != 99: 
   cur.execute('drop table ' + stg_table)
   cur.execute(createTableStr)


#Prepare and Execute Insert statements
idx=len(df.columns)
for i in range(len(df)):
    col = []
    for j in range(len(df.columns)):
        col.append(str(df.iloc[i][j]))
    f.write(str(col)+"\n")
    if idx == len(df.columns) and len(col) > 0:
        insertStmt = str(col).replace("[","insert into "+stg_table+" values (").replace("]",")")
        f.write(insertStmt+"\n")
        f.flush()
        if active_flag != 99: 
           cur.execute(insertStmt)
           cur.execute('commit')
    else:
        break



# Execute Procedure to update final table
procName = 'load_'+final_table
f.write('load_'+final_table+': '+param+"\n")
if active_flag != 99: 
   f.write('now calling load_'+final_table+"\n")
   if len(param) == 0:
      cur.callproc(procName)
   else:
      cur.callproc(procName, [param])

while 1:
    time.sleep(2)
    state = cur.execute('select state from dref_status').fetchone()
    print ('State='+str(state[0]))
    if state[0] == 0:
        break

f.close()

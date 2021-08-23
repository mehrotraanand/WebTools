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
import html5lib
#Set Connection to Oracle
#dsn_tns = cx_Oracle.makedsn(os.environ.get('HOST') , os.environ.get('PORT'), service_name = os.environ.get('SID'))
#connection = cx_Oracle.Connection(os.environ.get('USER'),os.environ.get('PSWD'),dsn_tns)
#connection = cx_Oracle.connect(os.environ.get("USER")+'/'+os.environ.get("PSWD")+'@'+os.environ.get("HOST")+'/'+os.environ.get("SID"))
connection = cx_Oracle.connect(os.environ.get("USER")+'/'+os.environ.get("PSWD"))
cur=connection.cursor()
now = datetime.now()

# Get/Set Input parameters
file_name = os.environ.get('LOG_DIR') + '/' + str(sys.argv[1]) + '_' + now.strftime("%Y%m%d%H%M%S") + '.dbg'
html_file = os.environ.get('LOG_DIR') + '/' + str(sys.argv[1]) + '_' + now.strftime("%Y%m%d%H%M%S") + '.html'
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
try:
   tables = pd.read_html(url)
except:
    print('failed to read url')
    print('curl "'+url+'" --output '+ html_file)
    os.system('curl "'+url+'" --output '+ html_file)
    #dataFrame = BeautifulSoup(open(html_file,'r').read()).find('table')
    tables = pd.read_html(html_file)

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
       createTableStr += 'var'+str(colIdx)+' VARCHAR2(200))'
    else:
       createTableStr += 'var'+str(colIdx)+' VARCHAR2(200), '
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

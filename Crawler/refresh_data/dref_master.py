import cx_Oracle
import os
import sys
connection = cx_Oracle.connect('<user>/<password>@<host>/<sid>')
ver=connection.version
#print('DB version : '+ ver)
# Master Cursor
# Active_flag = 0 ==> Inactive
# Active_flag = 1 ==> No recursion Active
# Active_flag = 2 ==> Recursion Active -- Does not have any test mode.
# Active_flag = 99 ==> Test Web Page No data load
cur=connection.cursor()
print(str(len(sys.argv)))
if len(sys.argv) > 1:
   testmode = str(sys.argv[1])
else:
   testmode = ''
if testmode == 'test':
   cur.execute('Select url, output_file, table_index, stage_table, destination_table, active_flag, param from dref_url_extract_master where active_flag = 99')
else:
   cur.execute('Select url, output_file, table_index, stage_table, destination_table, active_flag, param from dref_url_extract_master where active_flag > 0 and active_flag < 99')
for result in cur:
    print('url : ' + result[0])
    print('output_file : ' + result[1])
    print('table_index : ' + str(result[2]))
    print('stage_table : ' + str(result[3]))
    print('final_table : ' + str(result[4]))
    print('active_flag : ' + str(result[5]))
    print('param : ' + str(result[6]))
    # 
    if result[5] == 1:
       if str(result[6]) == 'None':
          os.system('python /home/oracle/refresh_data/dref_from_web.py ' + result[1] + ' "' + result[0] + '" ' + str(result[2]) + ' ' + result[3] + ' ' + result[4] + ' ' + str(result[5]))
       else:
          os.system('python /home/oracle/refresh_data/dref_from_web.py ' + result[1] + ' "' + result[0] + '" ' + str(result[2]) + ' ' + result[3] + ' ' + result[4] + ' ' + str(result[5]) + ' ' + result[6])
    if result[5] == 2:
       stockcur=connection.cursor()
       stockcur.execute("select replace(account_name,'Merril-') from fin_account where account_name like 'Merril-%' union select stock_type from fin_stock_history")
       for stock in stockcur:
           print(stock[0])
           if str(result[6]) == 'None':
              os.system('python /home/oracle/refresh_data/dref_from_web.py ' + result[1] + ' "' + result[0]+stock[0]+'/download-data" ' + str(result[2]) + ' ' + result[3] + ' ' + result[4] + ' ' + str(result[5]))
           else:
              os.system('python /home/oracle/refresh_data/dref_from_web.py ' + result[1] + ' "' + result[0]+stock[0]+'/download-data" ' + str(result[2]) + ' ' + result[3] + ' ' + result[4] + ' ' + str(result[5]) + ' ' + str(stock[0]))
       stockcur.close()
    if result[5] == 99:
       if str(result[6]) == 'None':
          os.system('python /home/oracle/refresh_data/dref_from_web.py ' + result[1] + ' "' + result[0] + '" ' + str(result[2]) + ' ' + result[3] + ' ' + result[4] + ' ' + str(result[5]))
       else:
          os.system('python /home/oracle/refresh_data/dref_from_web.py ' + result[1] + ' "' + result[0] + '" ' + str(result[2]) + ' ' + result[3] + ' ' + result[4] + ' ' + str(result[5]) + ' ' + result[6])
cur.close()
connection.close

#!/bin/ksh
. /home/oracle/.profile
LOG_DIR="/home/oracle/logs/dref"
echo "MAIN: LOG_DIR is $LOG_DIR"
procName=`basename ${0}`

mkdir -p ${LOG_DIR}
now=`date +%Y%m%d%H%M`

LOG_FILE=${procName}.${now}.$$.log

echo "Find the detail log for the process in ${LOG_DIR}/${LOG_FILE} at $(uname -n)"
exec 3>&1 4>&2 >${LOG_DIR}/${LOG_FILE} 2>&1

echo "MAIN: STARTED AT = $(date) "
/usr/bin/python /home/oracle/refresh_data/dref_master.py
echo "MAIN: FINISHED AT = $(date) "
find /home/oracle/logs/dref/ -name \*.log -mtime +7 -exec rm -f {} \;
find /home/oracle/logs/dref/ -name \*.dbg -mtime +7 -exec rm -f {} \;
exec 1>&3 2>&4

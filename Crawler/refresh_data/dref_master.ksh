#!/bin/ksh
. /home/oracle/.profile
export LOG_DIR="$HOME/logs/dref"
export BASE_DIR="$HOME/refresh_data"
export HOST=<HOSTNAME>
export PORT=<PORT>
export SID=<SID>
export USER=<USER>
export PSWD=<PASSWORD>
echo "MAIN: LOG_DIR is $LOG_DIR"
procName=`basename ${0}`

mkdir -p ${LOG_DIR}
now=`date +%Y%m%d%H%M`

LOG_FILE=${procName}.${now}.$$.log

echo "Find the detail log for the process in ${LOG_DIR}/${LOG_FILE} at $(uname -n)"
exec 3>&1 4>&2 >${LOG_DIR}/${LOG_FILE} 2>&1

echo "MAIN: STARTED AT = $(date) "
/usr/bin/python $BASE_DIR/dref_master.py $1
echo "MAIN: FINISHED AT = $(date) "
find $LOG_DIR -name \*.log -mtime +7 -exec rm -f {} \;
find $LOG_DIR -name \*.dbg -mtime +7 -exec rm -f {} \;
exec 1>&3 2>&4

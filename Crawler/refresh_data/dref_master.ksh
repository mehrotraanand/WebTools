#!/bin/ksh
. ~/.profile
export LOG_DIR="$HOME/logs/WebTools"
export BASE_DIR="$HOME/WebTools"
export HOST=<HOST>
export PORT=<PORT>
export SID=<SID>
export UID=<USERNAME>
export PSWD=<PASSWORD>
echo "MAIN: LOG_DIR is $LOG_DIR"
procName=`basename ${0}`

mkdir -p ${LOG_DIR}
now=`date +%Y%m%d%H%M`

LOG_FILE=${procName}.${now}.$$.log

echo "Find the detail log for the process in ${LOG_DIR}/${LOG_FILE} at $(uname -n)"
exec 3>&1 4>&2 >${LOG_DIR}/${LOG_FILE} 2>&1

echo "MAIN: STARTED AT = $(date) "
/usr/bin/python $BASE_DIR/dref_master.py
echo "MAIN: FINISHED AT = $(date) "
find $LOG_DIR -name \*.log -mtime +7 -exec rm -f {} \;
find $LOG_DIR -name \*.dbg -mtime +7 -exec rm -f {} \;
exec 1>&3 2>&4

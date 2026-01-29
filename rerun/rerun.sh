#!/bin/sh
#详细使用说明,见$BIPROG_ROOT/bin/common_init.sh

################################################################################
##  shell
################################################################################
##  Purpose : ***
##  Auther  : ***
##  Date    : YYYY-MM-DD
##  Modified History:
################################################################################
##  DATE        AUTHOR   VERSION         DESCRIPTION
##  YYYY-MM-DD  ***      V01.00.000        ***
################################################################################
##  Current Version : V01.00.000
##  输入参数
##  vJobSeq         :作业步骤，：1
##  vBgnStatDay     :开始日期，格式：YYYYMMDD。
##  vEndStatDay     :结束日期，格式：YYYYMMDD。
################################################################################

#临时需求日志设置为当前目录
#logfile=./`basename $0`.log

#调度作业日志设置为$BIPROG_ROOT/log
logfile=$BIPROG_ROOT/log/`basename $0`.log

ProcNam=`basename $0`
ProcPath=`pwd`

# 检查命令行参数个数，需根据实际情况修改
if [ $# -ne 3 ];then
    echo "$0: wrong number of arguments"
    echo "Usage: `basename $0` <vJobSeq> <vBgnStatDay> <vEndStatDay>"
    echo "Example: rerun.sh 1 20220501 20220505"
    exit 1
fi

echo "Init program...."
##############加载公共模块###########################
. $BIPROG_ROOT/bin/common_init.sh


##  用户代码从这里开始
writelog "Program start..."

##  变量初始化
vBgnStatDay=$2
vEndStatDay=$3
vStatDay=${vBgnStatDay}
while [ ${vStatDay} -le ${vEndStatDay} ]
do

    
    CMD="echo '在这里输入代码'"
    EXESH_CMD
    
    vStatDay=`GET_PREDATE ${vStatDay} -1`
done

################################################################################
### 完成
writelog "Program finish successfully."
exit 0

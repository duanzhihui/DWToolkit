#!/bin/sh
#BI所有SHELL的共用模板,需要放在$BIPROG_ROOT/bin里.
#  以下代码为模板代码，不要修改
startnum=$1     # 起跑作业序号
currentnum=0    # 当前作业

# 写日志的shell函数
sUserID=`whoami`
stmplogfile=result.log.$$.tmp

writelog()
{
  echo `date "+%Y-%m-%d %H:%M:%S"`" $1" | tee -a $logfile
}

# 执行SHELL命令的函数
EXESH_CMD()
{
    # 检查作业可否跳过
    currentnum=`expr $currentnum + 1`
    writelog "Running job #$currentnum ..."
    if [ "$currentnum" -lt "$startnum" ];then
        writelog "job ($currentnum < $startnum) skipped .."
        return 0  # 跳过
    fi

    # 执行之
    writelog "$CMD"
    eval "$CMD" > ${stmplogfile} 2>&1
    retcode=$?
    cat ${stmplogfile} >> $logfile
    if [ $retcode -ne 0 ]
    then
        cat ${stmplogfile}
        writelog "SH Command failed! exit..."
        rm ${stmplogfile}
        exit 1
    fi
    rm ${stmplogfile}
}

#功能 返回N天前的日期 GET_PREDATE YYYYMMDD N
#用法 GET_PREDATE 20060901  3
#返回 RETURN_DATE=20060828
GET_PREDATE()
{
    year=`echo $1 | awk '{print substr($1,1,4)}' `
    month=`echo $1 | awk '{print substr($1,5,2)}'`
    day=`echo $1 | awk '{print substr($1,7,2)}'  `
    predays=$2

    #判断年月日是否正确
    if [ $year -lt 1900 ]
    then
        writelog "Year is not right year=${year}"
        exit 1
    fi

    if [ $month -lt 0 -o $month -gt 12 ]
    then
        writelog "Month is not right month=${month}"
        exit 1
    fi

    #取一个月的最大天数
    if [ $month -eq 1 -o $month -eq 3 -o $month -eq 5 -o $month -eq 7 -o $month -eq 8 -o $month -eq 10 -o $month -eq 12 ]
    then
       maxday=31
    elif [ $month -eq 2 ]
    then
        if [ `expr $year % 4` -eq 0 ]
        then
           maxday=29
        else
           maxday=28
        fi
    else
       maxday=30
    fi

    if [ $day -gt maxday -o $day -lt 0 ]
    then
        writelog "Day is not right day=$day"
        exit 1
    fi

###开始对日期进行处理#################
    while [ ${predays} -gt 0 ]
    do
        day=`expr ${day} - 1`

        if [ $day -lt 1 ]
        then
            month=`expr $month - 1`
            if [ month -lt 1 ]
            then
                year=`expr $year - 1`
                month=12
            fi
            #计数该月的最大天数.
            if [ $month -eq 1 -o $month -eq 3 -o $month -eq 5 -o $month -eq 7 -o $month -eq 8 -o $month -eq 10 -o $month -eq 12 ]
            then
               maxday=31
            elif [ $month -eq 2 ]
            then
                if [ `expr $year % 4` -eq 0 ]
                then
                   maxday=29
                else
                   maxday=28
                fi
            else
               maxday=30
            fi
            day=$maxday
        fi
        predays=`expr $predays - 1`
    done
    RETURN_DATE=`expr $year \* 10000 + $month \* 100 + $day `
}


##返回上月月末的日期
##GET_PREMONTHEND  20070306
##返回  PRE_MONTHEND = 20070228
GET_PREMONTHEND()
{
    year=`echo $1 | awk '{print substr($1,1,4)}' `
    month=`echo $1 | awk '{print substr($1,5,2)}'`
    day=`echo $1 | awk '{print substr($1,7,2)}'  `

    ##计算上个月份和上一年的值
    pre_year=`expr ${year} - 1`
    pre_month=`expr ${month} - 1`

    ##根据${pre_month}值进行判断
    if [ ${pre_month} -eq 0 ]
    then
        year=${pre_year}
        pre_month=12
        month_end=31
    elif [ ${pre_month} -eq 1 -o ${pre_month} -eq 3 -o ${pre_month} -eq 5 -o ${pre_month} -eq 7 -o ${pre_month} -eq 8 -o ${pre_month} -eq 10 ]
        then
            month_end=31
    elif [ ${pre_month} -eq 4 -o ${pre_month} -eq 6 -o ${pre_month} -eq 9 -o ${pre_month} -eq 11 ]
        then
            month_end=30
    elif [ ${pre_month} -eq 2 ]
        then
            diff_val=`expr ${year} % 4`
            if [ ${diff_val} -eq 0 ]
            then
                month_end=29
            else
                month_end=28
            fi
    else
        exit 1
    fi

    pre_monthend=`expr ${year} \* 10000 + ${pre_month} \* 100 + ${month_end} `

}

#iniFile=$BIPROG_ROOT/config/prog.ini
#url=`$BIPROG_ROOT/bin/iniGetValue.sh         $iniFile hive   url`
#username=`$BIPROG_ROOT/bin/iniGetValue.sh    $iniFile hive   username`
#password=`$BIPROG_ROOT/bin/iniGetValue.sh    $iniFile hive   password`



#!/bin/sh
if [ $# -ne 3 ];then
    echo "usage: iniGetValue.sh <inifile> <section> <valueid>" 1>&2
    exit 1
fi
getValue()
{
    if [ $# -ne 3 ];then
        exit 1
    fi
    inifile=$1
    section=$2
    valueid=$3
    awk -F= 'BEGIN { retval = 1 }
    {
        if (substr($0, 1, 1) == "[")
            valid = 0;
        if ($1 == "['$section']")
        {
            valid = 1;
        }
        else if (valid == 1)
        {
            if ($1 == "'$valueid'")
            {
                print $2;
                retval = 0;
                exit;
            }
        }
    } END {exit retval}' "$inifile"
}

value=`getValue $1 $2 $3`
if [ -z "$value" ]
then
    value=`inigetvalue -d $2 $3`
fi
echo $value
exit 0


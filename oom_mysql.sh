#!/bin/bash

# 配合修改 /lib/systemd/system/mariadb.service
# ExecStartPost=/home/uta_iot/github_repo/mitsubishi-fx5/oom_mysql.sh
MYSQL_PIDFILE="/var/run/mysqld/mysqld.pid"

 

pgrep mysql > /dev/null

if [ ! $? ]; then

        exit 1

fi

 

if [ -f "$MYSQL_PIDFILE" ]; then

        MYSQL_PID=`cat $MYSQL_PIDFILE`

        echo "-17" > /proc/$MYSQL_PID/oom_score_adj

else

        exit 1

fi

 

exit 0
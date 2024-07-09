#!/bin/bash

MYSQL_PIDFILE="/var/run/mysqld/mysqld.pid"

 

pgrep mysql > /dev/null

if [ ! $? ]; then

        exit 1

fi

 

if [ -f "$MYSQL_PIDFILE" ]; then

        MYSQL_PID=`cat $MYSQL_PIDFILE`

        echo "-100" > /proc/$MYSQL_PID/oom_score_adj

else

        exit 1

fi

 

exit 0
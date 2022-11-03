#!/bin/sh
a=$(pgrep -f "test_6.py")
b=$(pgrep -f "oee_today3.py")

if [ -n "$a" ] && [ -n "$b" ]
then
    echo "All processes of rc-local.service are Running"
    logger "All processes of rc-local.service are Running"
else
    if ![ -n "$a" ]
    then
        echo "test_6.py not active"
        logger "test_6.py not active"
    fi
    if ![ -n "$b" ]
    then
        echo "oee_today3.py not active"
        logger "oee_today3.py not active"
    fi
    systemctl stop rc-local.service
    systemctl start rc-local.service
fi
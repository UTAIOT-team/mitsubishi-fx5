#!/bin/sh
a=$(pgrep -f "test_6.py")
b=$(pgrep -f "oee_today3.py")

if [ -n "$a" ] && [ -n "$b" ]
then
    echo "All processes of rc-local.service are Running."
    logger "All processes of rc-local.service are Running."
else
    if !([ -n "$a" ])
    then
        echo "test_6.py isn't active"
        logger "test_6.py isn't active, restart rc-local.service."
    fi
    if !([ -n "$b" ])
    then
        echo "oee_today3.py isn't active"
        logger "oee_today3.py isn't active, restart rc-local.service."
    fi
    systemctl stop rc-local.service
    systemctl start rc-local.service
fi
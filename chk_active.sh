#!/bin/sh
a=$(pgrep -f "test_6.py")
b=$(pgrep -f "oee_today3.py")

if [ -n "$a" ] && [ -n "$b" ]
then
    echo "All processes of rc-local.service are Running"
else
    echo "not active"
    systemctl stop rc-local.service
    systemctl start rc-local.service
fi
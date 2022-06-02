#!/bin/sh
if [pgrep -f "test6.py"] && [pgrep -f "oee_today3.py"] > /dev/null
then
    echo "All processes of rc-local.service are Running"
else
    systemctl stop rc-local.service
    systemctl start rc-local.service
fi
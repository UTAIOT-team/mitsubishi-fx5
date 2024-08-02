#!/bin/sh
a=$(pgrep -f "test_6_w.py")

if !([ -n "$a" ])
then
    echo "test_6_w.py isn't active, restart rc-local.service."
    logger "test_6_w.py isn't active, restart rc-local.service."
    systemctl stop rc-local.service
    systemctl start rc-local.service
else
    echo "test_6_w.py is actived."
    logger "test_6_w.py is  actived."
fi

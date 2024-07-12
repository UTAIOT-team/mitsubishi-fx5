#!/bin/sh
a=$(pgrep -f "test_6.py")

if !([ -n "$a" ])
then
    echo "test_6.py isn't active, restart rc-local.service."
    logger "test_6.py isn't active, restart rc-local.service."
    systemctl stop rc-local.service
    systemctl start rc-local.service
fi

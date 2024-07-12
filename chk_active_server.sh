#!/bin/sh

# 检查 MariaDB 服务状态
service_status=$(systemctl is-active mariadb.service)

# 如果服务未启动，则启动服务
if [ "$service_status" != "active" ]; then
    echo "MariaDB 服务未启动，正在启动..."
    logger "MariaDB 服务未启动，正在启动..."
    systemctl start mariadb.service

    # 检查服务是否成功启动
    service_status=$(systemctl is-active mariadb.service)
    if [ "$service_status" == "active" ]; then
        echo "MariaDB 服务已成功启动。"
        logger "MariaDB 服务已成功启动。"
    else
        echo "无法启动 MariaDB 服务，请检查日志。"
        logger "无法启动 MariaDB 服务，请检查日志。"
    fi
else
    echo "MariaDB 服务正在运行。"
    logger "MariaDB 服务正在运行。"
fi

b=$(pgrep -f "oee_today3.py")

if !([ -n "$b" ])
then
    echo "oee_today3.py isn't active, restart rc-local.service."
    logger "oee_today3.py isn't active, restart rc-local.service."
    systemctl stop rc-local.service
    systemctl start rc-local.service
else
    echo "oee_today3.py is actived."
    logger "oee_today3.py is  actived."
fi

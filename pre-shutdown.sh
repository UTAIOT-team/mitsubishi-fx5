#!/bin/bash
### BEGIN INIT INFO
# Provides: shutdownbefore
# Required-Start:
# Required-Stop:
# Default-Start:    2 3 4 5
# Default-Stop:     0 1 6
# Short-Description:
# Description:
### END INIT INFO
case "${1:-''}" in
	'start')
		mount -a
		#开机需要执行的逻辑
		;;
	'stop')
		#关机需要执行的逻辑
		#登出iscsi命令写在这里，本文不展示
		#mysqldump -uroot -puta1234 --databases test watch_files mqtt cms mes >> /home/uta_iot/utashare_iotdb/$(date +%Y_%m_%d_%H_%M_%S).sql;
		umount /home/uta_iot/excel_output /home/uta_iot/utashare_iotdb/
		;;
	*)
		;;
esac

exit 0
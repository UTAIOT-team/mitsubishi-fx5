#!/bin/bash
START=${SECONDS}
BACKUP=/home/uta_iot/dumpdb/$(date +%Y_%m_%d_%H_%M_%S)_fullbackup
#mysqldump -uroot -puta1234 -q --single-transaction --databases test watch_files mqtt cms mes >> /home/uta_iot/dumpdb/$(date +%Y_%m_%d_%H_%M_%S).sql;
mariabackup --backup --target-dir=$BACKUP  --user=root --password=uta1234
END=$(($SECONDS - $START))
logger "fullbackup finished in" $END "second(s)"
START=${SECONDS}
tar -cvf $BACKUP.tar $BACKUP  --remove-files
mv $BACKUP.tar /home/uta_iot/utashare_iotdb/
END=$(($SECONDS - $START))
logger "move files finished in" $END "second(s)"
#/usr/bin/find /home/uta_iot/utashare_iotdb/ -mtime +10  -exec rm -rf {} \;

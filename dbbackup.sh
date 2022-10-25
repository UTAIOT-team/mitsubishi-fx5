#!/bin/bash
START=${SECONDS}
DIR=/home/uta_iot/dumpdb/
BACKUP=$(date +%Y_%m_%d_%H_%M_%S)_fullbackup
#mysqldump -uroot -puta1234 -q --single-transaction --databases test watch_files mqtt cms mes >> /home/uta_iot/dumpdb/$(date +%Y_%m_%d_%H_%M_%S).sql;
mariabackup --backup --target-dir=$DIR$BACKUP  --user=root --password=uta1234
END=$(($SECONDS - $START))
logger "fullbackup finished in" $END "second(s)"
START=${SECONDS}
#tar -cvf /home/uta_iot/dumpdb/2022_10_25_16_48_01_fullbackup.tar -C /home/uta_iot/dumpdb ./2022_10_25_16_48_01_fullbackup
tar -cvf $DIR$BACKUP.tar -C $DIR ./$BACKUP --remove-files
mv $DIR$BACKUP.tar /home/uta_iot/utashare_iotdb/
END=$(($SECONDS - $START))
logger "move files finished in" $END "second(s)"
#/usr/bin/find /home/uta_iot/utashare_iotdb/ -mtime +10  -exec rm -rf {} \;

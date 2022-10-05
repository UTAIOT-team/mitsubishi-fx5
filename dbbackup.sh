#!/bin/sh
START=${SECONDS}
mysqldump -uroot -puta1234 --databases test watch_files mqtt cms mes >> /home/uta_iot/dumpdb/$(date +%Y_%m_%d_%H_%M_%S).sql;
END=${SECONDS}
echo "dumpdb finished in " $END - $START " second(s)"
START=${SECONDS}
mv /home/uta_iot/dumpdb/* /home/uta_iot/utashare_iotdb/ &
END=${SECONDS}
echo "dumpdb finished in " $END - $START " second(s)"
##/usr/bin/find /home/uta_iot/utashare_iotdb/ -mtime +10  -exec rm -rf {} \;

exit 0
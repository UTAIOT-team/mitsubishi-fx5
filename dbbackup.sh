#!/bin/sh
mysqldump -uroot -p --databases test watch_files mqtt cms mes >> /home/uta_iot/utashare_iotdb/$(date +%Y_%m_%d_%H:%M:%S).sql;

# /usr/bin/find /home/uta_iot/utashare_iotdb/ -mtime +10  -exec rm -rf {} \;
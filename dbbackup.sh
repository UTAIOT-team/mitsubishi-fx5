#!/bin/sh
mysqldump -uroot -puta1234 --databases test watch_files mqtt cms mes >> /home/uta_iot/utashare_iotdb/$(date +%Y_%m_%d_%H_%M_%S).sql;

# /usr/bin/find /home/uta_iot/utashare_iotdb/ -mtime +10  -exec rm -rf {} \;
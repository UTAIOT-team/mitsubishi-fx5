#!/bin/sh
mysqldump -uroot -puta1234 --databases test watch_files mqtt cms mes >> /home/uta_iot/utashare_iotdb/$(date +%Y_%m_%d_%H_%M_%S).sql;

umount /home/uta_iot/excel_output /home/uta_iot/utashare_iotdb/

exit 0
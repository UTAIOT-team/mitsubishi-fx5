# setting
$sudo nano /etc/apt/sources.list
    deb http://ftp.de.debian.org/debian sid main 

# setup
$sudo apt update
$sudo apt install python3-pandas python3-openpyxl python3-sqlalchemy python3-mysql.connector python3-ping3

# rc-local setting
$sudo nano /lib/systemd/system/rc-local.service
[Unit]
Description=/etc/rc.local Compatibility
Documentation=man:systemd-rc-local-generator(8)
ConditionFileIsExecutable=/etc/rc.local
After=network.target

[Service]
Type=forking
ExecStart=/etc/rc.local start
TimeoutSec=0
RemainAfterExit=yes
GuessMainPID=no

[Install]
WantedBy=multi-user.target
Alias=rc-local.service

$sudo nano /etc/rc.local
#!/bin/sh -e

cd /home/pi/github_repo/mitsubishi-fx5/
python3 test_6.py &
#python3 test_6_w.py &

exit 0

$sudo systemctl enable rc-local.service
$sudo systemctl start rc-local.service
$sudo systemctl status rc-local.service

# crontab setting
$sudo crontab -e
* * * * *  /home/pi/github_repo/mitsubishi-fx5/chk_active_edge.sh
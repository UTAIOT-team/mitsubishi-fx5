# ubuntu Autostarting a python3 script
    $sudo pip install -r requirement.txt
    $sudo vim /lib/systemd/system/rc-local.service

    # add these in the end
    [Install]
    WantedBy=multi-user.target
    Alias=rc-local.service

    $sudo vim /etc/rc.local
    
    #!/bin/sh -e

    cd /path.../mitsubishi-fx5/
    python3 test_6.py &
    python3 oee_today3.py &

    exit 0

    $sudo systemctl enable rc-local.service
    $sudo systemctl start rc-local.service
    $sudo systemctl status rc-local.service

# mitsubishi-fx5
三菱FX5シーケンサを操作するPythonのサンプルです。

主にDデバイスとMデバイスへの値の読み込みと書き込みが行えます。

自身の環境で実装する際のヒントとしてお使い下さい。

# 使い方

```
# Open connection
fx5 = FX5.get_connection('192.168.1.10:2555')

# Dデバイスへの操作
fx5.write('D500', 30)
print(fx5.read('D500')) # -> 30

# Mデバイスへの操作
fx5.write('M1600', 1)
print(fx5.read('M1600')) # -> 1

# 複数デバイスへの値の書き込み
fx5.exec_cmd('D150=31,D200=5,D300=2')

# Close connection
fx5.close()
```

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------
# Date: 2022/10/25
# Author:   Shaun
# 檔案功能描述: 跳板至遠端後下shell ping指令回傳

import subprocess,shlex
from subprocess import PIPE

#args = ['sshpass', '-p', '', 'ssh', '-oHostKeyAlgorithms=+ssh-rsa', 'root@10.10.0.181', 'ping 10.10.3.49 -w 1 -c 1']
args = shlex.split('sshpass -p "" ssh -oHostKeyAlgorithms=+ssh-rsa root@10.10.0.181 "ping 10.10.3.49 -w 1 -c 1"')
print(args)
comp_process = subprocess.run(args,stdout=PIPE, stderr=PIPE)
print(comp_process)
if comp_process.returncode==0:
	chk2_ping=float(str(comp_process.stdout).split("/")[-1].replace(" ms\\n'",""))
else:
	chk2_ping=None
print(chk2_ping,comp_process.returncode,type(comp_process.returncode))

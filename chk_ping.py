#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------
# Date: 2022/10/03
# Author:   Shaun
# 檔案功能描述: 使用while loop 檢查機台連線狀態。
# PLC_connect:使用thread 多線程處理檢查機台連線狀態。
# 讀取machine_config.xlsx 定義機台IP及收集數據欄位資料類型。
# -------------------------------------------------------------------
import os
import sys
import time
import threading
from fx5 import FX5
import pandas as pd
import numpy as np
import sqlalchemy as sqla
from sqlalchemy import update
from datetime import datetime
from datetime import timedelta
from ping3 import ping
NOW = datetime.today()


def PLC_connect(name,host,reg,q,i,times):
	res={'name':'','parts': np.nan ,'value': np.nan , 'user_id': np.nan ,'work_order_id': np.nan}
	global NOW
	noon_st=NOW.replace(hour=12,minute=0,second=0,microsecond=0)
	noon_ed=noon_st.replace(hour=13)
	dusk_st=noon_st.replace(hour=17)
	dusk_ed=dusk_st.replace(minute=30)
	# print(NOW,'\n',
	# noon_st,'\n',
	# noon_ed,'\n',
	# dusk_st,'\n',
	# dusk_ed
	# )
	# print(noon_st<=NOW<noon_ed)
	try:
		chk_ping=ping(host.split(":")[0],timeout=1)
		res['parts']=np.nan
		if chk_ping:
			res['name']=name
			res['value']=chk_ping

			q[i]=res
		else:
			res['name']=name
			res['value']=chk_ping

			q[i]={}

		# try:
		# 	fx5 = FX5.get_connection(host)
			
		# 	#fx5.write('D500',times)
		# 	res['name']=name
		# 	res['parts']=fx5.read(reg['parts'],reg['parts_type']) if not pd.isnull(reg['parts_type']) else 0
		# 	res['value']=fx5.read(reg['status'],reg['status_type']) if not pd.isnull(reg['status_type']) else 0
		# 	res['user_id']=fx5.read(reg['UID'],reg['UID_type']) if not pd.isnull(reg['UID_type']) else 0
		# 	res['work_order_id']=fx5.read(reg['WID'],reg['WID_type']) if not pd.isnull(reg['WID_type']) else 0
		# 	res['option1']=fx5.read(reg['option1'],reg['op_type1']) if not pd.isnull(reg['op_type1']) else 0
		# 	res['option2']=fx5.read(reg['option2'],reg['op_type2']) if not pd.isnull(reg['op_type2']) else 0
		# 	#print(res)
		# 	#print(times)
		# 	if noon_st<=NOW<noon_ed and res['value']!=1:
		# 		res['value']=res['value']+500
		# 	elif dusk_st<=NOW<dusk_ed and res['value']!=1:
		# 		res['value']=res['value']+500

		# 	q[i]=res


		# except OSError as err:
		# 	print(name, err)
		# 	res['name']=name
		# 	res['parts']=np.nan
		# 	chk_ping=ping(host.split(":")[0])
		# 	if chk_ping:
		# 		# res['value']==np.nan
		# 		q[i]={}
		# 	else:
		# 		if noon_st<=NOW<noon_ed:
		# 			res['value']=509
		# 		elif dusk_st<=NOW<dusk_ed:
		# 			res['value']=509
		# 		else:
		# 			res['value']=9
		# 		q[i]=res
	except OSError as err:
		print(name, err)
		exit()


if __name__ == '__main__':
	allst=time.time()

	# read from machine config
	machinedf=pd.read_excel("machine_config.xlsx")
	print(time.time()-allst)
	machinedf.columns=['name','host','parts','parts_type','status','status_type','UID','UID_type','WID','WID_type','option1','op_type1','option2','op_type2']
	#machinedf.fillna('NaN',inplace=True)
	#machinedf=machinedf.convert_dtypes()
	
	print(machinedf)
	print(time.time()-allst)
	
	times=0
	n=len(machinedf)
	q = [{} for _ in range(n)]
	tempdf=pd.DataFrame({},columns=['name','date','parts','during','speed'])
	
	while True:
		allst=time.time()
		times += 1
		NOW=datetime.today()
		print(NOW)
		threads=[]
		reg = {}
		for i in range(n):
			name=machinedf.iloc[i,0].lower()
			host=machinedf.iloc[i,1]
			reg=machinedf.iloc[i,2:].to_dict()
			#print(name,host,reg,times)
			#log=PLC_connect(name,host,reg,q,i,times)
			threads.append(threading.Thread(target=PLC_connect, args=(name,host,reg,q,i,times)))
			threads[i].start()
			#threads[i].join()

		for i in range(n):
			# name=machinedf.iloc[i,0].lower()
			threads[i].join()
			#print(name + '----- TestFX5 start ------')
			#print(q[i])

		print(q)
		df=pd.DataFrame(q)
		df=df.dropna(subset='name')
		df=df.reset_index()
		df['work_order_id']=df['work_order_id'].astype("Int64")
		df['parts']=df['parts'].astype("Int64")
		print(df)
		print(datetime.now())
		print('done all cost time %f' % (time.time()-allst))
		time.sleep(1)
	
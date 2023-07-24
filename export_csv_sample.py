#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------
# Date: 2021/11/08
# Author:   Shaun
# DB_connect: 使用class 操作資料庫讀取、寫入及資料庫資料減量。

import time
import pandas as pd
import sqlalchemy as sqla
from datetime import timedelta
from datetime import datetime

import sys,os
VALUE_OF_PLAN_ST =1
VALUE_OF_PLAN_ED =2
DAY_START= datetime.min.time().replace(hour=8)
CHK_TOLERANCE = timedelta(minutes=5)
NOW=datetime.today().date()

class DB_connect:
	# database setting
	__engine = None
	today_min = None
	today_max = None
	today_now = None

	def __init__(self):
		global NOW
		with open ("dbconfig.txt", "r") as dbconfig:
			data=dbconfig.readlines()
		self.__engine=sqla.create_engine('mysql+mysqlconnector://'+data[0])
		self.today_min=datetime.combine(NOW,DAY_START)
		self.today_max=self.today_min.replace(hour=20,minute=40)
		print(self.today_min,self.today_max,NOW)

		
	def read_from(self,name):
		t1=time.time()
		table = name

		# for test
		# seldf = pd.read_csv("testdb.csv", parse_dates=['date'])
		# return seldf

		#select ID between today
		sql = "Select id from " + table + " where date between '" + str(self.today_min) + "' and '" + str(self.today_max) + "'"
		iddf = pd.read_sql_query(sql, self.__engine)
		#print(iddf)
		if not iddf.empty and len(iddf) >=2 :
			idarr=tuple(iddf['id'].astype(int).values.tolist())
			#print(idarr)
			sql = "Select id,date,parts,value,user_id,work_order_id,option1,option2 from " + table + " where id in "+ str(idarr)
			seldf = pd.read_sql_query(sql, self.__engine)
			#seldf=predf[predf['date'].between(today, now)]
			#print(seldf)
			print('read database cost %f sec' %(time.time()-t1))
			#return predf.iloc[0,0:].to_dict()
			return seldf
		else:
			seldf = pd.DataFrame({},columns=['id,date,parts,value,user_id,work_order_id,option1,option2'])
			return seldf


	def close(self):
		self.__engine.dispose()		

if __name__ == '__main__':
	# dir=r'\\Nas\uta-share\UTA資料庫\UTA共用區\Q-專案執行\MES (製造執行系統)\生產統計表' + '\\'
	dir='/home/uta_iot/grafana_output/生產統計表/'
	if len(sys.argv) >1:
		NOW=datetime.strptime(sys.argv[1], "%Y%m%d").date()
		vdate=sys.argv[1]
	else:
		if datetime.today().weekday()==0 and datetime.today() < datetime.today().replace(hour=8):
			shift=timedelta(days=3)
		elif datetime.today() < datetime.today().replace(hour=8):
			shift=timedelta(days=1)
		else:
			shift=timedelta(days=0)
		NOW=datetime.today().date() - shift
		vdate=str(NOW).replace('-','')

	print(NOW)
	allst=time.time()
	# read from machine config
	# machinedf=pd.read_excel("machine_config.xlsx")
	# machinedf.columns=['name','host','parts','parts_type','status','status_type','UID','UID_type','WID','WID_type','option1','op_type1','option2','op_type2']
	# machine = machinedf['name'].values.tolist()
	machine = ['MJ4','MX14']
	print(machine)
	print(time.time()-allst)
	

	for i in range(len(machine)):
		name=machine[i].lower()
		path_d =f"{dir}{name.upper()}/" 
		path = f"{path_d}{name}-{vdate}.csv"
		if not os.path.exists(path_d):
			os.makedirs(path_d)
		if os.path.exists(path):
			os.remove(path)
		conn = DB_connect()
		seldf = conn.read_from(name)
		# print(seldf)
		seldf.to_csv(path,index=False)
		
	conn.close()
	alled = time.time()
	# 列印結果
	print("loop cost %f sec" % (alled - allst))  # 會自動做進位
	

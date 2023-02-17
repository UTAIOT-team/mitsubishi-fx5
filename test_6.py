#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------
# Date: 2021/11/08
# Author:   Shaun
# 檔案功能描述: 使用while loop 收集機台生產資料，並存至資料庫。
# PLC_connect:使用thread 多線程處理機台通訊，不關閉連線通道，持續收集狀態。
# DB_connect: 使用class 操作資料庫讀取及寫入。
# 讀取machine_config.xlsx 定義機台IP及收集數據欄位資料類型。
# -------------------------------------------------------------------
from operator import mod
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
import subprocess
from subprocess import PIPE
NOW = datetime.today()
import LineNotify
import sqlite3


def PLC_connect(name,host,reg,q,i,times,e):
	res={'name':name ,'parts': np.nan ,'value': np.nan , \
		'user_id': np.nan ,'work_order_id': np.nan,'option1':np.nan,'option2':np.nan,'ping':np.nan}
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
		hostip=host.split(":")[0]
		gateway=hostip.replace(".1.",".3.")
		chk_ping=ping(hostip,timeout=1)
		if name=='ai4':
			# chk2_ping=ping(hostip,timeout=1)
			args = ['sshpass', '-p', '', 'ssh', '-oHostKeyAlgorithms=+ssh-rsa', 'root@10.10.0.181', 'ping 10.10.1.49 -w 1 -c 1']
			comp_process = subprocess.run(args,stdout=PIPE, stderr=PIPE)
			print(args)
			print(comp_process.stdout)
			if comp_process.returncode==0:
				chk2_ping=float(str(comp_process.stdout).split("/")[-1].replace(" ms\\n'",""))
			else:
				chk2_ping=None
			print(name,chk2_ping,comp_process.returncode,type(comp_process.returncode))
		else:
			chk2_ping=ping(gateway,timeout=1)

		res['ping']=chk_ping
		if chk_ping:
			try:
				fx5 = FX5.get_connection(host)
				
				#fx5.write('D500',times)
				res['parts']=fx5.read(reg['parts'],reg['parts_type']) if not pd.isnull(reg['parts_type']) else 0
				res['value']=fx5.read(reg['status'],reg['status_type']) if not pd.isnull(reg['status_type']) else 0
				res['user_id']=fx5.read(reg['UID'],reg['UID_type']) if not pd.isnull(reg['UID_type']) else 0
				res['work_order_id']=fx5.read(reg['WID'],reg['WID_type']) if not pd.isnull(reg['WID_type']) else 0
				res['option1']=fx5.read(reg['option1'],reg['op_type1']) if not pd.isnull(reg['op_type1']) else 0
				res['option2']=fx5.read(reg['option2'],reg['op_type2']) if not pd.isnull(reg['op_type2']) else 0
				#print(res)
				#print(times)
				if noon_st<=NOW<noon_ed and res['value']!=1:
					res['value']=res['value']+500
				elif dusk_st<=NOW<dusk_ed and res['value']!=1:
					res['value']=res['value']+500
				q[i]=res

			except Exception as err:
				print("PLC connect err",name, err)
				e[i] = {'name':name,'err': 'PLC ERR ' + str(err) + 'and gateway_ping:' + str(chk2_ping)}
				pass

		else:
			e[i] = {'name':name,'err': 'NO responds. chk_ping(' + str(chk_ping) +')'+ 'and gateway_ping:' + str(chk2_ping)}
			#ping 超時判斷，待對策處理
			if not chk2_ping:
				if noon_st<=NOW<noon_ed:
					res['value']=509
				elif dusk_st<=NOW<dusk_ed:
					res['value']=509
				else:
					res['value']=9
				q[i]=res
			
	except Exception as err:
		print("ping err",name, err)
		e[i] = {'name':name,'err': 'PING ERR ' + str(err)}


class DB_connect:
	# database setting
	__engine = None

	def __init__(self):
		with open ("dbconfig.txt", "r") as dbconfig:
			data=dbconfig.readlines()
		self.__engine=sqla.create_engine('mysql+mysqlconnector://'+data[0])

	def write_to_sql(self,df):
		# add to database
		for i in range(len(df)):
			table=df.loc[i,'name'].lower()
			newdf.iloc[[i],2:].to_sql(table, self.__engine, if_exists='append', index=False)

	def catch_sqlite(self):
		if os.path.exists('temp.db'):
			with sqlite3.connect('temp.db') as dbcon:
				tables = list(pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", dbcon)['name'])
				# out = {tbl : pd.read_sql_query(f"SELECT * from {tbl}", dbcon) for tbl in tables}
				for table in tables:
					df=pd.read_sql_query(f"SELECT * from {table}", dbcon)
					df.to_sql(table, self.__engine, if_exists='append', index=False)

			os.remove('temp.db')

	def read_schedule_view(self):
		table = "schedule_view"
		sql = "Select name, A+B+C+D+overtime as hrs from " + table
		viewdf = pd.read_sql_query(sql, self.__engine)
		return viewdf

	def close(self):
		self.__engine.dispose()


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
		e = [{} for _ in range(n)]
		for i in range(n):
			name=machinedf.iloc[i,0].lower()
			host=machinedf.iloc[i,1]
			reg=machinedf.iloc[i,2:].to_dict()
			#print(name,host,reg,times)
			#log=PLC_connect(name,host,reg,q,i,times)
			threads.append(threading.Thread(target=PLC_connect, args=(name,host,reg,q,i,times,e)))
			threads[i].start()
			#threads[i].join()

		for i in range(n):
			# name=machinedf.iloc[i,0].lower()
			threads[i].join()
			#print(name + '----- TestFX5 start ------')
			#print(q[i])

		# print(q)
		# print((len(e)),e)
		for i in range(len(e)-1,-1,-1):
			#print(i,e[i])
			if e[i]=={}:
				del e[i]

		ef=pd.DataFrame(e)
		print(ef)
		if not ef.empty:
			ef['date']=NOW
			with open('./err.csv', mode = 'a+',newline='\n') as f:
				ef.to_csv(f , index=False,sep=",", line_terminator='\n', encoding='utf-8')
		newdf=pd.DataFrame(q)
		newdf=newdf.dropna(subset='name')
		newdf=newdf.reset_index()
		newdf['work_order_id']=newdf['work_order_id'].astype("Int64")
		newdf['parts']=newdf['parts'].astype("Int64")
		
		# print(df['ping'])
		# print(df['ping'].dtypes)
		newdf=newdf.drop(columns=['ping'])
		# print(newdf)
		# print(NOW,times,'\n',tempdf)
		if times==1: #init
			tempdf['name']=newdf['name']
			tempdf['parts']=newdf['parts']
			tempdf['date']=NOW
			
		elif times%6==1:
			tempdf['during']=NOW-tempdf['date']
			tempdf['speed']=round((newdf['parts']-tempdf['parts'])/tempdf['during'].dt.total_seconds()*60,ndigits=0)
			tempdf['parts']=newdf['parts']
			tempdf['date']=NOW
			newdf['speed']=tempdf['speed']
		else:
			newdf['speed']=tempdf['speed']
		
		try:
			if times>6:
				conn = DB_connect()
				viewdf=conn.read_schedule_view()
				if viewdf.hrs.eq(0).any():
					left=newdf[(newdf.value.eq(9)) | (newdf.value.eq(509))]
					right=viewdf[viewdf.hrs.eq(0)].copy(deep=False)
					right.columns=['name','value']
					right.value=10
					right=right.loc[right.name.isin(left.name)]
					right.index=left[left.name.isin(right.name)].index
					print(right)
					newdf.update(right)
				if times%6==1: # 每分鐘推播
				# if times%30==1: # 每5分鐘推播
					msg=""
					if newdf.value.eq(9).any():
						msgdf1=newdf[newdf.value.eq(9)]
						if msgdf1.empty==False:
							msg1=msgdf1.loc[:,['name','value']].to_string()
							msg+="\n應開機未開機:\n"+msg1
							print("msg1",msg1)
					if newdf.value.eq(10).any():
						left=newdf[newdf.value.ne(10)]
						right=viewdf[viewdf.hrs.eq(0)].copy(deep=False)
						right.columns=['name','value']
						right.value=10
						right=right.loc[right.name.isin(left.name)]
						msgdf2=left.loc[left.name.isin(right.name)]
						if msgdf2.empty==False:
							msg2=msgdf2.loc[:,['name','value']].to_string()
							print("msg2",msg2)
							msg+="\n無計畫開機:\n"+msg2
					if msg!="":
						if (NOW>NOW.replace(hour=9,minute=00)) or (NOW.weekday()<5):
							LineNotify.lineNotifyMessage(msg)
					
				conn.catch_sqlite()
				conn.write_to_sql(newdf)
				conn.close()
		
		except Exception as e:
			LineNotify.lineNotifyMessage(e)
			newdf['date']=NOW
			conn = sqlite3.connect('temp.db')
			for i in range(len(newdf)):
				table=newdf.loc[i,'name'].lower()
				sql = "Select * from " + table
				newdf.iloc[[i],2:].to_sql(table, conn, if_exists='append', index=False)

		print('new------------------')
		print(newdf)
		print('temp------------------')
		print(tempdf)
		print(datetime.now())
		print('done all cost time %f' % (time.time()-allst))
		time.sleep(10)
			


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
			if comp_process.returncode==0:
				chk2_ping=str(comp_process.stdout).split("/")[-1].replace(" ms\\n'","")
			else:
				chk2_ping=None
		elif name=='mj4':
			chk2_ping=ping('192.168.2.187',timeout=1)
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

	def read_last_from(self,name):
		t1=time.time()
		table = name

		#select last ID
		sql = "Select id from " + table +" ORDER BY id DESC LIMIT 0 , 1"
		iddf=pd.read_sql_query(sql, self.__engine)
		if not iddf.empty:
			sql = "Select * from " + table +" Where id="+iddf.iloc[0,0].astype("str")
			predf=pd.read_sql_query(sql, self.__engine)
			# predf.drop(columns=['id',],inplace=True)
			#predf.columns=['parts','status','UID','WID']
			predf.insert(0,'name',table)
			#st1=predf.loc[0,'status'].astype("int")
		else:
			#can't find id
			#st1=None
			predf=pd.DataFrame([{'name':name, 'parts':np.nan, 'value':np.nan, 'user_id':np.nan, 'work_order_id':np.nan, 'option1':np.nan, 'option2':np.nan}])
		t2=time.time()
		return predf.iloc[0,0:].to_dict()
		print('select id from database %s cost time %f' % (name,(t2-t1)))

	def write_to_sql(self,df,times,tempdf):
		pd.set_option('display.max_columns', None)
		print('new------------------')
		# newdf=pd.json_normalize(q)
		newdf=df.drop(columns=['ping'])
		newdf['work_order_id']=newdf['work_order_id'].astype("Int64")
		print(newdf)
		preq=[{} for _ in range(len(newdf))]
		
		# read last from database
		srtt1=time.time()
		for i in range(len(newdf)):
			preq[i]=self.read_last_from(newdf.loc[i,'name'])

		print('pre------------------')
		predf=pd.json_normalize(preq)
		predf['work_order_id']=predf['work_order_id'].astype("Int64")
		print(predf)
		global NOW
		print(tempdf.speed.eq(0).all())
		print(tempdf.speed.eq(0).any())
		if times==1 or tempdf.speed.empty:
			if tempdf.speed.empty:
				tempdf['name']=newdf['name']
				tempdf['parts']=newdf['parts']
				tempdf['date']=NOW
				tempdf['during']=NOW-predf['date']
				tempdf['speed']=round((newdf['parts']-predf['parts'])/tempdf['during'].dt.total_seconds()*60,ndigits=0)
				newdf['speed']=tempdf['speed']
			else:
				for i in range(len(tempdf)):
					if not tempdf.loc[i,'speed']>0:
						print(tempdf.loc[i,['name','speed']])
						tempdf.loc[i,'parts']=newdf.loc[i,'parts']
						tempdf.loc[i,'date']=NOW
						tempdf.loc[i,'during']=NOW-predf.loc[i,'date']
						tempdf.loc[i,'speed']=round((newdf.loc[i,'parts']-predf.loc[i,'parts'])/tempdf.loc[i,'during'].total_seconds()*60,ndigits=0)
						newdf.loc[i,'speed']=tempdf.loc[i,'speed']
			print('tempdf')
			print(tempdf)

			
		elif times%6==1:
			tempdf['during']=NOW-tempdf['date']
			print(tempdf)
			tempdf['speed']=round((newdf['parts']-tempdf['parts'])/tempdf['during'].dt.total_seconds()*60,ndigits=0)
			tempdf['parts']=newdf['parts']
			tempdf['date']=NOW
			newdf['speed']=tempdf['speed']
			print(newdf)
		else:
			newdf['speed']=tempdf['speed']

		# newdf['during']=datetime.today()-predf['date']
		# newdf['speed']=round((newdf['parts']-predf['parts'])/newdf['during'].dt.total_seconds()*60,ndigits=0)
		# print(newdf)
		# newdf.drop(columns=['during'],inplace=True)

		# add to database
		for i in range(len(newdf)):
			# print(q[i])
			# print(str(newdf.iloc[i,2]),type(newdf.iloc[i,2]))
			# None is a nan as numpy.float type
			# if(str(newdf.iloc[i,2])!= 'nan' and str(newdf.iloc[i,2])!= 'None' ):
			table=newdf.loc[i,'name'].lower()
			sql = "Select * from " + table
			st1=predf.loc[i,'value']
			st2=st1 if pd.isna(newdf.loc[i,'value']) else newdf.loc[i,'value']
			if pd.isnull(newdf.loc[i,'parts']):
				newdf.loc[i,'parts']=predf.loc[i,'parts']

			if st1!=st2:
				#print(table,st1,st2,'not equal')
				#print(newdf.iloc[i,1:])
				#resdf=newdf.iloc[[i],1:]
				print(newdf.iloc[[i],1:])
				print(table,st1,st2,'not equal','| write to database')
			else:
				print(table,st1,st2,'is equal','| write to database')
			newdf.iloc[[i],2:].to_sql(table, self.__engine, if_exists='append', index=False)
		return tempdf

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
		df=pd.DataFrame(q)
		df=df.dropna(subset='name')
		df=df.reset_index()
		df['work_order_id']=df['work_order_id'].astype("Int64")
		df['parts']=df['parts'].astype("Int64")
		print(df)
		conn = DB_connect()
		tempdf = conn.write_to_sql(df,times,tempdf)
		print(tempdf)
		conn.close()
		print(datetime.now())
		print('done all cost time %f' % (time.time()-allst))
		time.sleep(10)
	
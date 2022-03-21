#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------
# Date: 2021/11/08
# Author:   Shaun
# 檔案功能描述: 使用while loop 收集機台生產資料，計算當日OEE資料，並存至資料庫。
# DB_connect: 使用class 操作資料庫讀取、寫入及資料庫資料減量。
# OEE_Class: 使用class 計算OEE相關數據。
# 讀取machine_config.xlsx 機台名稱。
# -------------------------------------------------------------------
#公式定義
#1.OEE
#公式:OEE=A*P*Q=(實際機時/標準機時)*(實際產能/標準產能)*1
#A=稼働率  P=產能效率   Q=良率
#A=機台有效工作時間(0:0~now)/機台設定之作業工時(0:0~now) 
#P=機台成品數/(機台開機時間*每小時標準產能)

#2.FTE
#公式:以機器FTE(天)為標準，計算每位操作員的個人FTE。

#3.設備使用率
#公式:設備使用率=實際機時/(製程機台數*24小時)
# -------------------------------------------------------------------
import time
import threading
import pandas as pd
import sqlalchemy as sqla
from datetime import timedelta
from datetime import datetime

class OEE_Class:
	machine = ''
	workid = None
	work_time = None
	A = 0
	P = 0
	Q = 0
	OEE = 0
	actual_pcs = 0 #今日生產數量
	total_time = timedelta(days=0) #設備開機時間
	nomal_time = timedelta(days=0) #正常運轉時間
	capacity = 0 #pcs / hour 標準產能
	standard_pcs = 0 #今日標準產量

	# alarm
	nomal_min = 0
	nomal_max = 0
	nomal_avg = 0
	alarm_min = 0
	larm_max = 0
	alarm_avg = 0
	# alarm top 5
	piedf = None
	


	def __init__(self,seldf,name):
		t1=time.time()
		self.machine = name
		self.actual_pcs = seldf["parts"].max()-seldf["parts"].min()
		st_dt = seldf["date"].min()
		end_dt = seldf["date"].max()
		self.total_time = end_dt-st_dt
		standard=0
		self.nomal_time = timedelta(days=0)
		
		work_time=pd.DataFrame({},columns=['work_id', 'status', 'during'])
		#for i in range(seldf.shape[0]-1):
		#    status=seldf.iloc[i,3].astype(int)
		#    work_id=seldf.iloc[i,5].astype(int)
		#    during=seldf.iloc[i+1,2]-seldf.iloc[i,2]
		#    work_time=work_time.append({'work_id':work_id, 'status':status,'during':during},ignore_index=True)
		#    if status==1:
		#        #print(i+1,status,during,type(during))
		#        nomal_time+=during
		#    print('%d calc cost %f sec' %(i,time.time()-allst))
		#use for loop too slowly.......
		#seldf=seldf.loc[seldf['value']!=seldf['value'].shift(1)]

		#print(seldf)
		#print('seldf----------------------------------------------------------')
		work_time[['work_id','status']]=seldf[['work_order_id','value']]
		work_time['during']=seldf['date'].shift(-1)-seldf['date']
		#work_time.to_csv(name + ".csv")
		work_time=work_time.dropna()
		#print(work_time)
		#print('work_time----------------------------------------------------------')
		#print('calc cost %f sec' %(time.time()-t1))
		#print(work_time[['status','during']].loc[work_time['status']==1])
		#print('nomal_time----------------------------------------------------------')
		self.nomal_time=work_time['during'].loc[work_time['status']==1].agg('sum')
		
		self.work_time = work_time
		self.__alarm_analyze()

		self.work_time=work_time.groupby(['work_id'])['during'].agg('sum').reset_index()
		#print(self.work_time['work_id'].astype(int).values.tolist())
		self.workid = '(' + str(self.work_time['work_id'].astype(int).values[0]) + ')' if len(self.work_time) <2 \
			else tuple(self.work_time['work_id'].astype(int).values.tolist())
		
		#print(self.workid)
		#print(self.work_time)

		

	def calc_standrad(self,workdf):
		standard=0
		during = timedelta(days=0)
		ahour = timedelta(hours=1)
		print(self.work_time)
		for i in range(self.work_time.shape[0]):
			work_id=self.work_time.iloc[i,0].astype(int)
			during=self.work_time.iloc[i,1]
			cap=workdf.loc[work_id,'standard_CAP'].astype(int)
			standard+=during/ahour*cap
			print(standard,cap,during.total_seconds(),ahour.total_seconds())
			#print(work_id,psc,during,int(standard))
			#print()
		self.standard_pcs=standard
		

	def calc_OEE(self):
		self.standard_pcs= self.standard_pcs if self.standard_pcs != 0 else 1
		print('---------------------------------------------------')
		print('正常啟動:',self.nomal_time)
		print('開機時間:',self.total_time)
		self.A=float(self.nomal_time/self.total_time*100)
		print("稼動率",self.A,"%")
		print("今日產能:",self.actual_pcs)
		print("標準產能:",int(self.standard_pcs))
		self.P=float(self.actual_pcs/self.standard_pcs*100)
		print("產能效率:",self.P,"%")
		self.Q=float(1)
		self.OEE=float(self.A*self.P*self.Q/100)
		print("OEE:",self.OEE,"%")
		now=str(datetime.now())
		
		ls=[[now, self.machine, self.OEE, self.A, self.P, self.Q, self.nomal_min, self.nomal_max, self.nomal_avg,
			self.alarm_min, self.alarm_max, self.alarm_avg]]
		#print(self.nomal_min, self.nomal_max, self.nomal_avg, self.alarm_min, self.alarm_max, self.alarm_avg)
		oeedf=pd.DataFrame(ls,columns=['date', 'name', 'OEE', 'Availability', 'Performance', 'Quality',
									   'nomal_min', 'nomal_max', 'nomal_avg', 'alarm_min', 'alarm_max', 'alarm_avg'])
		oeedf=oeedf.fillna(0)
		oeedf['Production']=self.nomal_time.total_seconds()
		oeedf['total_time']=self.total_time.total_seconds()
		oeedf['standard_pcs']=self.standard_pcs
		oeedf['actual_pcs']=self.actual_pcs
		return oeedf

	def __alarm_analyze(self):
		#work_time=work_time[work_time['during']!=timedelta(days=0)]
		self.nomal_min=self.work_time[self.work_time['status']==1].during.min().total_seconds()
		self.nomal_max=self.work_time[self.work_time['status']==1].during.max().total_seconds()
		self.nomal_avg=self.work_time[self.work_time['status']==1].during.mean().total_seconds()

		self.alarm_min=self.work_time[self.work_time['status']!=1].during.min().total_seconds()
		self.alarm_max=self.work_time[self.work_time['status']!=1].during.max().total_seconds()
		self.alarm_avg=self.work_time[self.work_time['status']!=1].during.mean().total_seconds()
		self.piedf=self.work_time.groupby(['status'])['during'].agg(['sum','count']).reset_index()
		#piedf.columns=['during']
		#piedf['times']=self.work_time.groupby(['status'])
		self.piedf.columns=['status','during','times']
		self.piedf['during']=self.piedf['during'].dt.total_seconds()
		self.piedf.insert(0,'name',self.machine)
		print(self.piedf)



class DB_connect:
	# database setting
	__engine = None

	def __init__(self):
		with open ("dbconfig.txt", "r") as dbconfig:
			data=dbconfig.readlines()
		self.__engine=sqla.create_engine('mysql+mysqlconnector://'+data[0])

	def read_capacity(self,idarr):
		table = "work_order"
		sql = "Select * from " + table + " where id in "+ str(idarr)
		workdf=pd.read_sql_query(sql, self.__engine,index_col='id')
		#print(workdf)
		return workdf

		
	def read_from(self,name):
		t1=time.time()
		today=str(datetime.combine(datetime.today().date(),datetime.min.time()))
		print(today)
		#t2=t1+timedelta(days=1)
		now=str(datetime.now())
		table = name

		#select ID between today
		sql = "Select id from " + table + " where date between '" + today + "' and '" + now + "'"
		iddf = pd.read_sql_query(sql, self.__engine)
		#print(iddf)
		if not iddf.empty and len(iddf) >=2 :
			idarr=tuple(iddf['id'].astype(int).values.tolist())
			#print(idarr)
			sql = "Select id,date,value,parts,work_order_id from " + table + " where id in "+ str(idarr)
			seldf = pd.read_sql_query(sql, self.__engine)
			#seldf=predf[predf['date'].between(today, now)]
			#print(seldf)
			print('read database cost % sec' %(time.time()-t1))
			#return predf.iloc[0,0:].to_dict()
			return seldf
		else:
			return None

	def write_to_sql(self,df,table):
		#add oee data to database
		#table = "oee"
		#sql = '''Select date, name, OEE, Availability, Performance, Quality, nomal_min, nomal_max, nomal_avg, alarm_min, alarm_max, alarm_avg from ''' + table
		sql = 'Select * from ' + table
		df.to_sql(table, self.__engine, if_exists='append', index=False)

	def reduce_data(self,name):
		t1=time.time()
		table = name
		
		#select ID to reduce
		sql = "Select * from " + table
		seldf = pd.read_sql_query(sql, self.__engine)
		#print(seldf['value']==seldf['value'].shift(1) & seldf['value']==seldf['value'].shift(-1))
		seldf['a']=seldf['value']==seldf['value'].shift(1)
		#seldf['b']=seldf['value']==seldf['value'].shift(-1)
		seldf['date']=seldf['date'].dt.date
		seldf['c']=seldf['date']==seldf['date'].shift(1)
		seldf['d']=seldf['date']==seldf['date'].shift(-1)
		#seldf['e']=seldf.a & seldf.b & seldf.c & seldf.d
		seldf['e']= seldf.a & seldf.c & seldf.d
		seldf.to_csv(name + ".csv")
		seldf=seldf.loc[seldf['e']==True]
		print(seldf)
		#return 0

		if not seldf.empty:
			idarr='(' + str(seldf['id'].astype(int).values[0]) + ')' if len(seldf) <2 \
				else tuple(seldf['id'].astype(int).values.tolist())
			print(idarr)

			# reduce data from database with select id
			with self.__engine.connect() as cnn:
				sql = f"""
						DELETE FROM {table}
						WHERE id in {idarr};
						"""
				cnn.execute(sql)
		print('reduce database cost % sec' %(time.time()-allst))



if __name__ == '__main__':
	allst=time.time()
	# read from machine config
	machinedf=pd.read_excel("machine_config.xlsx")
	machinedf.columns=['name','host','parts','parts_type','status','status_type','UID','UID_type','WID','WID_type']
	machine = machinedf['name'].values.tolist()
	print(machine)
	print(time.time()-allst)

	while True:
		allst = time.time()
		oeedf=pd.DataFrame()
		piedf=pd.DataFrame()
		for i in range(len(machine)):
			conn = DB_connect()
			conn.reduce_data(machine[i])
			#continue
			seldf = conn.read_from(machine[i])
			if seldf is not None:
				#print(seldf)
				oee = OEE_Class(seldf, machine[i])
				#print(oee.workid)
				workdf = conn.read_capacity(oee.workid)
				#print(workdf)
				t1=time.time()
				oee.calc_standrad(workdf)
				t2=time.time()
				print('calc standard cost %f sec' %(t2-t1))
				#print(oee.standard_pcs)
				oeedf = oeedf.append(oee.calc_OEE())
				piedf = piedf.append(oee.piedf)
		print(oeedf)
		#print(oeedf.iloc[0,0:])
		conn.write_to_sql(oeedf,'oee')
		conn.write_to_sql(piedf,'pie')

		alled = time.time()
		# 列印結果
		print("loop cost %f sec" % (alled - allst))  # 會自動做進位
		time.sleep(600)





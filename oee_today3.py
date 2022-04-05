#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------
# Date: 2021/11/08
# Author:   Shaun
# 檔案功能描述: 使用while loop 收集機台生產資料，計算當日OEE資料，並存至資料庫。
# DB_connect: 使用class 操作資料庫讀取、寫入及資料庫資料減量。
# OEE_Class: 使用class 計算OEE相關數據。
# 讀取machine_config.xlsx 機台名稱。
#--------------------------------------------------------------------
# schedule 定義
# 早班		08:00-17:00 標準工時 8h   除外工時 1h
# 中班		17:00-23:59 標準工時 6.5h 除外工時 0.5h
# 夜班-隔日	00:00-08:00 標準工時 8h
#
# 除外工時 定義
# 除外工時需做加班檢查
# 中午休息時間	12:00-13:00
# 晚上休息時間	17:00-17:30 累計至中班
# 
# 每日計畫行程自動判斷 定義
# 每日早班計畫 預設	08:00-17:00
# 每日中班計畫 預設	17:30-18:30 每小時累計至 23:59
# 每日晚班計畫 預設	00:00-01:00 每小時累計至 08:00
# 
# 
# -------------------------------------------------------------------
# OEE公式定義
# OEE,標準產量,實際產量  先分班計算,再累計計算(由SQL 語法於dashboard累計)
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
VALUE_OF_PLAN_ST =1
VALUE_OF_PLAN_ED =2
# DAY_SHIFT=timedelta(days=1)
test_date = datetime(2022,4,1,0,0,0).date()
DAY_SHIFT=  test_date - datetime.today().date()
print('DAY_SHIFT',DAY_SHIFT)
DAY_START= datetime.min.time().replace(hour=8)
CHK_TOLERANCE = timedelta(minutes=10)

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
	speed = 0 #機台生產速度

	# alarm
	nomal_min = 0
	nomal_max = 0
	nomal_avg = 0
	alarm_min = 0
	larm_max = 0
	alarm_avg = 0
	# alarm top 5
	piedf = None
	


	def __init__(self,seldf,name,rest):
		t1=time.time()
		self.machine = name
		self.actual_pcs = seldf["parts"].max()-seldf["parts"].min()
		seldf=seldf.dropna()
		# st_dt = seldf.date.min()
		st_dt= seldf[seldf.value==1].date.min()
		end_dt = seldf.date.max()
		print(st_dt.time() , datetime.min.time().replace(hour=8),st_dt.time() > datetime.min.time().replace(hour=8))
		if st_dt.time() > datetime.min.time().replace(hour=8):
			st_dt=st_dt.replace(hour=8,minute=0,second=0,microsecond=0)
			print(st_dt)
		# st_dt = seldf.date.min()
		# max_id = seldf[seldf.value==1].index.max()
		# print(max_id)
		# end_dt = seldf.shift(-1).loc[max_id,'date']
		# if pd.isnull(end_dt):
		# 	end_dt=seldf.loc[max_id,'date']
		print('test st ed rest---------------------------------------')
		print(st_dt,end_dt,rest)
		self.total_time = end_dt-st_dt-rest
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
		work_time['work_id']=seldf['work_order_id']
		work_time['status']=seldf['value']
		work_time['during']=seldf['date'].shift(-1)-seldf['date']
		
		work_time['parts']=seldf['parts']
		work_time['parts']=work_time['parts'].shift(-1)-work_time['parts']
		#work_time.to_csv(name + ".csv")
		print(seldf)
		print(work_time)
		print('work_time----------------------------------------------------------')
		work_time=work_time.dropna()
		print(work_time)
		#print('work_time----------------------------------------------------------')
		#print('calc cost %f sec' %(time.time()-t1))
		#print(work_time[['status','during']].loc[work_time['status']==1])
		self.nomal_time=work_time['during'].loc[work_time['status']==1].agg('sum')
		self.work_time = work_time
		if self.total_time < self.nomal_time:
			self.total_time = self.nomal_time
		print('nomal_time ,total_time----------------------------------------------------------')
		print(self.nomal_time , self.total_time)
		
		spdf=pd.DataFrame({},columns=['parts', 'during'])
		print('speed-----------------------------------------------------')
		print(self.work_time[self.work_time['status']==1])
		if not self.work_time[self.work_time['status']==1].empty:
			spdf['parts']=self.work_time[self.work_time['status']==1]['parts']
			spdf['during']=self.work_time[self.work_time['status']==1].during.dt.total_seconds()
			spdf['speed']=spdf.apply(lambda x: round(x['during'] / x['parts'],ndigits=2) if x['parts']!=0 else 0 , axis=1)
			spdf=spdf[spdf.parts!=0]
			print(spdf)
			# self.speed=max(spdf['speed'])
			self.speed=spdf['during'].sum() / spdf['parts'].sum()
		else:
			self.speed=0
		
		
		
		self.__alarm_analyze()

		self.work_time=work_time.groupby(['work_id'])['during'].agg('sum').reset_index()
		#print(self.work_time['work_id'].astype(int).values.tolist())
		
		if len(self.work_time)>0:
			self.workid = '(' + str(self.work_time['work_id'].astype(int).values[0]) + ')' if len(self.work_time) <2 \
				else tuple(self.work_time['work_id'].astype(int).values.tolist())
		
		#print(self.workid)
		print(self.work_time)
		
		

		
	# 2022/03/25 改變算法
	# def calc_standrad(self,workdf):
		# standard=0
		# during = timedelta(days=0)
		# ahour = timedelta(hours=1)
		# # print(self.work_time)
		# for i in range(self.work_time.shape[0]):
			# work_id=self.work_time.iloc[i,0].astype(int)
			# during=self.work_time.iloc[i,1]
			# cap=workdf.loc[work_id,'standard_CAP'].astype(int)
			# standard+=during/ahour*cap
			# # print(standard,cap,during.total_seconds(),ahour.total_seconds())
			# #print(work_id,psc,during,int(standard))
			# #print()
		# self.standard_pcs=standard
		
	# 2022/03/25 改用機台標準速度計算
	# class DB_connect.read_capacity 設變
	# def calc_standrad(self,workdf):
	def calc_standrad(self,cap):
		standard=0
		during = self.total_time
		print(during)
		ahour = timedelta(hours=1)
		# print(self.work_time)
		print(cap)
		standard=during/ahour*cap
		self.standard_pcs=standard
		

	def calc_OEE(self):
		self.standard_pcs= self.standard_pcs if self.standard_pcs != 0 else 1
		print('---------------------------------------------------')
		print('正常啟動:',self.nomal_time)
		print('開機時間:',self.total_time)
		self.A=float(self.nomal_time/self.total_time*100)
		print("稼動率",self.A,"%")
		print("今日產能:",self.actual_pcs)
		print("標準產能:",round(self.standard_pcs,0))
		self.P=float(self.actual_pcs/self.standard_pcs*100)
		print("產能效率:",self.P,"%")
		self.Q=float(1)
		self.OEE=float(self.A*self.P*self.Q/100)
		print("OEE:",self.OEE,"%")
		now=str(datetime.now())
		print('機台速度',self.speed, 'sec/pcs')
		
		ls=[[now, self.machine, self.OEE, self.A, self.P, self.Q, self.nomal_min, self.nomal_max, self.nomal_avg,
			self.alarm_min, self.alarm_max, self.alarm_avg,self.speed]]
		#print(self.nomal_min, self.nomal_max, self.nomal_avg, self.alarm_min, self.alarm_max, self.alarm_avg)
		oeedf=pd.DataFrame(ls,columns=['date', 'name', 'OEE', 'Availability', 'Performance', 'Quality',
									   'nomal_min', 'nomal_max', 'nomal_avg', 'alarm_min', 'alarm_max', 'alarm_avg','speed'])
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
	today_min = ''
	today_max=''

	def __init__(self):
		# with open ("dbconfig.txt", "r") as dbconfig:
		# 	data=dbconfig.readlines()
		# self.__engine=sqla.create_engine('mysql+mysqlconnector://'+data[0])
		self.today_min=datetime.combine(datetime.today().date(),DAY_START) + DAY_SHIFT 
		self.today_max=self.today_min + timedelta(days=1)
		
	def read_schedule(self,name,seldf):
		rest= timedelta(0)
		table = "schedule_raw"
		# sql = "Select * from " + table + " where date between '" \
		# 	+ str(self.today_min) + "' and '" + str(self.today_max) + "' and name='" + name + "'"
		# schdf = pd.read_sql_query(sql, self.__engine)
		schdf = pd.DataFrame({},columns=['id','date','value','parts','work_order_id'])
		global VALUE_OF_PLAN_ST,VALUE_OF_PLAN_ED
		today_min=self.today_min
		today_max=self.today_max
		print(today_min,today_max)
		if schdf.empty:
			init=[
				[today_min.replace(hour=8),name,VALUE_OF_PLAN_ST,'morning'],#morning
				[today_min.replace(hour=12),name,VALUE_OF_PLAN_ED,'noon'],#noon
				[today_min.replace(hour=13),name,VALUE_OF_PLAN_ST,'afternoon'],#afternoon
				[today_min.replace(hour=17),name,VALUE_OF_PLAN_ED,'dusk'],#下班
				[today_min.replace(hour=17,minute=30),name,VALUE_OF_PLAN_ED,'dusk'],#下班
				[today_min.replace(hour=17,minute=30),name,VALUE_OF_PLAN_ED,'night'],#night
				[today_min.replace(hour=17,minute=30),name,VALUE_OF_PLAN_ED,'night'],#night
				[today_max.replace(hour=0),name,VALUE_OF_PLAN_ED,'midnight'],#midnight
				[today_max.replace(hour=0),name,VALUE_OF_PLAN_ED,'midnight'],#midnight
				]
			schdf=pd.DataFrame(init,columns=['date', 'name', 'value','raw_by'])
			# 新增至資料庫 and reload
			# schdf.to_sql(table, self.__engine, if_exists='append', index=False)
			# schdf = pd.read_sql_query(sql, self.__engine)
			# print('test schdf-------------------------------------------------')
			# print(schdf)
			
		# Define check time
		chk8 = today_min - CHK_TOLERANCE
		chk12 = today_min.replace(hour=12) + CHK_TOLERANCE
		chk13 = today_min.replace(hour=13) - CHK_TOLERANCE
		chk17 = today_min.replace(hour=17) + CHK_TOLERANCE
		chk1730 = today_min.replace(hour=17,minute=30) - CHK_TOLERANCE
		chk1830 = today_min.replace(hour=18,minute=30)
		chk2330 = today_min.replace(hour=23,minute=30)
		chk24 = today_max.replace(hour=0) 
		chkmax = today_max - CHK_TOLERANCE
		# chkls=[chk8,chk12,chk13,chk17,chk1730,chk24,chkmax]
		# print(pd.DataFrame(chkls))

		# plan today flag
		plan_flag=0
		# morring
		mask=(seldf['date'] > chk8 ) & (seldf['date'] <= chk12 )
		morring_df=seldf.loc[mask]
		# noon_rest
		mask=(seldf['date'] > chk12 ) & (seldf['date'] <= chk13 )
		noon_df=seldf.loc[mask]
		# afternoon
		mask=(seldf['date'] > chk13 ) & (seldf['date'] <= chk17 )
		afternoon_df=seldf.loc[mask]
		# dusk
		mask=(seldf['date'] > chk17 ) & (seldf['date'] <= chk1730 )
		dusk_df=seldf.loc[mask]
		# night
		mask=(seldf['date'] > chk1730 ) & (seldf['date'] <= chk24 )
		night_df=seldf.loc[mask]
		#midnight
		mask=(seldf['date'] > chk24 ) & (seldf['date'] <= chkmax )
		midnight_df=seldf.loc[mask]

		start=chk8
		end=chk12
		print(start,end)
		print(morring_df)
		if morring_df.empty:
			plan_flag=0
			morring=timedelta(0)
			schdf.value=VALUE_OF_PLAN_ED
		# elif not morring_df[morring_df.value==1].empty:
		else:
			plan_flag=1
			morring=timedelta(hours=8)

		print(plan_flag)
		if plan_flag != 0:
			print('test noon---------------------------------------')
			start=chk12
			end=chk13
			print(start,end)
			print(noon_df)
			if morring_df['value'].iloc[-1]==1:
				morring+=timedelta(hours=1)
				schdf.loc[schdf.raw_by=='noon','value']=VALUE_OF_PLAN_ST
			elif not noon_df[noon_df.value==1].empty:
				morring+=timedelta(hours=1)
				schdf.loc[schdf.raw_by=='noon','value']=VALUE_OF_PLAN_ST
			else:
				rest+=timedelta(hours=1)
				schdf.loc[schdf.raw_by=='noon','value']=VALUE_OF_PLAN_ED

			print(afternoon_df)
			print('test dusk---------------------------------------')
			start=chk17
			end=chk1730
			print(start,end)
			print(dusk_df)
			print(noon_df)
			dusk=timedelta(0)
			if afternoon_df['value'].iloc[-1]==1 and not dusk_df.empty:
				dusk=timedelta(minutes=30)
				schdf.loc[schdf.raw_by=='dusk','value']=(VALUE_OF_PLAN_ST,VALUE_OF_PLAN_ED)
			elif not dusk_df[dusk_df.value==1].empty:
				dusk=timedelta(minutes=30)
				schdf.loc[schdf.raw_by=='dusk','value']=(VALUE_OF_PLAN_ST,VALUE_OF_PLAN_ST)
			else:		
				rest+=timedelta(minutes=30)
				schdf.loc[schdf.raw_by=='dusk','value']=(VALUE_OF_PLAN_ED,VALUE_OF_PLAN_ED)

			print('test night---------------------------------------')
			start=chk1730
			end=chk24
			print(start,end)
			print(night_df)
			night=timedelta(0)
			# if night_df.empty:
			# 	# update night
			# 	schdf.loc[schdf.raw_by=='night','date']=(start + CHK_TOLERANCE, start + CHK_TOLERANCE )
			# 	schdf.loc[schdf.raw_by=='night','value']=(VALUE_OF_PLAN_ED,VALUE_OF_PLAN_ED)
			# elif not night_df[night_df.value==1].empty:
			if not night_df[night_df.value==1].empty:
				id1=min(night_df[night_df.value==1].index)
				id2=max(night_df[night_df.value==1].index)
				start=night_df.loc[id1,'date']
				end=seldf.shift(-1).loc[id2,'date']
				print(end)
				if not pd.isnull(end):
					if end > chk2330:
						end = chk24 - timedelta(1)
					elif end <= end.replace(minute=30,second=0,microsecond=0) + CHK_TOLERANCE:	
						end=end.replace(minute=30,second=0,microsecond=0)
					else:
						end=end.replace(minute=30,second=0,microsecond=0) + timedelta(hours=1)
				else: # end is null
					end = night_df.loc[id2,'date'].replace(minute=30,second=0,microsecond=0) + timedelta(hours=1)
				
				print(dusk_df.empty)
				# update dusk
				# if dusk_df.empty:
				# 	if afternoon_df['value'].iloc[-1]==1:
				# 		schdf.loc[schdf.raw_by=='dusk','value']=(VALUE_OF_PLAN_ST,VALUE_OF_PLAN_ST)
				
				# update night
				# start=start.replace(minute=30,second=0,microsecond=0)
				# if start == chk1830:
				# 	schdf.loc[schdf.raw_by=='night','value']=(VALUE_OF_PLAN_ED,VALUE_OF_PLAN_ED)	
				# if schdf.loc[schdf.raw_by=='dusk','value'].iloc[-1]==VALUE_OF_PLAN_ST:
				# 	night= end - (chk1730 + CHK_TOLERANCE) +dusk
				# else:
				# 	night=end-start+dusk
				start = chk1730 + CHK_TOLERANCE
				night=end-start+dusk #有晚班
				schdf.loc[schdf.raw_by=='night','date']=(start , end )
				schdf.loc[schdf.raw_by=='night','value']=(VALUE_OF_PLAN_ST,VALUE_OF_PLAN_ED)

			else:
				# update night
				morring += dusk #無晚班
				schdf.loc[schdf.raw_by=='night','date']=( start + CHK_TOLERANCE, start + CHK_TOLERANCE )
				schdf.loc[schdf.raw_by=='night','value']=(VALUE_OF_PLAN_ED,VALUE_OF_PLAN_ED)
			
			# midnight
			print('test midnight---------------------------------------')
			start=chk24
			end=chkmax
			print(start,end)
			print(midnight_df)
			midnight=timedelta(0)
			# if midnight_df.empty:
			# 	# update midnight
			# 	schdf.loc[schdf.raw_by=='midnight','date']=(start, start )
			# 	schdf.loc[schdf.raw_by=='midnight','value']=(VALUE_OF_PLAN_ED,VALUE_OF_PLAN_ED)
			# elif not midnight_df[midnight_df.value==1].empty:
			if not midnight_df[midnight_df.value==1].empty:
				id1=min(midnight_df[midnight_df.value==1].index)
				id2=max(midnight_df[midnight_df.value==1].index)
				start=midnight_df.loc[id1,'date'].replace(minute=0,second=0,microsecond=0)
				end=midnight_df.shift(-1).loc[id2,'date']
				print(end)
				if not pd.isnull(end):
					end = end.replace(minute=0,second=0,microsecond=0) + timedelta(hours=1)
				else: # end is null
					end = midnight_df.loc[id2,'date'].replace(minute=0,second=0,microsecond=0) + timedelta(hours=1)

				# update night
				if night_df['value'].iloc[-1]==1:
					schdf.loc[schdf.raw_by=='night','value']=(VALUE_OF_PLAN_ST,VALUE_OF_PLAN_ST)

				# update midnight
				midnight=end-start
				schdf.loc[schdf.raw_by=='midnight','date']=(start , end )
				schdf.loc[schdf.raw_by=='midnight','value']=(VALUE_OF_PLAN_ST,VALUE_OF_PLAN_ED)

			else:
				# update midnight_df
				schdf.loc[schdf.raw_by=='midnight','date']=( start , start )
				schdf.loc[schdf.raw_by=='midnight','value']=(VALUE_OF_PLAN_ED,VALUE_OF_PLAN_ED)
			
			print('rest time:',rest)
			print('morring:',morring)
			print('dusk:',dusk)
			print('night:',night)
			print('midnight:',midnight)
			print(schdf)
			quit()

		# update to database
# 		if len(schdf)>0:
# 			with self.__engine.connect() as cnn:
# 				for i in range(len(schdf)):
# 					sql = f"""
# UPDATE {table}
# SET date='{schdf.loc[i,'date']}',
# value={schdf.loc[i,'value']}
# WHERE id ={schdf.loc[i,'id']};
# """
# 					# print(sql)
# 					cnn.execute(sql)
		print('rest time:',rest)
		print(schdf)
		quit()
		print('read schedule_raw database %s cost % sec' %(name,(time.time()-allst)))
		return schdf,rest


	# 2022/03/25 改用機台標準速度計算
	# class DB_connect.read_capacity 設變
	# def read_capacity(self,idarr):
		# table = "work_order" #id = 製令編號
		# sql = "Select * from " + table + " where id in "+ str(idarr)
		# workdf=pd.read_sql_query(sql, self.__engine,index_col='id')
		# #print(workdf)
		# return workdf

	# 2022/03/25 改用機台標準速度計算
	# class DB_connect.read_capacity 設變
	def read_capacity(self,name):
		table = "standard_speed"
		sql = "Select standard_CAP from " + table + " where name='"+ name + "'"
		workdf=pd.read_sql_query(sql, self.__engine)
		#print(workdf)
		# return workdf
		cap=workdf.loc[0,'standard_CAP'].astype(int)
		return cap

		
	def read_from(self,name):
		t1=time.time()
		table = name

		# for test
		seldf = pd.read_csv("testdb.csv", parse_dates=['date'])
		return seldf

		#select ID between today
		sql = "Select id from " + table + " where date between '" + str(self.today_min) + "' and '" + str(self.today_max) + "'"
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
			seldf = pd.DataFrame({},columns=['id','date','value','parts','work_order_id'])
			return seldf

	def write_to_sql(self,df,table):
		sql = 'Select * from ' + table
		df.to_sql(table, self.__engine, if_exists='append', index=False)

	def reduce_data(self,name):
		t1=time.time()
		table = name
		
		#select ID to reduce
		sql = "Select * from " + table
		seldf = pd.read_sql_query(sql, self.__engine)
		seldf['value']=seldf['value'].fillna('NA')
		# print(seldf)
		if not seldf.empty:
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
		
		if len(seldf)>0:
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
		print('reduce database %s cost % sec' %(name,(time.time()-allst)))



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
			# conn.reduce_data(machine[i])
			
			seldf = conn.read_from(machine[i])
			seldf.sort_values(by=['date'],inplace=True)
			seldf.reset_index(drop=True,inplace=True)
			if seldf is not None:
				name=machine[i]
				schdf, rest = conn.read_schedule(name,seldf)
				
				print(schdf)
				oee = OEE_Class(seldf, machine[i],rest)
				#print(oee.workid)
				
				# 2022/03/25 cap設變
				# workdf = conn.read_capacity(oee.workid)
				workdf = conn.read_capacity(machine[i])
				#print(workdf)
				t1=time.time()
				oee.calc_standrad(workdf)
				t2=time.time()
				print('calc standard cost %f sec' %(t2-t1))
				#print(oee.standard_pcs)
				oeedf = oeedf.append(oee.calc_OEE())
				piedf = piedf.append(oee.piedf)
		print(oeedf)
		print(piedf)
		#print(oeedf.iloc[0,0:])
		# conn.write_to_sql(oeedf,'oee')
		# piedf=piedf.replace('NA',pd.NA)
		# conn.write_to_sql(piedf,'pie')

		alled = time.time()
		# 列印結果
		print("loop cost %f sec" % (alled - allst))  # 會自動做進位
		time.sleep(600)





#!/usr/bin/env python
#coding=utf8
import redis
import sys
import json
import unittest
import time
import copy

class TestTableMethods(unittest.TestCase):
	#index num：3
	data0 = {"fid":"","zone":"01","filename":"zone", "phone":[0, 1, 2, 3, 4, 5], "tags":[1, 2], 
					"time":20150101, "fsize":11111,"category" : "PIC", "fsha1" : "1e8de28172866b3907d0604a343d3817eed6294f",
					"ext" : {"longitude" : "0.0", "duration" : "0", "captureTimeStamp" : "1484898189000"},
					"ctime":1488969564, "utime": "1"}

	#index num：4
	data1 = {"fid":"00001","zone":"01","filename":"file1.jpg", "phone":["1","2","3", "4", "5"], "tags":[1, 2, 3], 
					"time":20150101, "fsize":11111,"category" : "PIC", "fsha1" : "1636bfdb903f85bb54431c8dd3b3d3721ec96817",
					"ext" : {"longitude" : "0.0", "duration" : "0","captureTimeStamp" : "1484898030000"},
					"ctime":1488969565, "utime": "2"}

	#index num：3
	data2 = {"fid":"00002","zone":"01","filename":"file2.jpg", "phone":[2, 3, 4, 5], "tags":[1, 2], 
					"time":20150101, "fsize":11111,"category" : "REC", "fsha1" : "0d3440f9db0e141a742ea0a598306f2944496016",
					"ext" : { "utcTimestamp" : "1480996895", "width" : "3264","captureTimeStamp" : "1485076858919"},
					"ctime":1488969566, "utime": "3"}

	#index num：3
	data3 = {"fid":"00003","zone":"02","filename":"file3.jpg", "phone":[3, 4, 5], "tags":["tiankong", "dadi3"], 
					"time":20150101, "fsize":11111,"category" : "REC",  "fsha1" : "6019f33fdadd1876e204d356df63ec589588632a",
					"ext" : { "utcTimestamp" : "1484825452", "width" : "1080", "addressJson" : "", "captureTimeStamp" : "1485138399899"},
					"ctime":1488969567, "utime": "4"}
	#index num：3
	data4 = {"fid":"00004","zone":"02","filename":"file4.jpg", "phone":[4, 5], "tags":["tiankong", "dadi4"],
					 "time":20150101, "fsize":11111,"category" : "REC", "fsha1" : "05afefa48f5b0436ce2e1a547512127df9e44d8e",
					 "ext" : {"utcTimestamp" : "1484898030", "width" : "1088", "captureTimeStamp" : "1485136093378"},
					 "ctime":1488969568, "utime": "5"}
    #index num：3
	data5 = {"fid":"00005","zone":"02","filename":"file5.jpg", "phone":[5], "tags":["tiankong", "dadi5"], 
					"time":20150101, "fsize":11111,"category" : "REC", "fsha1" : "4c542076e57d1834a3ffe5d8af3869b54f3fd667",
					"ext" : { "utcTimestamp" : "1485136093", "width" : "1080", "captureTimeStamp" : "1486707875000"},
					"ctime":1488969569, "utime": "6"}
	

	data01 = {"fid":"","zone":"01","filename":"zone", "phone":[0, 1, 2, 3, 4, 5], "tags":[1, 2], 
					"time":20150101, "fsize":11111,"category" : "PIC", "fsha1" : "fsha0",
					"longitude" : "0.0", "duration" : "0", "width":"1080", "captureTimeStamp" : "1484898189000"}

	#index num：4
	data11 = {"fid":"00001","zone":"01","filename":"file1.jpg", "phone":["1","2","3", "4", "5"], "tags":[1, 2, 3], 
					"time":20150101, "fsize":11111,"category" : "PIC", "fsha1" : "fsha1",
					"longitude" : "0.0", "duration" : "0", "width":"1088", "captureTimeStamp" : "1484898030000"}

	#index num：3
	data21 = {"fid":"00002","zone":"01","filename":"file2.jpg", "phone":[2, 3, 4, 5], "tags":[1, 2], 
					"time":20150101, "fsize":11111,"category" : "REC", "fsha1" : "fsha2",
					"utcTimestamp" : "1480996895", "width" : "1080","captureTimeStamp" : "1485076858919"}

	#index num：3
	data31 = {"fid":"00003","zone":"02","filename":"file3.jpg", "phone":[3, 4, 5], "tags":["tiankong", "dadi3"], 
					"time":20150101, "fsize":11111,"category" : "REC",  "fsha1" : "fsha3",
					"utcTimestamp" : "1484825452", "width" : "1080", "addressJson" : "", "captureTimeStamp" : "1485138399899"}
	#index num：3
	data41 = {"fid":"00004","zone":"02","filename":"file4.jpg", "phone":[4, 5], "tags":["tiankong", "dadi4"],
					 "time":20150101, "fsize":11111,"category" : "REC", "fsha1" : "fsha0",
					 "utcTimestamp" : "1484898030", "width" : "1088", "captureTimeStamp" : "1485136093378"}
    #index num：3
	data51 = {"fid":"00005","zone":"02","filename":"file5.jpg", "phone":[5], "tags":["tiankong", "dadi5"], 
					"time":20150101, "fsize":11111,"category" : "REC", "fsha1" : "fsha1",
					"utcTimestamp" : "1485136093", "width" : "1080", "captureTimeStamp" : "1486707875000"}

	master = redis.StrictRedis(host="127.0.0.1", port=8888)
	slave = redis.StrictRedis(host="127.0.0.1", port=8889)
	#master = redis.StrictRedis(host="10.58.89.236", port=38888)

	servers = []
	servers.append(master)#主
	servers.append(slave) #从

	def setUp(self):
		#每个测试函数都会进来一次
		pass

	def createUnionKeyTable(self, list):
		self.master.cltp_create("photo_user_1234567", 
			'''{"primary":[{"name":"zone"},{"name":"fid"}],
			"index":[{"name":"time"},{"name":"tags"}],"stat":["fsize"]}''')
		self.initServerData(list)

	def createUniqueTable(self, list):
		self.master.cltp_create("photo_user_1234567", 
			'''{"primary":[{"name":"fid"}],
			"index":[{"name":"time"},{"name":"tags"}, {"name":"fsha1","type":"unique"}],"stat":["fsize"]}''')
		self.initServerData(list)
	def createNormalTable(self, list):
		self.master.cltp_create("photo_user_1234567", 
			'''{"primary":[{"name":"fid"}],
			"index":[{"name":"time"},{"name":"tags"}],"stat":["fsize"]}''')
		self.initServerData(list)
	def initServerData(self, list):
		for i in range(len(list)):
			self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(list[i])),1)

	def copyData(self):
		list = []
		list.append(copy.deepcopy(self.data0))
		list.append(copy.deepcopy(self.data1))
		list.append(copy.deepcopy(self.data2))
		list.append(copy.deepcopy(self.data3))
		list.append(copy.deepcopy(self.data4))
		list.append(copy.deepcopy(self.data5))
		return list

	def copyUniqueData(self):
		list = []
		list.append(copy.deepcopy(self.data01))
		list.append(copy.deepcopy(self.data11))
		list.append(copy.deepcopy(self.data21))
		list.append(copy.deepcopy(self.data31))
		list.append(copy.deepcopy(self.data41))
		list.append(copy.deepcopy(self.data51))
		return list
	def dropTable(self):
		try:
			self.master.cltp_drop("photo_user_1234567")
		except Exception, exception:
			 self.assertEqual(str(exception),'Collection not found')

	def cmpGet(self, src, dst):
		if dst == None:
			self.assertEqual(src, dst)
		else:
			self.assertDictEqual(json.loads(src), dst)

	def cmpFind(self, src, dst):
		self.assertEqual(len(src), len(dst))

		for i in range(len(src)):
			idx = i
			self.cmpDict(json.loads(src[idx]), dst[idx])

	def cmpStat(self, src, dst):
		self.assertDictEqual(json.loads(src), dst)	

	def cmpDict(self, src_data, dst_data):     
		if isinstance(src_data,dict):  
			assert len(src_data) == len(dst_data), "dict len: '{}' != '{}'".format(src_data, dst_data)  
			for key in src_data:                  
				assert dst_data.has_key(key)      
				self.cmpDict(src_data[key], dst_data[key])      
		elif isinstance(src_data,list):
			assert len(src_data) == len(dst_data), "list len: '{}' != '{}'".format(src_data, dst_data)      
			for src_list, dst_list in zip(sorted(src_data), sorted(dst_data)):  
				self.cmpDict(src_list, dst_list)  
		else:  
			assert src_data == dst_data,"value '{}' != '{}'".format(src_data, dst_data) 

	def print_list(self, list):
		for i in range(len(list)):
			print list[i]
	def print_slave_no(self, id):
		if id == 0:
			print "master cmd test success"
		else:
			print "slave cmd test success"
	
        def sum_data_fsize(self, data):
            sum = 0
            for d in data:
                sum += d["fsize"]
            return sum
	def test_find_data(self):
		print "\n--------------------------------test_find_data start--------------------------------"
		self.dropTable()

		datas = self.copyData()
		self.createUnionKeyTable(datas)

		time.sleep(1)
		for server in self.servers:
			#主键查找
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"fid":"00001", "zone":"01"}'), [datas[1]])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"fid":"00003", "zone":"qianzhui"}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"fid":"00003", "zone":"02"}'), [datas[3]])

			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

			#索引查找 正序，倒序
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}'), [datas[0],datas[1],datas[2]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong", "zone":"02"}', '{"order":-1}'), [datas[5], datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
									[datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])


			#普通查找
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"fid":"00001"}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"zone":"01", "filename":"file2.jpg"}', '{"order":-1}'), [datas[2]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"zone":"01"}', '{"order":-1}'), [datas[2], datas[1], datas[0]])

			#find query index
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":[1,3]}}', '{"order":-1}'), [datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":[0,3]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":[0,4]}}', '{"order":-1}'), [])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,3]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,4]}}', '{"order":-1}'), [])


			self.cmpFind(server.cltp_find("photo_user_1234567", '{"phone":{"$in":["1","10"]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"phone":{"$in":[0,10]}}', '{"order":-1}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"phone":{"$in":[10,11]}}', '{"order":-1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"phone":{"$all":["1","2"]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"phone":{"$all":["10","11"]}}', '{"order":-1}'), [])
			
		#覆盖datas[5]
		data51 = {"fid":"00005","zone":"02","filename":"file51.jpg", "tags":["tiankong", "dadi51"], "time":20150101, "fsize":11111}
		datas[5]["filename"] = 'file51.jpg'
		datas[5]["tags"] = ["tiankong", "dadi51"]
		datas[5]["time"] = 20150101
		datas[5]["fsize"] = 11111
		self.assertEqual(self.master.cltp_upsert("photo_user_1234567", json.dumps(data51)),1)

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi51"}'), [datas[5]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi5"}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[4], datas[3]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})
	
		print "--------------------------------test_find_data success------------------------------\n"

	def test_update_primary_set(self):
		print "\n--------------------------------test_update_query_primary_set start-----------------"
		self.dropTable()

		datas = self.copyData()
		self.createUnionKeyTable(datas)

		#update normal field
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"", "zone":"01"}', 
							'{"$set":{"filename":"IMG_0820.JPG", "category":"MOV"}}'),1)
		datas[0]["filename"] = "IMG_0820.JPG"
		datas[0]["category"] = "MOV"

		#根据主键更新主键
		#要更新的主键存在
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00001", "zone":"01"}', 
							'{"$set":{"fid":"00005", "zone":"02", "category":"MOV", "filename":"file5.png"}}'),1)
		datas[1]["fid"] = "00005"
		datas[1]["zone"] = "02"
		datas[1]["category"] = "MOV"
		datas[1]["filename"] = "file5.png"

		#要更新的主键不存在
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00002", "zone":"01"}', 
							'{"$set":{"fid":"00006", "zone":"03", "category":"MOV", "filename":"file6.png"}}'),1)
		datas[2]["fid"] = "00006"
		datas[2]["zone"] = "03"
		datas[2]["category"] = "MOV"
		datas[2]["filename"] = "file6.png"

		time.sleep(1)
		for server in self.servers:
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"01"}'), datas[0])
			#fid更改了 找不到原先的
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00001", "zone":"01"}'), None)
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00005", "zone":"02"}'), datas[1])
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00002", "zone":"01"}'), None)
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00006", "zone":"03"}'), datas[2])
			
			#索引需要保持
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1}'), [datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1, 2]}}', '{"order":-1}'), [datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1, 2, 3]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
									[datas[2], datas[1], datas[4], datas[3], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[2], datas[1], datas[4], datas[3], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 55555, '_refcount': 5})
		

		#根据主键更新索引
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00004", "zone":"02"}', 
							'{"$set":{"tags":["tiankong4", "dadi"], "fsha1":"datas[4]"}}'),1)
		datas[4]["tags"] = ["tiankong4","dadi"]
		datas[4]["fsha1"] = "datas[4]"
		
		#根据主键更新索引与主键(覆盖其他主键) datas[3]->datas[2]
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00003", "zone":"02"}', 
							'{"$set":{"tags":["tiankong3", "dadi"], "fsha1":"datas[3]", "fid":"00006", "zone":"03"}}'), 1)
		datas[3]["tags"] = ["tiankong3", "dadi"]
		datas[3]["fsha1"] = "datas[3]"
		datas[3]["fid"] = "00006"
		datas[3]["zone"] = "03"

		#根据主键更新索引与主键（新建一个主键）
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"", "zone":"01"}', 
							'{"$set":{"tags":[4, 5, 6], "fid":"", "zone":"04"}}'), 1)
		datas[0]["zone"] = "04"
		datas[0]["tags"] = [4,5,6]

		time.sleep(1)
		for server in self.servers:
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"01"}'), None)
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"04"}'), datas[0])

			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00003", "zone":"02"}'), None)
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00006", "zone":"03"}'), datas[3])
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00004", "zone":"02"}'), datas[4])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong3"}', '{"order":-1}'), [datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong4"}', '{"order":-1}'), [datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi"}', '{"order":-1}'), [datas[3], datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"zone":"02"}', '{"order":-1}'), [datas[1], datas[4]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 44444, '_refcount': 4})

			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[0], datas[3], datas[1], datas[4]])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":[1,2]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":4}', '{"order":-1}'), [datas[0]])
		
		#update index 
		print "--------------------------------test_update_query_primary_set success---------------\n"

	def test_update_incrby(self):
		print "\n--------------------------------test_update_incrby start-------------------"
		self.dropTable()
		datas = self.copyData()
		self.createUnionKeyTable(datas)
		print "\n--------------------------------test_update_incrby success-------------------"
		#根据索引更新普通字段
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":1}', 
						'{"$incrby":{"fsize":11111}}', '{"multi":true}'),3)
		datas[0]["fsize"] += 11111
		datas[1]["fsize"] += 11111
		datas[2]["fsize"] += 11111

                fsize_sum = self.sum_data_fsize(datas)
		time.sleep(1)
		for server in self.servers:
                    self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"01"}'), datas[0])
                    self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00001", "zone":"01"}'), datas[1])
                    self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00002", "zone":"01"}'), datas[2])
                    self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': fsize_sum, '_refcount': len(datas)})

		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00003"}', 
                            '{"$incrby":{"fsize":-11111}, "test_incr": -100}', '{"multi":true}'),1)
		datas[3]["fsize"] -= 11111
		datas[3]["test_incr"] = -100
                fsize_sum = self.sum_data_fsize(datas)
		time.sleep(1)
		for server in self.servers:
                    self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00003", "zone":"02"}'), datas[3])
                    self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': fsize_sum, '_refcount': len(datas)})

	def test_update_index_set(self):
		print "\n--------------------------------test_update_query_index_set start-------------------"
		self.dropTable()
		datas = self.copyData()
		self.createUnionKeyTable(datas)

		#根据索引更新普通字段
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":1}', 
											'{"$set":{"filename":"mov.png"}}', '{"multi":true}'),3)
		datas[0]["filename"] = "mov.png"									
		datas[1]["filename"] = "mov.png"									
		datas[2]["filename"] = "mov.png"									

		#根据索引更新主键 不能更新所有主键
		try:
			self.master.cltp_update("photo_user_1234567", '{"tags":1}', '{"$set":{"fid":"", "zone":"qianzhui"}}')
 		except Exception, exception:
			 self.assertEqual(str(exception),'Primary update faild')

		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":1}', 
											'{"$set":{"zone":"03", "category":"MOV"}}', '{"multi":true}'), 3)
		
		datas[0]["zone"] = "03"
		datas[1]["zone"] = "03"
		datas[2]["zone"] = "03"
		datas[0]["category"] = "MOV"				
		datas[1]["category"] = "MOV"
		datas[2]["category"] = "MOV"

		time.sleep(1)
		for server in self.servers:
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"01"}'), None)
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00001", "zone":"01"}'), None)
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00002", "zone":"01"}'), None)

			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"03"}'), datas[0])
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00001", "zone":"03"}'), datas[1])
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"00002", "zone":"03"}'), datas[2])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"category":"MOV"}', '{"order":-1}'), [datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2]}}', '{"order":-1}'), [datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
							[datas[2], datas[1], datas[0], datas[5], datas[4], datas[3]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

		#根据索引更新索引
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":1}', 
											'{"$set":{"tags":[3,4,5], "zone":"04"}}', '{"multi":true}'), 3)
		datas[0]["zone"] = "04"
		datas[1]["zone"] = "04"
		datas[2]["zone"] = "04"
		datas[0]["tags"] = [3,4,5]
		datas[1]["tags"] = [3,4,5]
		datas[2]["tags"] = [3,4,5]

		time.sleep(1)
		for server in self.servers:
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"03"}'), None)
			self.cmpGet(server.cltp_get("photo_user_1234567", '{"fid":"", "zone":"04"}'), datas[0])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":2}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":3}', '{"order":-1}'), [datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[3,4,5]}}', '{"order":-1}'), [datas[2], datas[1], datas[0]])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
							[datas[2], datas[1], datas[0], datas[5],datas[4],datas[3]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

		print "--------------------------------test_update_query_index_set success-----------------\n"

	def test_update_unset(self):
		print "\n--------------------------------test_update_unset start-----------------------------"
		self.dropTable()
		datas = self.copyData()
		self.createUnionKeyTable(datas)

		#根据主键删除普通字段
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"", "zone":"01"}', 
											'{"$unset":{"category":1, "fsha1":1}}'), 1)
		
		del datas[0]["category"]
		del datas[0]["fsha1"]
		
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"category":"PIC"}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"filename":"zone"}'), [datas[0]])
			
		
		#根据主键删除索引字段
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00003", "zone":"02"}', 
											'{"$unset":{"tags":1}}'), 1)
		del datas[3]["tags"]
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi3"}', '{"order":-1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
							[datas[5], datas[4], datas[3], datas[2], datas[1],datas[0]])
			
		
		#根据索引删除普通字段与设置索引
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$unset":{"filename":1}, "$set":{"time":20150102}}', '{"multi":true}'), 2)
		del datas[4]["filename"]
		del datas[5]["filename"]
		datas[4]["time"] = 20150102
		datas[5]["time"] = 20150102

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi4"}', '{"order":-1}'), [datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi5"}', '{"order":-1}'), [datas[5]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), [datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150102}', '{"order":-1}'), [datas[5], datas[4]])
		
		
		print "--------------------------------test_update_unset success---------------------------\n"
		
	def test_update_pull(self):
		print "\n--------------------------------test_update_pull start------------------------------"
		self.dropTable()

		datas = self.copyData()
		self.createUnionKeyTable(datas)
	
		#根据主键更新普通数组 $in
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"", "zone":"01"}', 
											'{"$pull":{"phone":{"$in":[0,1,10]}}}'), 1)

		datas[0]["phone"].remove(0)
		datas[0]["phone"].remove(1)

		#根据主键删除index数组 $all
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00001", "zone":"01"}', 
											'{"$pull":{"tags":{"$all":[1,2]}}}'), 1)
		datas[1]["tags"].remove(2)
		datas[1]["tags"].remove(1)


		#根据主键删除index数组 $in
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00002", "zone":"01"}', 
											'{"$pull":{"tags":{"$in":[1,10]}}}'), 1)								
		datas[2]["tags"].remove(1)
	
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":2}', '{"order":-1}'), [datas[2], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1, 2]}}', '{"order":-1}'), [datas[0]])

		#根据索引删除普通数组 $in
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$pull":{"phone":{"$in":[5, 6]}}}', '{"multi":true}'), 3)
		datas[3]["phone"].remove(5)
		datas[4]["phone"].remove(5)
		datas[5]["phone"].remove(5)

		#根据索引删除普通数组 $in 应该是返回0才较对
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
								'{"$pull":{"phone":{"$all":[4, 5]}}}', '{"multi":true}'), 3)
		
		
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":["tiankong", "dadi4"]}}', '{"order":-1}'), [datas[4]])

		#根据索引删除索引数组 $all 应该返回1才对
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$pull":{"tags":{"$all":["tiankong", "dadi4"]}}}', '{"multi":true}'), 3)
		datas[4]["tags"].remove("tiankong")
		datas[4]["tags"].remove("dadi4")

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":3}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi5"}', '{"order":-1}'), [datas[5]])
		#根据索引删除索引数组 $in
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"time":20150101}', 
											'{"$pull":{"tags":{"$in":["tiankong", 3, "dadi5"]}}}', '{"multi":true, "cursor":0, "limit":1}'),1)
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"time":20150101}', 
											'{"$pull":{"tags":{"$in":["tiankong", 3, "dadi5"]}}}', '{"multi":true}'), 6)
		datas[1]["tags"].remove(3)
		datas[3]["tags"].remove("tiankong")
		datas[5]["tags"].remove("tiankong")
		datas[5]["tags"].remove("dadi5")
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"3"}', '{"order":-1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"datas[5]"}', '{"order":-1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1, "limit":2, "cursor":1}'), [datas[4], datas[3]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})
	
		# #根据索引删除索引数组 $in
		print "--------------------------------test_update_pull success----------------------------"
	
	def test_update_pullall(self):
		print "\n--------------------------------test_update_pullAll start---------------------------"
		self.dropTable()
		
		datas = self.copyData()
		self.createUnionKeyTable(datas)
	
		#根据主键删除普通数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"", "zone":"01"}', 
											'{"$pullAll":{"phone":[1,2,3]}}', '{"multi":true}'), 1)
		datas[0]["phone"].remove(1)
		datas[0]["phone"].remove(2)
		datas[0]["phone"].remove(3)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1}'), [datas[2], datas[1], datas[0]])

		#根据主键删除普通数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00001", "zone":"01"}', 
											'{"$pullAll":{"tags":[1,2,3]}}', '{"multi":true}'), 1)
		datas[1]["tags"].remove(1)
		datas[1]["tags"].remove(2)
		datas[1]["tags"].remove(3)							
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1}'), [datas[2], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":2}', '{"order":-1}'), [datas[2], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2]}}', '{"order":-1}'), [datas[2], datas[0]])


		#根据索引删除普通数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$pullAll":{"phone":[4, 5]}}', '{"multi":true}'), 3)		
		datas[3]["phone"].remove(4)
		datas[3]["phone"].remove(5)
		datas[4]["phone"].remove(4)
		datas[4]["phone"].remove(5)
		datas[5]["phone"].remove(5)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[4], datas[3]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

		#根据索引删除索引数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$pullAll":{"tags":["tiankong"]}}', '{"multi":true}'), 3)	
		datas[3]["tags"].remove("tiankong")
		datas[4]["tags"].remove("tiankong")
		datas[5]["tags"].remove("tiankong")

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi3"}', '{"order":-1}'), [datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi4"}', '{"order":-1}'), [datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"dadi5"}', '{"order":-1}'), [datas[5]])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
								[datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

		print "--------------------------------test_update_pullAll success-------------------------\n"
	
	def test_update_push(self):
		print "\n--------------------------------test_update_push start------------------------------"
		self.dropTable()
	
		datas = self.copyData()
		self.createUnionKeyTable(datas)
		#根据主键添加普通数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"", "zone":"01"}', 
											'{"$push":{"phone":[1,2,3]}}', '{"multi":true}'), 1)
		datas[0]["phone"].append(1)
		datas[0]["phone"].append(2)
		datas[0]["phone"].append(3)
	
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"fid":""}', '{"order":-1}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'),
									 [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})
				
		#根据主键添加索引数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00001", "zone":"01"}', 
											'{"$push":{"tags":[3, 4, 5]}}', '{"multi":true}'), 1)
		datas[1]["tags"].append(3)
		datas[1]["tags"].append(4)
		datas[1]["tags"].append(5)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2, 3, 4, 5]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[3,4, 5]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
							[datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

		#根据索引添加普通数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$push":{"phone":[3,4,5]}}', '{"multi":true}'), 3)
		datas[3]["phone"].append(3)
		datas[3]["phone"].append(4)
		datas[3]["phone"].append(5)
		datas[4]["phone"].append(3)
		datas[4]["phone"].append(4)
		datas[4]["phone"].append(5)
		datas[5]["phone"].append(3)
		datas[5]["phone"].append(4)
		datas[5]["phone"].append(5)
		
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})
	
		#根据索引添加索引数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$push":{"tags":[3,4,5]}}', '{"multi":true}'), 3)
		datas[3]["tags"].append(3)
		datas[3]["tags"].append(4)
		datas[3]["tags"].append(5)
		datas[4]["tags"].append(3)
		datas[4]["tags"].append(4)
		datas[4]["tags"].append(5)
		datas[5]["tags"].append(3)
		datas[5]["tags"].append(4)
		datas[5]["tags"].append(5)		

		time.sleep(3)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":3}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":4}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":5}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2,3,4,5]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":["tiankong", 3]}}', '{"order":-1}'),[datas[5], datas[4], datas[3], datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'),
										 [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

		print "--------------------------------test_update_push success----------------------------\n"
	def test_update_addToSet(self):
		print "\n--------------------------------test_update_addToSet start--------------------------"
		self.dropTable()
		datas = self.copyData()
		self.createUnionKeyTable(datas)
		#根据主键添加普通数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"", "zone":"01"}', 
											'{"$addToSet":{"phone":[1,2,3]}}', '{"multi":true}'), 1)
	
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"fid":""}', '{"order":-1}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'),
									 [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})
				
		#根据主键添加索引数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00001", "zone":"01"}', 
											'{"$addToSet":{"tags":[3, 4, 5]}}', '{"multi":true}'), 1)
	
		datas[1]["tags"].append(4)
		datas[1]["tags"].append(5)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2, 3, 4, 5]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[3,4, 5]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
							[datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})

		#根据索引添加普通数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$addToSet":{"phone":[3,4,5]}}', '{"multi":true}'), 3)

	
		datas[4]["phone"].append(3)
		datas[5]["phone"].append(3)
		datas[5]["phone"].append(4)
	
		
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})
	
		#根据索引添加索引数组
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":"tiankong"}', 
											'{"$addToSet":{"tags":["tiankong",4,5]}}', '{"multi":true}'), 3)
		datas[3]["tags"].append(4)
		datas[3]["tags"].append(5)
		datas[4]["tags"].append(4)
		datas[4]["tags"].append(5)
		datas[5]["tags"].append(4)
		datas[5]["tags"].append(5)		

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":3}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":4}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":5}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2,3,4,5]}}', '{"order":-1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong"}', '{"order":-1}'), [datas[5], datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":["tiankong", 3]}}', '{"order":-1}'),
												 [datas[5], datas[4], datas[3], datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":["tiankong", 4]}}', '{"order":-1}'),
												 [datas[5], datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'),
										 [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 66666, '_refcount': 6})
		print "--------------------------------test_update_addToSet success------------------------\n"

	def test_clear(self):
		print "\n--------------------------------test_clear start------------------------------------"
		self.dropTable()
		datas = self.copyData()
		self.createUniqueTable(datas)

		
		self.assertEqual(self.master.cltp_clear("photo_user_1234567"),1)
		time.sleep(1)
		for server in self.servers:
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 0, '_refcount': 0})

		print "--------------------------------test_clear success----------------------------------\n"

	def test_upsert(self):
		print "\n--------------------------------test_upsert start-----------------------------------"
		self.dropTable()
		datas = self.copyUniqueData()
		self.createUniqueTable(datas[:4])
		time.sleep(1)
		try:
			self.assertEqual(self.master.cltp_upsert("photo_user_1234567", json.dumps(datas[4])),1)
			self.assertEqual(1,0)#到这出错
		except Exception, exception:
			self.assertEqual(str(exception),'Unique index conflict')

		datas[4]['fsha1'] = 'fsha4'
		self.assertEqual(self.master.cltp_upsert("photo_user_1234567", json.dumps(datas[4])),1)
		self.assertEqual(self.master.cltp_upsert("photo_user_1234567", json.dumps(datas[4])),0)
	
		time.sleep(1)
		for server in self.servers:
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 55555, '_refcount': 5})
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong", "zone":"02"}', '{"order":-1}'), [datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
									[datas[4], datas[3], datas[2], datas[1], datas[0]])

		print "--------------------------------test_upsert success---------------------------------\n"

	def test_uinque_index(self):
		print "\n--------------------------------test_unique_index start-----------------------------"
		self.dropTable()

		datas = self.copyUniqueData()
		self.createUniqueTable(datas[:4])

		#唯一索引冲突，不能写入
		try:
			self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[4])),1)
			self.assertEqual(1,0)#到这出错
		except Exception, exception:
			self.assertEqual(str(exception),'Unique index conflict')

		datas[4]['fsha1'] = 'fsha4'
		self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[4])),1)
		self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[4])),0)

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong", "zone":"02"}', '{"order":-1}'), [datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
									[datas[4], datas[3], datas[2], datas[1], datas[0]])
		
		try:
			self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[5])),1)
			self.assertEqual(1,0)#到这出错
		except Exception, exception:
			self.assertEqual(str(exception),'Unique index conflict')
		
		datas[5]['fsha1'] = 'fsha5'
		self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[5])),1)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong", "zone":"02"}', '{"order":-1}'), [datas[5], datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
									[datas[5], datas[4], datas[3], datas[2], datas[1], datas[0]])

		print "--------------------------------test_unique_index success---------------------------\n"

	def test_update_unique_index(self):
		print "\n--------------------------------test_update_unique_index start----------------------"
		self.dropTable()

		datas = self.copyUniqueData()
		self.createUniqueTable(datas[:4])

		#唯一索引冲突，不能写入
		try:
			self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[4])),1)
			self.assertEqual(1,0)#到这出错
		except Exception, exception:
			self.assertEqual(str(exception),'Unique index conflict')

		#删除了唯一索引字段就能写入了
		del datas[0]['fsha1']
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":""}', '{"$unset":{"fsha1":1}}'), 1)
		self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[4])),1)

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), 
									[datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
									[datas[4], datas[3], datas[2], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2]}}', '{"order":-1}'), 
									[datas[2], datas[1], datas[0]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 55555, '_refcount': 5})

		try:
			#更新主键并更新唯一索引时，可以更新到原主键的唯一索引，也可以更新到新主键的唯一索引
			self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00004"}', '{"$set":{"fid":"00005","fsha1":"fsha2"}}'), 1)
			self.assertEqual(1,0)#到这出错
		except Exception, exception:
			self.assertEqual(str(exception),'Unique index conflict')
		
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":"00004"}', '{"$set":{"fid":"00002","fsha1":"fsha2"}}'), 1)
		datas[4]['fsha1'] = 'fsha2'
		datas[4]['fid'] = '00002'

		time.sleep(1)       
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), [datas[3], datas[4], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":-1}'), 
									[datas[3], datas[4], datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$all":[1,2]}}', '{"order":-1}'), 
									[datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":["tiankong"]}}', '{"order":-1}'), 
									[datas[3], datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":{"$in":["dadi4"]}}', '{"order":-1}'), 
									[datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"fsha1":"fsha2"}', '{"order":-1}'), 
									[datas[4]])
			self.cmpStat(server.cltp_stat("photo_user_1234567"), {'fsize': 44444, '_refcount': 4})


		try:
			self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":""}', '{"$set":{"fsha1":"fsha2"}}'), 1)
			self.assertEqual(1,0)#到这出错
		except Exception, exception:
			self.assertEqual(str(exception),'Unique index conflict')
		
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":""}', '{"$set":{"fsha1":"fsha0"}}'), 1)
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"fid":""}', '{"$set":{"fsha1":"fsha0"}}'), 1)

		try:
			self.assertEqual(self.master.cltp_update("photo_user_1234567", 
						'{"tags":"tiankong"}', '{"$set":{"fsha1":"fsha3"}}','{"multi":true}'), 0)
			self.assertEqual(1,0)#到这出错
		except Exception, exception:
			self.assertEqual(str(exception),'Unique index conflict')

		print "--------------------------------test_update_unique_index success--------------------\n"

	def test_start_end(self):
		print "\n--------------------------------test_start_end start----------------------"
		self.dropTable()
		datas = self.copyUniqueData()
		self.createNormalTable(datas)

		time.sleep(1)       
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":1, "end":"00004"}'), datas[:5])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":1, "start":"","end":"00004"}'), datas[1:5])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":1, "start":"0000","end":"00004"}'), datas[1:5])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":1, "start":"00001","end":"00004"}'), datas[2:5])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":1, "start":"00001","end":""}'), datas[2:])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":1, "start":"00001"}'), datas[2:])


			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1, "end":"00004"}'), [datas[5], datas[4]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1, "start":"00005", "end":"00003"}'), [datas[4], datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1, "start":"00005"}'), datas[4::-1])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1}'), datas[::-1])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1, "end":""}'), datas[::-1])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":-1, "start":"", "end":""}'), datas[::-1])

		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1,"start":"", "end":""}'), datas[1:3])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1,"start":"", "end":"00001"}'), datas[1:2])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1, "end":""}'), datas[:3])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1}'), datas[:3])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1, "start":"0000", "limit":1}'), datas[1:2])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1, "start":"0000", "cursor":1}'), datas[2:3])

			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1,"start":"", "end":""}'), [datas[2], datas[1],datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1,"start":"00002", "end":""}'), [datas[1],datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1,"start":"00002", "end":"00001"}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1,"start":"00001", "end":"00001"}'), [])


		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":1}', '{"$pull":{"tags":2}}', 
									'{"order":1,"start":"", "end":"", "multi":true}'), 2) 
		datas[1]["tags"].remove(2)
		datas[2]["tags"].remove(2)

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1, "end":""}'), datas[:3])

		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":1}', '{"$pull":{"tags":1}}',
							 '{"order":1,"start":"", "end":"", "multi":true, "cursor":1}'), 1) 
		datas[2]["tags"].remove(1)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":2}', '{"order":1, "end":""}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":1, "end":""}'), datas[:2])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1, "end":""}'), [datas[1],datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong", "width":"1080"}',
									'{"order":-1,"start":"","end":""}'), [datas[5],datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong","width":"1080"}', 
								'{"order":-1, "start":"","end":"","cursor":1}'), [datas[3]])

		self.assertEqual(self.master.cltp_remove("photo_user_1234567", '{"tags":"tiankong","width":"1080"}',
						 '{"order":-1,"start":"", "end":"", "multi":true, "cursor":1}'), 1) 
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong","width":"1080"}', 
								'{"order":-1, "start":"","end":""}'), [datas[5]])


		self.assertEqual(self.master.cltp_insert("photo_user_1234567", json.dumps(datas[3])),1)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong","width":"1080"}', 
								'{"order":-1, "start":"","end":""}'), [datas[5],datas[3]])

		self.assertEqual(self.master.cltp_remove("photo_user_1234567", '{"tags":"tiankong","width":"1080"}',
						 '{"order":-1,"start":"", "end":"", "multi":true, "cursor":2}'), 0) 

		self.assertEqual(self.master.cltp_remove("photo_user_1234567", '{"tags":"tiankong","width":"1080"}',
						 '{"order":-1,"start":"00005", "end":"", "multi":true}'), 1) 

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong","width":"1080"}', 
								'{"order":-1, "start":"","end":""}'), [datas[5]])

		print "--------------------------------test_start_end end------------------------\n"
	
	def test_cursor(self):
		print "\n--------------------------------test_cursor start----------------------"
		self.dropTable()
		datas = self.copyUniqueData()
		self.createNormalTable(datas)

		time.sleep(1)       
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{}', '{"order":1, "cursor":2}'), datas[2:])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1,"width":"1080"}', '{"order":1, "cursor":1}'), [datas[2]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1,"width":"1080"}', '{"order":-1, "cursor":1}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1, "cursor":1}'), [datas[1], datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1, "cursor":1,"limit":1}'), [datas[1]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', '{"order":-1, "cursor":2,"limit":1}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":"tiankong","width":"1080"}', 
											'{"order":-1, "cursor":1,"limit":1}'), [datas[3]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"width":"1080"}', 
											'{"order":-1, "cursor":2,"limit":3}'), [datas[2],datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"width":"1080"}', 
											'{"order":-1, "limit":3}'), [datas[5], datas[3], datas[2]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"width":"1080"}', 
											'{"order":-1,"cursor":1, "limit":3}'), [datas[3], datas[2],datas[0]])
		
		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"tags":1,"width":"1080"}', '{"$pull":{"tags":1}}',
							 '{"order":1, "cursor":1}'), 1) 
		datas[2]["tags"].remove(1)

		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', 
						'{"order":-1, "cursor":1}'), [datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"time":20150101}', '{"order":1}'), datas)


		self.assertEqual(self.master.cltp_update("photo_user_1234567", '{"width":"1080"}', '{"$addToSet":{"tags":[1]}}',
							 '{"order":-1, "cursor":1,"limit":2,"multi":true}'), 2) 
		datas[3]["tags"].append(1)
		datas[2]["tags"].append(1)
		time.sleep(1)
		for server in self.servers:
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1}', 
						'{"order":-1, "cursor":1}'), [datas[2],datas[1],datas[0]])
			self.cmpFind(server.cltp_find("photo_user_1234567", '{"tags":1,"width":"1080"}', 
						'{"order":-1, "cursor":1}'), [datas[2],datas[0]])
		
		print "--------------------------------test_cursor end------------------------\n"
	# def test_94_recreate(self):
	# 	print "\n--------------------------------test_recreate start-----------------------------------"
	# 	self.master.cltp_drop("photo_user_1234567")
	# 	datas = self.copyUniqueData()
	# 	time.sleep(1)
		

	# 	print "--------------------------------test_recreate success---------------------------------\n"

if __name__ == '__main__':
    unittest.main()




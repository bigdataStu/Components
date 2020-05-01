# Hive 实操演练

本案例来自知乎 [Hive 与 Hadoop 大数据分析专栏](]https://zhuanlan.zhihu.com/c_1226144823318958080)，关于 hadoop 与 hive 等环境的搭建、数据准备等工作，请参考上述专栏，以下篇幅主要集中于实战演练案例。

[Hive 官网](https://hive.apache.org/) 是最权威的教材。

## 实战基础

创建数据库

```sql
create (darabase/schema)[if not exists] database_name
[comment database_comment]
[location hdfs_path]
[with dbproperties (property_name=property_value,...)];
```

案例

create database if not exists test_demo;

查看数据库 show databases;

在 Hive 中查看本地文件系统

hive> ! ls file path;

hive> dfs -ls -R(表示递归) file path;

查看数据库信息

desc database extended database_name;

```shell
hive (default)> desc database extended test;
OK
db_name	comment	location	owner_name	owner_type	parameters
test		hdfs://node100:9000/user/hive/warehouse/test.db	hadoop	USER	
Time taken: 0.008 seconds, Fetched: 1 row(s)
```

删除数据库

drop database if exists test;

drop database if exists test cascade;

修改数据库

alter (database/schema) database_name set location hdfs_path;

数据表相关

```sql
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.] table_name

[(col_name data_type [COMMENT col_comment], ...)]
[COMMENT table_comment]
[ROW FORMAT row_format]
[STORED AS file_format]
```

```sql
CREATE TABLE IF NOT EXISTS employee 
(eid int, name String,
salary String, destination String)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
```

```sql
create [external] table if not exists 表名 (列名 数据类型 [comment 本列注释]，...)
[comment 表注释]
[partitioned by (列名 数据类型 [comment 本列注释],...)]
[clustered by(列名,列名,...)]
[sorted by (列名 [asc|desc],...) into num_buckets buckets]
[row format row_format]
[stored as file_format]
[location hdfs_path]
[tblproperties (property_name=property_value,...)]
[as select_statement]
```

external:内部表

partitioned by:分区表

clustered by:分桶表

sorted by:
**row format delimited [fields terminated by char]
[collection items terminated by char]
[map keys terminated by char]
[line terminated by char]**

stored as 文件类型(sequencefile 二进制文件 textfile 文本文件 rcfile 列式存储格式)

查看数据表结构(use kaikeba;)

```
hive (kaikeba)> desc formatted dim_month;
OK
col_name	data_type	comment
# col_name            	data_type           	comment             
month               	string              	                    
	 	 
# Detailed Table Information	 	 
Database:           	kaikeba             	 
OwnerType:          	USER                	 
Owner:              	hadoop              	 
CreateTime:         	Thu Jan 02 17:43:23 CST 2020	 
LastAccessTime:     	UNKNOWN             	 
Retention:          	0                   	 
Location:           	hdfs://node100:9000/user/hive/warehouse/kaikeba.db/dim_month	 
Table Type:         	MANAGED_TABLE       	 
Table Parameters:	 	 
	bucketing_version   	2                   
	numFiles            	1                   
	numRows             	0                   
	rawDataSize         	0                   
	totalSize           	216                 
	transient_lastDdlTime	1577958211          
	 	 
# Storage Information	 	 
SerDe Library:      	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	 
InputFormat:        	org.apache.hadoop.mapred.TextInputFormat	 
OutputFormat:       	org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat	 
Compressed:         	No                  	 
Num Buckets:        	-1                  	 
Bucket Columns:     	[]                  	 
Sort Columns:       	[]                  	 
Storage Desc Params:	 	 
	field.delim         	\t                  
	serialization.format	\t                  
Time taken: 0.474 seconds, Fetched: 31 row(s)

```

分区表

```
create table test (name string,age int)
partitioned by (country string)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;
```

insert into table test partition(country="china") values("test1",1);

insert into table test partition(country="china") values("test2",2);

insert into table test partition(country="japan") values("test3",3);

```
hive (kaikeba)> select * from test where country = "china";
OK
test.name	test.age	test.country
test1	1	china
test2	2	china
Time taken: 0.077 seconds, Fetched: 2 row(s)
```

查看创建表 test 的语句

```
hive (kaikeba)> show create table test;
OK
createtab_stmt
CREATE TABLE `test`(
  `name` string, 
  `age` int)
PARTITIONED BY ( 
  `country` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/test'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1588303393')
```

删除分区

alter table test drop partition(country="china");

显示分区

```
hive (kaikeba)> show partitions test;
OK
partition
country=china
country=japan
Time taken: 0.036 seconds, Fetched: 2 row(s)
```

数据的导入与导出

本地 -> 表

load data local inpath 'local path' overwrite into table table_name [partition(col=v,...)];

HDFS -> 表

load data inpath 'hdfs path' into table table_name [partition(col=v,...)];

Hive 表 -> 本地

insert overwrite local repository 'local path' 查询语句;

Hive 表 -> HDFS

insert overwrite directory 'local path' 查询语句;

## 实战数据准备

数据获取方式访问知乎 [Hive 与 Hadoop 大数据分析专栏](]https://zhuanlan.zhihu.com/c_1226144823318958080) [第六章 Hive 基本查询分析](https://zhuanlan.zhihu.com/p/127209857) 

使用的数据库

```shell
hive (kaikeba)> show tables;
OK
tab_name
dim_month
stu_mess
stu_mess_part
stu_messages
test
trade_2017
trade_2018
trade_2019
user_goods_category
user_info
user_list_1
user_list_2
user_list_3
user_refund
user_trade
user_trade_bak
```

dim_month 表
```shell
hive (kaikeba)> desc dim_month;
OK
col_name	data_type	comment
month               	string  
```

```
hive (kaikeba)> show create table dim_month;
OK
createtab_stmt
CREATE TABLE `dim_month`(
  `month` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/dim_month'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577958211')
```

stu_mess 表

```
hive (kaikeba)> desc stu_mess;
OK
col_name	data_type	comment
stu_id              	int                 	                    
sex                 	string              	                    
age                 	int                 	                    
address             	string              	                    
tel_num             	string              	                    
ts                  	date                	                    
y                   	int                 	                    
m                   	int  
```

```
hive (kaikeba)> show create table stu_mess;
OK
createtab_stmt
CREATE TABLE `stu_mess`(
  `stu_id` int, 
  `sex` string, 
  `age` int, 
  `address` string, 
  `tel_num` string, 
  `ts` date, 
  `y` int, 
  `m` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/stu_mess'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1578916483')
```

stu_mess_part 表

```
hive (kaikeba)> desc stu_mess_part;
OK
col_name	data_type	comment
stu_id              	int                 	                    
sex                 	string              	                    
age                 	int                 	                    
address             	string              	                    
tel_num             	string              	                    
ts                  	date                	                    
y                   	int                 	                    
m                   	int                 	                    
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
y                   	int                 	                    
m                   	int                 	                    
Time taken: 0.336 seconds, Fetched: 13 row(s)
```

```
hive (kaikeba)> show create table stu_mess_part;
OK
createtab_stmt
CREATE TABLE `stu_mess_part`(
  `stu_id` int, 
  `sex` string, 
  `age` int, 
  `address` string, 
  `tel_num` string, 
  `ts` date)
PARTITIONED BY ( 
  `y` int, 
  `m` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/stu_mess_part'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1578916508')
Time taken: 0.028 seconds, Fetched: 24 row(s)
```

stu_messages 表

```
hive (kaikeba)> desc stu_messages;
OK
col_name	data_type	comment
stu_id              	int                 	                    
sex                 	string              	                    
age                 	int                 	                    
address             	string              	                    
tel_num             	string              	                    
ts                  	date                	                    
Time taken: 0.024 seconds, Fetched: 6 row(s)
```

```
hive (kaikeba)> show create table stu_messages;
OK
createtab_stmt
CREATE TABLE `stu_messages`(
  `stu_id` int, 
  `sex` string, 
  `age` int, 
  `address` string, 
  `tel_num` string, 
  `ts` date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/stu_messages'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1578916423')
Time taken: 0.022 seconds, Fetched: 21 row(s)
```

trade_2017 表

```
hive (kaikeba)> desc trade_2017;
OK
col_name	data_type	comment
user_name           	string              	                    
amount              	double              	                    
trade_time          	string              	                    
Time taken: 0.025 seconds, Fetched: 3 row(s)
```

```
hive (kaikeba)> show create table  trade_2017;
OK
createtab_stmt
CREATE TABLE `trade_2017`(
  `user_name` string, 
  `amount` double, 
  `trade_time` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/trade_2017'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577957939')
Time taken: 0.02 seconds, Fetched: 18 row(s)
```

trade_2018 表

```
hive (kaikeba)> desc trade_2018;
OK
col_name	data_type	comment
user_name           	string              	                    
amount              	double              	                    
trade_time          	string              	                    
Time taken: 0.023 seconds, Fetched: 3 row(s)
```

```
hive (kaikeba)> show create table  trade_2018;
OK
createtab_stmt
CREATE TABLE `trade_2018`(
  `user_name` string, 
  `amount` double, 
  `trade_time` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/trade_2018'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577957956')
Time taken: 0.024 seconds, Fetched: 18 row(s)
```

trade_2019 表

```
hive (kaikeba)> desc trade_2019;
OK
col_name	data_type	comment
user_name           	string              	                    
amount              	double              	                    
trade_time          	string              	                    
Time taken: 0.02 seconds, Fetched: 3 row(s)
```

```
hive (kaikeba)> show create table  trade_2019;
OK
createtab_stmt
CREATE TABLE `trade_2019`(
  `user_name` string, 
  `amount` double, 
  `trade_time` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/trade_2019'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577957974')
Time taken: 0.02 seconds, Fetched: 18 row(s)
```

user_goods_category 表

```
hive (kaikeba)> desc user_goods_category;
OK
col_name	data_type	comment
user_name           	string              	                    
category_detail     	string              	                    
Time taken: 0.025 seconds, Fetched: 2 row(s)
```

```
hive (kaikeba)> show create table user_goods_category;
OK
createtab_stmt
CREATE TABLE `user_goods_category`(
  `user_name` string, 
  `category_detail` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/user_goods_category'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577958194')
Time taken: 0.02 seconds, Fetched: 17 row(s)
```

user_info 表

```
hive (kaikeba)> desc user_info;
OK
col_name	data_type	comment
user_id             	string              	                    
user_name           	string              	                    
sex                 	string              	                    
age                 	int                 	                    
city                	string              	                    
firstactivetime     	string              	                    
level               	int                 	                    
extra1              	string              	                    
extra2              	map<string,string>  	                    
Time taken: 0.019 seconds, Fetched: 9 row(s)
```

```
hive (kaikeba)> show create table user_info;
OK
createtab_stmt
CREATE TABLE `user_info`(
  `user_id` string, 
  `user_name` string, 
  `sex` string, 
  `age` int, 
  `city` string, 
  `firstactivetime` string, 
  `level` int, 
  `extra1` string, 
  `extra2` map<string,string>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'collection.delim'=',', 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'mapkey.delim'=':', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/user_info'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577957782')
Time taken: 0.02 seconds, Fetched: 27 row(s)
```

user_list_1 表

```
hive (kaikeba)> desc user_list_1;
OK
col_name	data_type	comment
user_id             	string              	                    
user_name           	string              	                    
Time taken: 0.022 seconds, Fetched: 2 row(s)
```

```
hive (kaikeba)> show create table user_list_1;
OK
createtab_stmt
CREATE TABLE `user_list_1`(
  `user_id` string, 
  `user_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'collection.delim'=',', 
  'field.delim'='\t', 
  'mapkey.delim'=':', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/user_list_1'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577957998')
Time taken: 0.018 seconds, Fetched: 19 row(s)
```

user_list_2 表

```
hive (kaikeba)> desc user_list_2;
OK
col_name	data_type	comment
user_id             	string              	                    
user_name           	string              	                    
Time taken: 0.018 seconds, Fetched: 2 row(s)
```

```
hive (kaikeba)> show create table user_list_2;
OK
createtab_stmt
CREATE TABLE `user_list_2`(
  `user_id` string, 
  `user_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'collection.delim'=',', 
  'field.delim'='\t', 
  'mapkey.delim'=':', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/user_list_2'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577958019')
Time taken: 0.021 seconds, Fetched: 19 row(s)
```

user_list_3 表

```
hive (kaikeba)> desc user_list_3;
OK
col_name	data_type	comment
user_id             	string              	                    
user_name           	string              	                    
Time taken: 0.021 seconds, Fetched: 2 row(s)
```

```
hive (kaikeba)> show create table user_list_3;
OK
createtab_stmt
CREATE TABLE `user_list_3`(
  `user_id` string, 
  `user_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'collection.delim'=',', 
  'field.delim'='\t', 
  'mapkey.delim'=':', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/user_list_3'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577958037')
Time taken: 0.017 seconds, Fetched: 19 row(s)
```

user_refund 表

```
hive (kaikeba)> desc user_refund;
OK
col_name	data_type	comment
user_name           	string              	                    
refund_piece        	int                 	                    
refund_amount       	double              	                    
refund_time         	string              	                    
dt                  	string              	                    
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
dt                  	string              	                    
Time taken: 0.138 seconds, Fetched: 9 row(s)
```

```
hive (kaikeba)> show create table user_refund;
OK
createtab_stmt
CREATE TABLE `user_refund`(
  `user_name` string, 
  `refund_piece` int, 
  `refund_amount` double, 
  `refund_time` string)
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/user_refund'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577958054')
Time taken: 0.02 seconds, Fetched: 21 row(s)
```

user_trade 表

```
hive (kaikeba)> desc user_trade;
OK
col_name	data_type	comment
user_name           	string              	                    
piece               	int                 	                    
price               	double              	                    
pay_amount          	double              	                    
goods_category      	string              	                    
pay_time            	bigint              	                    
dt                  	string              	                    
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
dt                  	string              	                    
Time taken: 0.115 seconds, Fetched: 11 row(s)
```

```
hive (kaikeba)> show create table user_trade;
OK
createtab_stmt
CREATE TABLE `user_trade`(
  `user_name` string, 
  `piece` int, 
  `price` double, 
  `pay_amount` double, 
  `goods_category` string, 
  `pay_time` bigint)
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://node100:9000/user/hive/warehouse/kaikeba.db/user_trade'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1577957792')
Time taken: 0.017 seconds, Fetched: 23 row(s)
```

其实通过上面的 show create table table_name 可以发现有趣的东西。

## 实战演练

执行以下语句，select 查看数据时, 会打印对应的表头:

set hive.cli.print.header=ture;

需求 1：某次经营活动中，商家发起了"异性拼团购"，试着针对某个地区的用户进行推广，找出匹配用户。[拼团中，每个人也是不同的订单]

注：若该表是分区表，则 where 条件中必须对分区字段进行限制。

user_info 表

选出城市在北京，性别为女的 10 个用户名。

```
hive (kaikeba)> select user_name from user_info where city="beijing" and sex='female' limit 5;
OK
user_name
Angela
Becky
Betty
Cheryl
Christine
Time taken: 0.059 seconds, Fetched: 5 row(s)
```

需求 2：某天，发现食物类的商品卖的很好，你能找出几个资深吃货吗？
该表存在分区，则必须加上分区条件。

user_trade 表

选出 在 2019 年 04 月 12 日，购买的商品种类是 food 的用户名、购买数量、支付金额。

```
hive (kaikeba)> select * from user_trade where  goods_category='food' and dt = '2019-04-12';
OK
HELLEN	6	1.1	6.6	food	1555099326	2019-04-12
Knight	70	1.1	77.0	food	1555041764	2019-04-12
Washington	79	1.1	86.9	food	1555112322	2019-04-12
Time taken: 0.153 seconds, Fetched: 3 row(s)
```

需求 3：试着对本公司 2019 年第一季度商品的热度与价值度进行分析。
热度就是被买的数量多，价值度就是买的金额多

user_trade 表 

知识点

count()  sum()  avg() max()  min()  

```sql
select goods_category,sum(pay_amount) as total_amount,count(distinct user_name) from user_trade where substr(dt,0,7) = '2019-01' group by goods_category;
```

```shell
OK
book	4672.5	7
clothes	50200.0	5
computer	946572.0	2
electronics	308858.0	2
food	256.3	4
shoes	169444.8	5
Time taken: 19.8 seconds, Fetched: 6 row(s)
```

```sql
select goods_category,sum(pay_amount) as total_amount,
count(distinct user_name) as user_num 
from user_trade  where dt between '2019-01-01' and '2019-03-31'
group by goods_category;
```

```shell
OK
book	8455.0	14
clothes	124800.0	15
computer	4852848.0	10
electronics	1775378.0	11
food	843.7	14
shoes	292740.0	7
Time taken: 17.285 seconds, Fetched: 6 row(s)
```

需求 4：2019 年 4 月，支付金额超过 5 万元的用户。给 VIP 用户赠送优惠劵。

```sql
select user_name,sum(pay_amount) as pay_total 
from user_trade where dt between '2019-04-01' and '2019-04-30'
group by user_name having pay_total > 50000;
```

```shell
OK
user_name	pay_total
Carroll	58596.0
Cheryl	123144.0
Iris	188870.0
JUNE	519948.0
Mitchell	193314.0
Morris	166984.0
Scott	95546.0
Time taken: 24.442 seconds, Fetched: 7 row(s)
```

一般 SQL 的执行顺序是：

from -> where -> group by -> having -> select -> order by

需求 5：2019 年 4 月，支付金额前 5 名用户。

2 个 Stage  Stage-1 Stage-2

```sql
select user_name,sum(pay_amount) as pay_total 
from user_trade where dt between '2019-04-01' and '2019-04-30'
group by user_name order by pay_total desc limit 5;
```

```
OK
user_name	pay_total
JUNE	519948.0
Mitchell	193314.0
Iris	188870.0
Morris	166984.0
Cheryl	123144.0
Time taken: 42.885 seconds, Fetched: 5 row(s)
```

2 个 Stage  Stage-1 Stage-2

```sql
select user_name,sum(pay_amount) as pay_total 
from user_trade where dt between '2019-04-01' and '2019-04-30' group by user_name having pay_total > 50000 order by pay_total desc limit 5;
```

```
OK
user_name	pay_total
JUNE	519948.0
Mitchell	193314.0
Iris	188870.0
Morris	166984.0
Cheryl	123144.0
Time taken: 44.016 seconds, Fetched: 5 row(s)
```

需求 6：去年的劳动节新用户推广活动价值分析。即拉新分析。
[就是从去年劳动节开始到现在激活了多少用户][首次激活时间大于 5.1]

user_info 表

在 hive 中查看某个函数的用法：desc function extended XX函数名

```sql
select count(user_id) as num from user_info 
where firstactivetime > '2019-05-01';
```

```shell
OK
num
1
Time taken: 18.271 seconds, Fetched: 1 row(s)
```

用户的首次激活时间与 2019-05-01 的日期时间间隔

```sql
select user_id,user_name,sex,datediff('2019-05-01',to_date(firstactivetime)) from user_info limit 5;
```

```
OK
user_id	user_name	sex	_c3
10001	Abby	female	383
10002	Ailsa	female	332
10003	Alice	female	148
10004	Alina	female	45
10005	Allison	female	440
Time taken: 0.072 seconds, Fetched: 5 row(s)
```

```sql
select pay_time , from_unixtime(pay_time,'yyyy-MM-dd hh:mm:ss')
from user_trade where dt = '2019-04-09'; 
```

```shell
OK
pay_time	_c1
1554770447	2019-04-09 12:40:47
1554793198	2019-04-09 06:59:58
1554838064	2019-04-09 07:27:44
1554780966	2019-04-09 03:36:06
1554811814	2019-04-09 12:10:14
Time taken: 0.094 seconds, Fetched: 5 row(s)
```

需求 7：对用户的年龄段进行分析，观察分布情况。

user_info 表

case when

group by 后面能不能使用 age_type ?

一般 SQL 的执行顺序是：

from -> where -> group by -> having -> select -> order by

统计 20 岁以下、20-30 岁、30-40岁、40 岁以上四个年龄段的用户分布情况

```sql
select count(distinct user_id) as user_num,
case when age < 20 then '20 岁以下' 
    when age >= 20 and age < 30 then '20-30岁'
    when age >= 30 and age < 40 then '30-40岁'
    else '40岁以上' end as age_type
from user_info group by
    case when age < 20 then '20 岁以下' 
    when age >= 20 and age < 30 then '20-30岁'
    when age >= 30 and age < 40 then '30-40岁'
    else '40岁以上' end;
```

```
OK
user_num	age_type
37	20 岁以下
48	20-30岁
62	30-40岁
180	40岁以上
Time taken: 19.904 seconds, Fetched: 4 row(s)
```

需求 8：王思聪的微博抽奖活动引起争议，如果我们想要观察用户等级随性别分布情况。

user_info 表

```sql
select sex,if(level<5,'低','高') as level_dense,count(distinct user_id) user_num from user_info 
group by sex,if(level<5,'低','高');
```

```
OK
sex	level_dense	user_num
female	低	66
female	高	111
male	低	67
male	高	83
Time taken: 20.268 seconds, Fetched: 4 row(s)
```

```sql
select sex,if (level>5,'高','低') as level_type,
count(distinct user_id) user_num 
from user_info
group by sex,if (level>5,'高','低')
```

```
OK
sex	level_type	user_num
female	低	80
female	高	97
male	低	83
male	高	67
Time taken: 19.24 seconds, Fetched: 4 row(s)
```

需求 9：分析每个月的拉新情况，可以倒推回运营效果。

```sql
select count(distinct user_id) as user_num,month(firstactivetime) as active_time from  user_info group by month(firstactivetime);
```

```
user_num	active_time
26	1
31	2
40	3
33	4
28	5
24	6
27	7
31	8
21	9
20	10
27	11
19	12
```

```sql
select substr(firstactivetime,1,7) as month,count(distinct user_id) user_num from user_info group by substr(firstactivetime,1,7);
```

```shell
month	user_num
2017-01	7
2017-02	14
2017-03	11
2017-04	15
2017-05	14
2017-06	10
2017-07	12
2017-08	20
2017-09	12
2017-10	11
2017-11	17
2017-12	7
2018-01	12
2018-02	5
2018-03	15
2018-04	10
2018-05	13
2018-06	14
2018-07	15
2018-08	11
2018-09	9
2018-10	9
2018-11	10
2018-12	12
2019-01	7
2019-02	12
2019-03	14
2019-04	8
2019-05	1
```

需求 10：找出不同手机品牌的用户分布情况。

```json
user_info.user_id	user_info.user_name	user_info.sex	user_info.age	user_info.city	user_info.firstactivetime	user_info.level	user_info.extra1	user_info.extra2
10001	Abby	female	38	hangzhou	2018-04-13 01:06:07	2	{"systemtype": "android", "education": "doctor", "marriage_status": "1", "phonebrand": "VIVO"}	{"systemtype":"android","education":"doctor","marriage_status":"1","phonebrand":"VIVO"}
10002	Ailsa	female	42	shenzhen	2018-06-03 20:30:03	2	{"systemtype": "android", "education": "bachelor", "marriage_status": "0", "phonebrand": "YIJIA"}	{"systemtype":"android","education":"bachelor","marriage_status":"0","phonebrand":"YIJIA"}
Time taken: 0.096 seconds, Fetched: 2 row(s)
```

```sql
select count(distinct user_id) as user_num,extra2['phonebrand'] from user_info group by extra2['phonebrand'] order by user_num desc limit 10;
```

```
OK
user_num	_c1
36	VIVO
35	MI
35	CHUIZI
33	YIJIA
28	HUAWEI
27	iphone6
24	iphone6s
24	iphone5
24	iphoneXS
22	iphone7
Time taken: 36.726 seconds, Fetched: 10 row(s)
```

```sql
select extra2['phonebrand'] as phone_brand,
count(distinct user_id) as user_num from user_info 
group by extra2['phonebrand'] order by user_num desc limit 10;
```

```
OK
phone_brand	user_num
VIVO	36
MI	35
CHUIZI	35
YIJIA	33
HUAWEI	28
iphone6	27
iphone6s	24
iphone5	24
iphoneXS	24
iphone7	22
Time taken: 36.336 seconds, Fetched: 10 row(s)
```

```sql
select get_json_object(extra1,'$.phonebrand') as phone_brand,
count(distinct user_id) as user_num from user_info 
group by get_json_object(extra1,'$.phonebrand') order by user_num desc limit 10;
```

```
OK
phone_brand	user_num
VIVO	36
MI	35
CHUIZI	35
YIJIA	33
HUAWEI	28
iphone6	27
iphone6s	24
iphone5	24
iphoneXS	24
iphone7	22
Time taken: 36.827 seconds, Fetched: 10 row(s)
```

需求 11：不同性别、教育程度的分布情况(使用 user_info 表)

```sql
select sex,extra2['education'],count(distinct user_id) from user_info group by sex,extra2['education'];
```

```sql
select sex,get_json_object(extra1,'$.education'),count(distinct user_id) from user_info group by sex,get_json_object(extra1,'$.education');
```

```
OK
sex	_c1	_c2
female	bachelor	69
female	doctor	61
female	master	47
male	bachelor	47
male	doctor	49
male	master	54
Time taken: 16.686 seconds, Fetched: 6 row(s)
```

需求 12：ELLA 用户 2018 年的平均每次支付金额，以及 2018 年最大的支付日期与最小的支付日期的间隔。

```
hive (kaikeba)> desc function extended datediff;
OK
tab_name
datediff(date1, date2) - Returns the number of days between date1 and date2
date1 and date2 are strings in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. The time parts are ignored.If date1 is earlier than date2, the result is negative.
Example:
   > SELECT datediff('2009-07-30', '2009-07-31') FROM src LIMIT 1;
  1
Function class:org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateDiff
Function type:BUILTIN
Time taken: 0.016 seconds, Fetched: 7 row(s)
```

```sql
select avg(pay_amount) as avg_amount,
datediff(max(from_unixtime(pay_time,'yyyy-MM-dd')),min(from_unixtime(pay_time,'yyyy-MM-dd')))
from user_trade where year(dt) = '2018' and user_name = 'ELLA';
```

```
OK
avg_amount	_c1
49156.93333333333	-154
Time taken: 20.581 seconds, Fetched: 1 row(s)
```

```sql
select avg(pay_amount) as avg_amount,
datediff(max(from_unixtime(pay_time,'yyyy-MM-dd')),min(from_unixtime(pay_time,'yyyy-MM-dd')))
from user_trade where year(dt) = '2018' and user_name = 'ELLA';
```

```
avg_amount	_c1
49156.93333333333	154
Time taken: 21.196 seconds, Fetched: 1 row(s)
```

需求 13：找出在 2018 年具有 VIP 潜质的用户，发送 VIP 试用劵。

参考实现：2018 年购买的商品品类在两个以上的用户数[如果这个数量很大，比如十几万，就可以再把条件门槛调高，直至一个合适的范围]

```sql
select user_name,count(distinct goods_category) as category_num
from user_trade where year(dt) = '2019' group by user_name
having count(goods_category) > 2;
```

```
OK
user_name	category_num
Christy	2
Fiona	2
Janet	2
Keith	2
Morris	2
Wheeler	2
```

```sql
SELECT count(a.user_name)
FROM
(SELECT user_name,
count(distinct goods_category) as category_num
FROM user_trade
WHERE year(dt)='2018'
GROUP BY user_name
HAVING count(distinct goods_category)>2) a;
```

需求 14：用户激活时间在 2018 年，年龄段在 20-30 岁和 30-40 岁的婚姻状况分布。

user_info 表

第一步：先选出激活时间在 2018 年的用户，并把他们所在的年龄段计算好，并提取出婚姻状况。

第二步：取出年龄段在 20-30 岁和 30-40 岁的用户，把他们的婚姻状况转义
成可理解的说明。

第三步：聚合计算，针对年龄段、婚姻状况的聚合。

```sql
select user_name,age,extra2['marriage_status'] as marriage_status from user_info where year(firstactivetime) = '2018' limit 10;
```

```sql
select case 
when age < 20 then '20岁以下'
when age >= 20 and age < 30 then '20-30岁'
when age >= 30 and age < 40 then '30-40岁'
else '40岁以上' end as age_type,extra2['marriage_status'] as marriage_status,user_id from user_info where year(firstactivetime) = '2018' limit 10;
```

```
OK
age_type	marriage_status	user_id
30-40岁	1	10001
40岁以上	0	10002
20岁以下	1	10003
40岁以上	1	10005
40岁以上	1	10007
20-30岁	1	10010
30-40岁	0	10017
20岁以下	1	10019
20-30岁	0	10021
40岁以上	0	10024
Time taken: 0.058 seconds, Fetched: 10 row(s)
```

```sql
select case 
when age < 20 then '20岁以下'
when age >= 20 and age < 30 then '20-30岁'
when age >= 30 and age < 40 then '30-40岁'
else '40岁以上' end as age_type,if(extra2['marriage_status']=0,'未婚','已婚') as marriage_status,user_id from user_info where year(firstactivetime) = '2018' limit 10;
```

```shell
OK
age_type	marriage_status	user_id
30-40岁	已婚	10001
40岁以上	未婚	10002
20岁以下	已婚	10003
40岁以上	已婚	10005
40岁以上	已婚	10007
20-30岁	已婚	10010
30-40岁	未婚	10017
20岁以下	已婚	10019
20-30岁	未婚	10021
40岁以上	未婚	10024
Time taken: 0.052 seconds, Fetched: 10 row(s)
```

```sql
select a.age_type,a.marriage_status,count(distinct user_id) as user_num from
(select case 
when age < 20 then '20岁以下'
when age >= 20 and age < 30 then '20-30岁'
when age >= 30 and age < 40 then '30-40岁'
else '40岁以上' end as age_type,if(extra2['marriage_status']=0,'未婚','已婚') as marriage_status,user_id from user_info where year(firstactivetime) = '2018')  a group by a.age_type,a.marriage_status;
```

```
OK
a.age_type	a.marriage_status	user_num
20-30岁	已婚	9
20-30岁	未婚	9
20岁以下	已婚	7
20岁以下	未婚	7
30-40岁	已婚	10
30-40岁	未婚	14
40岁以上	已婚	33
40岁以上	未婚	46
Time taken: 19.943 seconds, Fetched: 8 row(s)
```

```sql
select a.age_type,a.marriage_status,count(distinct user_id) as user_num from
(select case 
when age < 20 then '20岁以下'
when age >= 20 and age < 30 then '20-30岁'
when age >= 30 and age < 40 then '30-40岁'
else '40岁以上' end as age_type,if(extra2['marriage_status']=0,'未婚','已婚') as marriage_status,user_id from user_info where year(firstactivetime) = '2018')  a where a.age_type in ('20-30岁','30-40岁') group by a.age_type,a.marriage_status;
```

```
OK
a.age_type	a.marriage_status	user_num
20-30岁	已婚	9
20-30岁	未婚	9
30-40岁	已婚	10
30-40岁	未婚	14
Time taken: 20.8 seconds, Fetched: 4 row(s)
```

```sql
SELECT a.age_type,
if(a.marriage_status=1,'已婚','未婚'),
count(distinct a.user_id)
FROM
(SELECT case when age<20 then '20岁以下'
when age>=20 and age<30 then '20-30岁'
when age>=30 and age<40 then '30-40岁'
else '40岁以上' end as age_type,
get_json_object(extra1, '$.marriage_status') as
marriage_status,
user_id
FROM user_info
WHERE to_date(firstactivetime) between '2018-01-01'
and '2018-12-31') a
WHERE a.age_type in ('20-30岁','30-40岁')
GROUP BY a.age_type,
if(a.marriage_status=1,'已婚','未婚');
```

```
OK
a.age_type	_c1	_c2
20-30岁	已婚	9
20-30岁	未婚	9
30-40岁	已婚	10
30-40岁	未婚	14
Time taken: 18.525 seconds, Fetched: 4 row(s)
```

小练习

需求 15-1：激活天数距今超过 300 天的男女分布情况。

需求 15-2：不同性别、教育程度的分布情况。

需求 15-3：2019 年 1 月 1 日到 2019 年 4 月 30 日，每个时段的不同品类购买金额分布。

```sql
select sex,count(distinct user_id) from user_info where datediff(current_date(),to_date(firstactivetime)) > 300 group by sex;
```

```
OK
sex	_c1
female	177
male	150
Time taken: 19.818 seconds, Fetched: 2 row(s)
```

```sql
select sex,extra2['education'],count(distinct user_id) from user_info group by sex,extra2['education'];
```

```sql
select sex,get_json_object(extra1, '$.education'),count(distinct user_id)  from user_info group by sex,get_json_object(extra1, '$.education');
```

```
sex	_c1	_c2
female	bachelor	69
female	doctor	61
female	master	47
male	bachelor	47
male	doctor	49
male	master	54
Time taken: 17.75 seconds, Fetched: 6 row(s)
```
-- 12 小时制
```sql
select substr(from_unixtime(pay_time,'yyyy-MM-dd hh'),12) as hour_time, goods_category,sum(pay_amount) as total_amount
from user_trade where dt between '2019-01-01' and '2019-04-30'
group by substr(from_unixtime(pay_time,'yyyy-MM-dd hh'),12), goods_category;
```

```
OK
hour_time	goods_category	total_amount
01	book	1504.1
01	clothes	20800.0
01	computer	1146552.0
01	electronics	237754.0
01	food	144.10000000000002
02	book	1459.6
02	clothes	1600.0
02	electronics	188870.0
02	food	74.8
02	shoes	32373.6
03	clothes	15400.0
03	computer	839916.0
03	electronics	246642.0
03	food	47.3
03	shoes	2066.4
04	book	1548.6
04	electronics	144430.0
04	food	77.0
05	book	801.0
05	electronics	353298.0
05	food	46.2
05	shoes	47527.2
06	clothes	12200.0
06	computer	359964.0
06	electronics	413292.0
06	food	94.6
07	book	801.0
07	clothes	18400.0
07	computer	719928.0
07	food	104.5
07	shoes	57170.4
08	clothes	42400.0
08	computer	126654.0
08	food	69.3
08	shoes	88166.4
09	book	1397.3
09	clothes	23200.0
09	computer	839916.0
09	electronics	195536.0
09	shoes	62680.8
10	computer	466620.0
10	electronics	97768.0
10	food	181.5
10	shoes	66124.8
11	book	418.3
11	clothes	15000.0
11	computer	726594.0
11	food	86.9
12	book	1014.6
12	clothes	23400.0
12	computer	539946.0
12	electronics	297748.0
12	food	110.0
Time taken: 16.779 seconds, Fetched: 53 row(s)
```
-- 24 小时制
```sql
select substr(from_unixtime(pay_time,'yyyy-MM-dd HH'),12), 
goods_category,sum(pay_amount)
from user_trade where dt between '2019-01-01' and '2019-04-30'
group by substr(from_unixtime(pay_time,'yyyy-MM-dd HH'),12),
goods_category;
```

```
OK
_c0	goods_category	_c2
00	book	1014.6
00	clothes	19200.0
01	book	1504.1
01	clothes	13800.0
01	computer	586608.0
01	electronics	217756.0
01	food	97.9
02	clothes	1600.0
02	food	12.1
03	clothes	15400.0
03	computer	286638.0
03	electronics	246642.0
03	food	47.3
04	book	1548.6
04	electronics	144430.0
04	food	77.0
05	book	801.0
05	electronics	151096.0
05	food	46.2
06	clothes	12200.0
06	computer	359964.0
06	electronics	304414.0
07	book	801.0
07	clothes	18400.0
07	computer	139986.0
07	food	82.5
08	clothes	42400.0
08	food	46.2
08	shoes	22041.6
09	book	854.3999999999999
09	clothes	10000.0
09	computer	839916.0
09	shoes	62680.8
10	computer	466620.0
10	electronics	97768.0
11	book	418.3
11	clothes	14800.0
11	computer	193314.0
12	clothes	4200.0
12	computer	539946.0
12	electronics	297748.0
12	food	110.0
13	clothes	7000.0
13	computer	559944.0
13	electronics	19998.0
13	food	46.2
14	book	1459.6
14	electronics	188870.0
14	food	62.7
14	shoes	32373.6
15	computer	553278.0
15	shoes	2066.4
17	electronics	202202.0
17	shoes	47527.2
18	electronics	108878.0
18	food	94.6
19	computer	579942.0
19	food	22.0
19	shoes	57170.4
20	computer	126654.0
20	food	23.1
20	shoes	66124.8
21	book	542.9
21	clothes	13200.0
21	electronics	195536.0
22	food	181.5
22	shoes	66124.8
23	clothes	200.0
23	computer	533280.0
23	food	86.9
Time taken: 17.378 seconds, Fetched: 70 row(s)
```

需求 16 ：某年度对用户满意度进行调研分析，找出目标人群。

[特别注意大多数情况下都是要去重的和表的别名备注]

购买很多次只计算一次，所以要去重，不然购买一次退款两次;而且是要先去重再做表连接;部门定期分析客户满意度,重点关注购买完又退款的,明显是不满意的;或者购买最多的,前多少人;或者使用时长很长的，看广告越多的;

找出既在 user_list_1 也在 user_list_2 的用户

需求 17 ：用户忠诚度类项目的调研分析，找出目标人群。

在 2017 年和 2018 年都购买的用户[也可以写好几年，或者消费频次]

需求 18：高忠诚度用户匹配分析，以生成心路历程类报表推送给用户，找出目标人群

网易云音乐每年生成的年度歌单[先把每年都在使用的找出来]

在 2017 2018 2019 都有交易的用户

需求 19：取出在 user_list_1 表 但是不在 user_list_2 的用户。



需求 20：无退款服务类用户的定位分析，用以发送服务判断类用户调研。

在 2019 年购买，但是没有退款的用户。

需求 21：对重点客户的学历进行调研分析，观察其分布情况。

需求 22：老客户召回计划，匹配到目标人群。

查找 2017 年，2018 年都有的，但是 2019 没有的[大部分以月为判定]

需求 23：对近几年的交易进行分析，评估平台的价值。

需求 24：对某年度的客户交易价值进行分析。

2019 年每个用户的支付和退款金额汇总[注重买了又退的] [这个是所有的客户，下一个是支付的客户]。


需求 25：对某年度的重点客户交易价值进行分析。

2019 年每个支付用户的支付金额和退款金额。
关注的是支付的金额不为 0 的用户，有的用户是注册了，但是没有支付。

需求 26：对沉默用户的年龄段进行分析，用以部署活动来刺激其消费。

激活之后一直没有使用，这里理解为一直没有支付的用户。

参考实现：首次激活时间在 2017 年，但是一直没有支付的用户年龄段分布。

需求 27：对活跃用户的激活时间段进行分析，用以部署活动来刺激其消费。

2018、2019 年交易的用户,其激活时间段分布。

需求 28-1：在 2019 年购买后又退款的用户性别分布。

需求 28-2：在 2018 年购买，但是没在 2019 年购买的用户城市分布。

需求 28-3：2017-2019 年，有交易但是没退款的用户的手机品牌分布。

需求 29：对 2018 年公司的支付总额按月度累计进行分析。

需求 30：对 2017 年和 2018 年公司的支付总额按月度累计进行分析，按年度进行汇总。

需求 31：对 2018 年每个月的近三个月进行移动平均求平均支付金额。

需求 32：对 2019 年 1 月份用户的购买爱好进行分析。

2019 年 1 月，用户购买的商品品类数量的排名。

需求 33：选出 2019 年支付金额排名在第 10、20、30 名的用户。

需求 34：将 2019 年 1 月的支付用户，按照支付金额分成 5 组。

需求 35：选出 2019 年退款金额排名前 10% 的用户。

需求 36：支付时间间隔超过 100 天的用户数[跨时间潜在 VIP 用户流失分析]。

需求 37：每个城市，不同性别，2018 年支付金额最高 TOP3 用户。

需求 38：每个手机品牌退款金额前 25% 用户。

需求 39：计算出每 12 个月的用户累计支付金额。

需求 40：计算出每 4 个月的最大退款金额。

需求 41：退款时间间隔最长的用户。



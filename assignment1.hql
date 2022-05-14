!hadoop fs -mkdir /salma01;


create database assign1_salma;
describe database assign1_salma;
create database assign1_loc location '/user/hive/warehouse/hp_db/assign1_loc';

create table if not exists assign1_salma.assign1_intern_tab ( id int, name String,  age int , jobtype String,  storeid int, storelocation String, salary bigint , yrsofexp int )
    row format delimited
    fields terminated by ',';

load data local inpath 'employee.csv' into table assign1_salma.assign1_intern_tab;

select * from assign1_salma.assign1_intern_tab limit 10;
! hadoop fs -mkdir /mydata;

!hadoop fs -put employee.csv /mydata;

create external table if not exists assign1_loc.assign1_extern_tab (id int, name String, age int , job string , store int , loc string , salary bigint, yrs int )
     row format delimited
    fields terminated by ','
    location 'hdfs://namenode:8020/mydata';

drop table assign1_salma.assign1_intern_tab;
drop table assign1_loc.assign1_intern_tab;

create table if not exists assign1_salma.assign1_intern_tab ( id int, name String,  age int , jobtype String,  storeid int, storelocation String, salary bigint , yrsofexp int )
    row format delimited
    fields terminated by ',';

create external table if not exists assign1_loc.assign1_extern_tab (id int, name String, age int , job string , store int , loc string , salary bigint, yrs int )
     row format delimited
    fields terminated by ','
    location 'hdfs://namenode:8020/mydata';


create table if not exists assign1_salma.assign1_intern_tab ( id int, name String,  age int , jobtype String,  storeid int, storelocation String, salary bigint , yrsofexp int )
    row format delimited
    fields terminated by ',';

create external table if not exists assign1_loc. assign1_extern_tab (id int, name String, age int , job string , store int , loc string , salary bigint, yrs int )
     row format delimited
    fields terminated by ','
    location 'hdfs://namenode:8020/mydata';

create  table if not exists assign1_salma.staging(id int, name String, age int , job string , store int , loc string , salary bigint, yrs int )
     row format delimited
    fields terminated by ','
    ;

load data local inpath 'employee.csv' into table assign1_salma.staging;

INSERT INTO assign1_salma.assign1_intern_tab SELECT * FROM assign1_salma.staging; 
INSERT INTO  assign1_loc.assign1_extern_tab SELECT * FROM assign1_salma.staging;
describe formatted  assign1_salma.assign1_intern_tab;
describe formatted assign1_loc.assign1_extern_tab;
!wc -l songs.csv;
create table if not exists mysong (id string,latitude string, loc string, long string , name string, duration string, num string , song_id string, title string , year string) row format delimited
    fields terminated by ',';

load data local inpath 'songs.csv' into table mysong;

Select * from mysong limit 10;

select count(*) from mysong;

!hadoop fs -mkdir /externaldir;
	
create external table if not exists mysong1 (id string,latitude string, loc string, long string , name string, duration string, num string , song_id string, title string , year string) row format delimited
    fields terminated by ','
    location 'hdfs://namenode:8020/externaldir';

!hadoop fs -put songs.csv /externaldir;

DROP TABLE IF EXISTS mytable;

create table if not exists mytable
(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

load data local inpath 'employee.csv' into table  mytable;

Describe formatted assign1_salma.assign1_intern_tab;




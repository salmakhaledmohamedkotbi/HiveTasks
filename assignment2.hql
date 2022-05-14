create database asign2_salma;
!hadoop - mkdir /new
create table  asign2_salma.son( artist_id String,  artist_latitude decimal,artist_location String, artist_longitude decimal ,artist_name String,duration date , num_songs int, song_id String ,titile String , year bigint)
    partitioned by (artist string, year1 bigint)
     row format delimited
    fields terminated by ',';

!hadoop fs -put songs.csv hdfs:///new;
Select * from asign2_salma.son;

alter table asign2_salma.son add partition (artist ='salma', year1=2009) location 'hdfs:///salma01';

set hive.exec.dynamic.partition.mode=nonstrict
create external table staging2 (id string,latitude decimal, loc string, long decimal , name string, duration date, num int , song_id string, title string , year bigint)
     row format delimited
    fields terminated by ','
    location 'hdfs://namenode:8020/new';

insert overwrite table asign2_salma.son partition (artist,year1)
 select id,latitude,loc,long,name,duration,num,song_id,title,year,name,year from stag;

truncate table asign2_salma.son;
from staging2
    insert overwrite table asign2_salma.son partition(artist='salma',year1=2008)
    select id,latitude,loc,long,name,duration,num,song_id,title,year
    where name='salma' and year =2008
    insert overwrite table asign2_salma.son partition(artist='marc',year1=2006)
    select id,latitude,loc,long,name,duration,num,song_id,title,year
    where name='marc' and year =2006
     ;
truncate table asign2_salma.son;
drop table asign2_salma.son;
create table  asign2_salma.son( artist_id String,  artist_latitude decimal,artist_location String, artist_longitude decimal ,duration date , num_songs int, song_id String ,titile String  )
    partitioned by (year bigint, artist_name string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");


from staging2
insert overwrite table asign2_salma.son partition(year  = '2005',artist_name)
    select id,latitude,loc,long,name,duration,num,song_id,title

where year='2006' ;
create table new like staging2;
avro-tools-1.9.1.jar getschema myschema.avro;

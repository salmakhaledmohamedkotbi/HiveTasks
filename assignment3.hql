create table events(
artist string,auth string,firstName string,gender string,itemInSession string,lastName string,length string,level string,location string,method string,page string,registration string,sessionId string,song string,
status string,ts string,userAgent string,userId string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


load data local inpath 'events.csv' into table events;


Select userId, song, sessionId, last_value(song)over(partition by sessionId order by itemInSession ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following), first_value(song)over(partition by sessionId order by itemInSession ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) 
 from events limit 50;


 SELECT userId,count(distinct song), RANK() OVER  (Order BY COUNT(distinct song) DESC) FROM events group by userId;


SELECT userId,count(distinct song), Row_number() OVER  (Order BY COUNT(distinct song) DESC) from events where page ='nextSong'  group by userId ;

SELECT COUNT(song) FROM events GROUP BY location, artist
GROUPING SETS ((location,artist),location,());


SELECT COUNT(song) FROM events GROUP BY location, artist
GROUPING SETS ((location,artist),location, artist, ());



select sessionId, userId,lead(song) over
(partition by userId order by sessionId desc) from events
order by sessionId desc;


select userId,song ,ts from events
order by userId,song, ts;


select userId,song ,ts from events
cluster by userId, song, ts;

use dtarasov;

SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

CREATE EXTERNAL TABLE IF NOT EXISTS PURCHASE_raw(product_name STRING, product_price DECIMAL(10,2), purchase_date STRING, product_category STRING, client_ip STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '/user/cloudera/flume/events';

CREATE EXTERNAL TABLE IF NOT EXISTS PURCHASE (product_name STRING, product_price DECIMAL(10,2), purchase_timestamp BIGINT, product_category STRING, client_ip STRING) PARTITIONED BY (purchase_date DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '/user/cloudera/hive/purchase'; 

set hive.exec.dynamic.partition.mode=nonstrict;

insert into PURCHASE PARTITION (purchase_date) select product_name, product_price, unix_timestamp(purchase_date, "yyyy-MM-dd'T'HH:mm:ss") as purchase_timestamp, product_category, client_ip, to_date(from_unixtime(unix_timestamp(purchase_date, "yyyy-MM-dd'T'HH:mm:ss"))) as purchase_date from PURCHASE_raw;

create table top_categories as select product_category, count as category_count from PURCHASE group by product_category order by category_count desc limit 10;

create table top_products_in_categories as select product_name, product_category, count as product_count from (select product_name, product_category, count, row_number() over (partition by product_category order by count desc) as rn from (select product_name, product_category, count(1) as count from PURCHASE group by product_name, product_category) as ss) as sss where rn<=10;

create external table GEO_BLOCKS (network string,geoname_id string,registered_country_geoname_id string,represented_country_geoname_id string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '/user/cloudera/geo/blocks';

create external table GEO_LOCATIONS (geoname_id string,locale_code string,continent_code string,continent_name string,country_iso_code string,country_name string,is_in_european_union string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '/user/cloudera/geo/locations';

add jar hdfs:///user/cloudera/hive-udf-ip-range-1.0-SNAPSHOT.jar;
create temporary function is_in_range as 'su.test.hiveudfiprange.IsInRange';

set hive.auto.convert.join=true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;

create table ORC_GEO_IP_RANGE STORED AS ORC tblproperties("compress.mode"="SNAPPY") as select * from GEO_IP_RANGE;
create table ORC_GEO_COUNTRY STORED AS ORC tblproperties("compress.mode"="SNAPPY") as select * from GEO_COUNTRY;

set mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx2048m;
set mapreduce.reduce.java.opts=-Djava.net.preferIPv4Stack=true -Xmx2048m;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;

create table top_countries_by_total as select gc.country_name as country, stt.total as total from ORC_GEO_COUNTRY gc join (select r.geoname_id,sum(st.total) as total from ORC_GEO_IP_RANGE r ,(select client_ip, sum(product_price) as total from PURCHASE group by client_ip) st where (is_in_range(r.network,st.client_ip) = true) group by r.geoname_id) stt on stt.geoname_id = gc.geoname_id order by stt.total desc limit 10;
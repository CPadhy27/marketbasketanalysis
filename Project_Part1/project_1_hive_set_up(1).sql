create database amazon_review;
--drop table amazon_review.amazon_reviews_parquet;

CREATE EXTERNAL TABLE amazon_review.amazon_reviews_parquet(
  `marketplace` string, 
  `customer_id` string, 
  `review_id` string, 
  `product_id` string, 
  `product_parent` string, 
  `product_title` string, 
  `star_rating` int, 
  `helpful_votes` int, 
  `total_votes` int, 
  `vine` string, 
  `verified_purchase` string, 
  `review_headline` string, 
  `review_body` string, 
  `review_date` DATE, 
  `year` int)
PARTITIONED BY ( 
  `product_category` string)
--ROW FORMAT DELIMITED
--STORED AS PARQUET
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs:///hive/amazon-reviews-pds/parquet/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1583454851');

Msck repair table amazon_review.amazon_reviews_parquet;

set hive.cli.print.header=true;

select count(*) from amazon_review.amazon_reviews_parquet;
-- 32,380,262

select product_category,count(*) as cnt, avg(length(review_body)) as avg_review
from amazon_review.amazon_reviews_parquet
group by product_category;
/*
product_category        cnt     avg_review
Music   6177781 644.6888965651874
Video_Games     1808486 533.2577278143809
Wireless        9038249 259.3749854083725
Toys    4981601 254.49733589629056
Automotive      3516476 215.5575777915878
Sports  4860054 255.40180549050854
Digital_Music_Purchase  1852184 239.50034338731066
Digital_Video_Games     145431  360.69393999821216
*/


CREATE TABLE amazon_review.amazon_reviews_exclude
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs:///hive/amazon-reviews-pds/exclude/'  
AS
SELECT customer_id, product_id, product_category
 FROM  amazon_review.amazon_reviews_parquet
 GROUP BY customer_id, product_id, product_category
 HAVING count(*)>1;

select count(*) from amazon_review.amazon_reviews_exclude;
--1,024,207
 
CREATE TABLE amazon_review.amazon_reviews_exclude_part
(customer_id string, 
 product_id string
)
PARTITIONED BY ( 
  `product_category` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs:///hive/amazon-reviews-pds/exclude-part/';   

insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Automotive')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Automotive'
 GROUP BY customer_id, product_id
 HAVING count(*)>1;
 
insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Wireless')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Wireless'
 GROUP BY customer_id, product_id
 HAVING count(*)>1; 
 
insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Music')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Music'
 GROUP BY customer_id, product_id
 HAVING count(*)>1; 

insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Digital_Music_Purchase')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Digital_Music_Purchase'
 GROUP BY customer_id, product_id
 HAVING count(*)>1; 

insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Sports')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Sports'
 GROUP BY customer_id, product_id
 HAVING count(*)>1; 

insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Toys')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Toys'
 GROUP BY customer_id, product_id
 HAVING count(*)>1;  
 
insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Digital_Video_Games')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Digital_Video_Games'
 GROUP BY customer_id, product_id
 HAVING count(*)>1; 

insert into amazon_review.amazon_reviews_exclude_part
partition(product_category='Video_Games')
SELECT customer_id, product_id
 FROM  amazon_review.amazon_reviews_parquet
 WHERE product_category='Video_Games'
 GROUP BY customer_id, product_id
 HAVING count(*)>1;  
 
select count(*) from amazon_review.amazon_reviews_exclude_part; 
--1,024,207

select product_category,count(*) as cnt
from amazon_review.amazon_reviews_parquet
group by product_category;
/*
ELAPSED TIME: 180.69 s

product_category        cnt
Music   6177781
Video_Games     1808486
Wireless        9038249
Toys    4981601
Automotive      3516476
Sports  4860054
Digital_Music_Purchase  1852184
Digital_Video_Games     145431
*/

--Exclude table is partitioned 
select product_category,count(*) as cnt
from amazon_review.amazon_reviews_parquet p
where not exists(
SELECT 1 
FROM amazon_review.amazon_reviews_exclude_part e
WHERE p.customer_id = e.customer_id
and p.product_id = e.product_id
and p.product_category = e.product_category)
group by product_category; 
/*
----------------------------------------------------------------------------------------------
ELAPSED TIME: 124.20 s
----------------------------------------------------------------------------------------------
OK
product_category        cnt
Automotive      3515501
Music   4538542
Digital_Video_Games     145413
Wireless        8989144
Toys    4863220
Digital_Music_Purchase  1636204
Video_Games     1765326
Sports  4850620
Time taken: 125.165 seconds, Fetched: 8 row(s)
*/

--Exclude table is not partitioned
select product_category,count(*) as cnt
from amazon_review.amazon_reviews_parquet p
where not exists(
SELECT 1 
FROM amazon_review.amazon_reviews_exclude e
WHERE p.customer_id = e.customer_id
and p.product_id = e.product_id
and p.product_category = e.product_category)
group by product_category; 
/*
ELAPSED TIME: 122.38 s
----------------------------------------------------------------------------------------------
OK
product_category        cnt
Automotive      3515501
Music   4538542
Digital_Video_Games     145413
Wireless        8989144
Toys    4863220
Digital_Music_Purchase  1636204
Video_Games     1765326
Sports  4850620
Time taken: 123.101 seconds, Fetched: 8 row(s)
*/

-- Using JOIN and Hint - almost twice as fast
select /*+ MAPJOIN(e) */ p.product_category,count(*) as cnt
from amazon_review.amazon_reviews_parquet p
LEFT OUTER JOIN amazon_review.amazon_reviews_exclude_part e
ON p.customer_id = e.customer_id and p.product_id = e.product_id and p.product_category = e.product_category
WHERE e.customer_id is NULL
group by p.product_category; 
/*
----------------------------------------------------------------------------------------------
ELAPSED TIME: 79.29 s
----------------------------------------------------------------------------------------------
OK
p.product_category      cnt
Automotive      3515501
Music   4538542
Digital_Video_Games     145413
Wireless        8989144
Toys    4863220
Digital_Music_Purchase  1636204
Video_Games     1765326
Sports  4850620
Time taken: 79.932 seconds, Fetched: 8 row(s)
*/
 
-- Hint with non partitioned table
select /*+ MAPJOIN(e) */ p.product_category,count(*) as cnt
from amazon_review.amazon_reviews_parquet p
LEFT OUTER JOIN amazon_review.amazon_reviews_exclude e
ON p.customer_id = e.customer_id and p.product_id = e.product_id and p.product_category = e.product_category
WHERE e.customer_id is NULL
group by p.product_category;  
/*
ELAPSED TIME: 82.32 s
----------------------------------------------------------------------------------------------
OK
p.product_category      cnt
Automotive      3515501
Music   4538542
Digital_Video_Games     145413
Wireless        8989144
Toys    4863220
Digital_Music_Purchase  1636204
Video_Games     1765326
Sports  4850620
Time taken: 82.934 seconds, Fetched: 8 row(s)
*/
--Load Data in hive
CREATE EXTERNAL TABLE IF NOT EXISTS raw_tweet_2  
(t_location string,t_following BIGINT,followers BIGINT,totaltweets BIGINT,usercreatedts timestamp,tweetid BIGINT,tweetcreatedts timestamp,retweetcount BIGINT,t_text string,hashtags ARRAY<STRUCT<text : STRING, indices : ARRAY<INT>>>,language string,coordinates STRUCT<l_type:STRING,lat:ARRAY<BIGINT>>,favorite_count BIGINT,extractedts timestamp) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/user/svijai/tmp/tweetraw/' 
TBLPROPERTIES ('skip.header.line.count'='1'); 

--Load Dictionary with lable(Postive, Negative , Netural)
CREATE EXTERNAL TABLE if not exists dictionary (  
 type string,  
 length int,  
 word string,  
 pos string,  
 stemmed string,  
 polarity string )  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS TEXTFILE  
LOCATION 'user/vmehala/directory/'; 

-- Creating views of array for all the sentences in raw_tweet_2.Text
create view s1 as  
 select tweetid,raw_tweet_2.text , words 
 from raw_tweet_2  
 lateral view explode(sentences(lower(text))) dummy as words;  

 
 
-- Creating views of array for all the Words in s1.words
create view IF NOT EXISTS s2 as  
select tweetid,word 
from s1  
lateral view explode(words) dummy as word;  

 
-- Create view s3 from s2 to compute sentiment  

create view IF NOT EXISTS s3 as select  
 s2.word, tweetid, 
 case d.polarity  
 when 'negative' then -1  
 when 'positive' then 1  
 else 0 end as polarity  
 from s2 left outer join dictionary d on s2.word = d.word; 

-- Computing the sentiment of the text
create table IF NOT EXISTS tweets_sentiment  
stored as orc as select tweetid, 
 case  
 when sum( polarity ) > 0 then 'positive'  
 when sum( polarity ) < 0 then 'negative'  
 else 'neutral' end as sentiment  
from s3 group by tweetid; 


-- joining the sentiment of the text with the the raw data
select a.tweetid,a.text,b. sentiment as polarity,a.t_location,a.t_following,a.followers,a.usercreatedts,a.tweetcreatedts,a.retweetcount,a.favorite_count from raw_tweet_2 a join tweets_sentiment b on a.tweetid = b.tweetid;

-- generating NGRAMS all the type of the polarity
SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(text)), 3, 50)) AS bigrams FROM raw_tweets_sentiment where polarity="positive";
SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(text)), 3, 50)) AS bigrams FROM raw_tweets_sentiment where polarity="negative";
SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(text)), 3, 50)) AS bigrams FROM raw_tweets_sentiment where polarity="neutral";
load_tweets = LOAD '/users/vyom/documents/tweets/' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS myMap;*/

extract_details = FOREACH load_tweets GENERATE myMap#'user' as User ,myMap#'id' AS id, myMap#'text' AS text;
tokens = FOREACH extract_details GENERATE id,text,FLATTEN(TOKENIZE(text)) AS word;
dictionary = LOAD '/users/vyom/documents/tweets/AFINN.txt' using PigStorage('\t') AS (word:chararray,rating:int);
word_rating = JOIN tokens BY word left outer, dictionary BY word using 'replicated';
rating = FOREACH word_rating GENERATE tokens::id as id, tokens::text as text, dictionary::rating as rate;
word_group = GROUP rating BY (id,text);
avg_rate = FOREACH word_group GENERATE group, AVG(rating.rate) as tweet_rating;


dump avg_rate;
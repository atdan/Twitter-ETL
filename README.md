# Twitter-ETL
A simple ETL pipeline that gets data from twitter stream api based on certain keywords, caches it using redis and sends the data to kafka

The application connects to tweeter stream API using tweepy,
it then recieves a streem of tweets transformed it to only get some specific JSON objects
and caches them using redis.

The json objects is then pushed from redis to kafka, using kafka Producer.
A kafka consumer gets back the values sent by the producer and writes each one to a json file


## Dependencies
Python 3.0+

Redis 3.0+

kafka 1.1.0

kafka-python 1.4+

schedule 0.6+

tweepy 3.8+


## How to run
``` python3 getTweets.py ```

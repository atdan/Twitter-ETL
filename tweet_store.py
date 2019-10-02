import json
import schedule
from kafka import KafkaProducer, KafkaConsumer
import time, calendar
import redis


class TweetStore:
    # Redis Configuration
    redis_host = "localhost"
    redis_port = 6379
    redis_password = ""

    # Tweet Configuration
    redis_key = 'tweets'
    num_tweets = 20
    trim_threshold = 100
    count = 5

    # Kafka config
    KAFKA_HOST_URL = '52.19.199.252:9092,52.19.199.252:9093,52.19.199.252:9094'
    topic_name = "atuma_twitter_stream_topic"

    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST_URL,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    consumer = KafkaConsumer(topic_name, group_id='tweets',
                             bootstrap_servers=KAFKA_HOST_URL,
                             value_deserializer=lambda item: json.loads(item.decode('utf-8')))
    consumer.subscribe(topic_name)

    def __init__(self):
        self.db = r = redis.Redis(
            host=self.redis_host,
            port=self.redis_port
        )
        self.trim_count = 0
        schedule.every(2).minutes.do(self.kafka_consumer)

    # def cronjob(self):
    #     myCron = CronTab(user='root')

    def redis_to_kafka(self, data, limit=15):
        # KAFKA_HOST_URL = '52.19.199.252:9092,52.19.199.252:9093,52.19.199.252:9094'
        tweets = []
        print("kafka sending.....")

        ack = self.producer.send(self.topic_name, data)
        print("Ack: ", ack)
        print("\n")

        self.consumer.subscribe(self.topic_name)
        self.kafka_consumer()
        print(self.count)
        self.count = self.count - 1

        for item in self.db.lrange(self.redis_key, 0, 10):
            tweet_obj = json.loads(item)
            tweets.append(tweet_obj)

            # self.kafka_consumer()

    def kafka_consumer(self):
        ts = calendar.timegm(time.gmtime())

        f = open(self.topic_name + '_' + str(ts) + '.json', 'a')
        for message in self.consumer:
            print("Kafka Consumer: ", message.value)
            f.write(str(message.value))

            print('\n')
            return

        # f.close()

    schedule.every(2).minutes.do(kafka_consumer)

    def push(self, data):
        self.db.lpush(self.redis_key, json.dumps(data))
        self.trim_count += 1

        for item in self.db.lrange(self.redis_key, 0, 10):
            tweet_obj = json.loads(item)
            self.redis_to_kafka(tweet_obj)
        # Periodically trim the list so it doesn't grow too large.
        # if self.trim_count > 100:
        #     self.db.ltrim(self.redis_key, 0, self.num_tweets)
        #     self.trim_count = 0

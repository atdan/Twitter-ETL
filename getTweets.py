from tweet_store import TweetStore

from config.settings import *
import tweepy

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
# print('CONSUMER SECRET', CONSUMER_SECRET)

api = tweepy.API(auth)
store = TweetStore()


class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        tweet_obj = {
            'id_str': status.id_str,
            'source': status.source,
            'text': status.text,
            'retweeted': status.retweeted,
            'retweet_count': status.retweet_count,
            'created_at': str(status.created_at),
            'username': status.user.screen_name,
            'profile_img_url': status.user.profile_image_url,
            'followers': status.user.followers_count,
            'user_id': status.user.id,
        }

        # print(status)
        # print(tweet_obj)
        store.push(tweet_obj)
        # print('Pushed to redis: ', tweet_obj)
        print("\n\n\n\n")


    def on_error(self, status_code):
        if status_code == 420:
            print('Error 420')
            # returning False in on_data disconnects the stream
            return False


# user = api.me()

# print('name: ', user.name)

# print("TRACK TERMS 1: ", TRACK_TERMS)
stream_listener = StreamListener()

stream = tweepy.Stream(auth=api.auth, listener=stream_listener)

stream.filter(track=TRACK_TERMS)

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os
import json
import psycopg2
import time

#Setup database for tweets
conn = psycopg2.connect("dbname=twitter user=meutband")
c = conn.cursor()
c.execute("CREATE TABLE nfl(CurrentTime INTEGER, Tweet VARCHAR, Location VARCHAR)")

#Variables that contains the user credentials to access Twitter API
access_token = os.environ.get('TWITTER_ACCESS_KEY')
access_token_secret = os.environ.get('TWITTER_SECRET_ACCESS_KEY')
consumer_key = os.environ.get('TWITTER_CONSUMER_KEY')
consumer_secret = os.environ.get('TWITTER_SECRET_CONSUMER_KEY')


class StdOutListener(StreamListener):

    '''
    This is a basic listener that just prints received tweets to stdout.
    '''

    def on_data(self, data):

        #End stream at 11:00pm (GMT Locat Time)
        if int(time.time()) > 1510290001:
            return False

        all_data = json.loads(data)
        location = all_data['user']['location']
        tweet = all_data['text']
        c.execute("INSERT INTO nfl(CurrentTime, Tweet, Location) VALUES (%s,%s,%s)",
                    (int(time.time()), tweet, location))
        conn.commit()
        print("\nNext Tweet")

        return True

    def on_error(self, status):

        print(status)



if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StdOutListener())

    try:
        #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
        stream.filter(track=['#NFL','#ThursdayNightFootball','#SEAvsAZ','#TNF',
                                '#Seahawks','#Cardinals','#WeAre12','#BeRedSeeRed',
                                '#BirdGang','#Sea12Hawk'])

    except KeyboardInterrupt:
        pass

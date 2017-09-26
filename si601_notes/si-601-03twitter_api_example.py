import oauth2 as oauth

# Twitter API documentation: https://dev.twitter.com/docs/api/1.1

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)
token = oauth.Token(key=access_token, secret=access_secret)
client = oauth.Client(consumer, token)

# Get your own home timeline
header, output = client.request('https://api.twitter.com/1.1/statuses/home_timeline.json?count=1', method="GET")
#print output

# Get the tweets from umsi
header, output = client.request('https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name=umsi&count=200', method="GET")
print output

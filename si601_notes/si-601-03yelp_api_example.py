"""Example for signing a search request using the oauth2 library."""

import oauth2, urllib2, json

# Get your Yelp API keys from here:
# https://www.yelp.com/login?return_url=%2Fdevelopers%2Fgetting_started%2Fapi_access
# Fill in these values
consumer_key = ''
consumer_secret = ''
token = ''
token_secret = ''

consumer = oauth2.Consumer(consumer_key, consumer_secret)
url = 'http://api.yelp.com/v2/search?term=bars&location=sf'

print 'URL: %s' % (url,)

oauth_request = oauth2.Request('GET', url, {})
oauth_request.update({'oauth_nonce': oauth2.generate_nonce(),
                      'oauth_timestamp': oauth2.generate_timestamp(),
                      'oauth_token': token,
                      'oauth_consumer_key': consumer_key})

token = oauth2.Token(token, token_secret)

oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)

signed_url = oauth_request.to_url()

print 'Signed URL: %s' % (signed_url,)

conn = urllib2.urlopen(signed_url, None)
json_response = conn.read()

print json_response

yelp_data = json.loads(json_response)

print yelp_data

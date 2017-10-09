import requests
import json
import re
import emoji
import random

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U0001f300-\U0001f5fF"
        u"\U00002600-\U000026FF"
        u"\U00002700-\U000027BF"
        u"\U0001F600-\U0001F64F"
        u"\U0001F910-\U0001F91E"
        u"\U0001F920-\U0001F927"
        u"\U0001F310-\U0001F31F"
        u"\U0001F937"
        u"\U0001F9C0"
        u"\U0000231A"
        u"\U0000203C"
                           "]+", flags=re.UNICODE)

app_id = "1503542683068452"
app_secret = "64c53a604d723174f1f0826dc8bfa992" # DO NOT SHARE WITH ANYONE!
access_token = app_id + "|" + app_secret
page_id = 'wsj'

# function to remove emojis
def remove_emojis(str):
    return ''.join(c for c in str if c not in emoji.UNICODE_EMOJI and emoji_pattern)

def get_post_data():
    # Construct the URL string
    base = "https://graph.facebook.com/v2.10/"
    node = page_id + "/posts"
    fields = "?fields=created_time&until=2017-09-27&since=2017-09-20&limit=100"
    parameters = "&order=chronological&access_token=%s" % access_token
    url = base + node + fields + parameters
    # retrieve data
    res = requests.get(url)
    
    return res.json()['data']
    

# posts by date
post_ids = get_post_data()
comment_ids = []
comment_messages = []
num_processed = 0

fpid = open('comment_messages_post_ids.csv','a', encoding='utf-8')
fcid = open('comment_messages_comment_ids.csv','a', encoding='utf-8')
fa = open('comment_messages_all.csv','a', encoding='utf-8')
for p_id in post_ids:
    nextpage = True
    
    post_id = p_id['id']
    
    base = "https://graph.facebook.com/v2.10/"
    node = post_id + "/comments"
    fields = "?order=reverse_chronological&filter=stream&comment_count&limit=25"
    parameters = "&access_token=%s" % access_token
    url = base + node + fields + parameters
    
    comments = requests.get(url).json()
    #comments = request_until_succeed(url)
    if 'data' in comments.keys() and comments['data'] != []:
        comments_data = comments['data']
        # while next page 
        while nextpage:
            # get comments data
            for comment_data in comments_data:
                # remove empty messages
                if comment_data['message']:
                    comment_messages.append([post_id, comment_data['id'], remove_emojis(comment_data['message']).strip()])
                    fpid.write(post_id + '\n')
                    fcid.write(comment_data['id'] + '\n')
                    fa.write(remove_emojis(comment_data['message']).strip() + '\n')
                    num_processed += 1
                    print(num_processed)
            
            if 'paging' in comments.keys() and comments['paging'] != []:
                if 'next' in comments['paging'].keys():
                    url = comments['paging']['next']
                    comments = requests.get(url).json()
                    #comments = request_until_succeed(url)
                    comments_data = comments['data']
                else:
                    nextpage = False
            else: 
                nextpage = False

# record the total number of messages scraped
fa.close()
fpid.close()
fcid.close()

# clean messages
newline_pattern = re.compile('[\n\r]+$')
comment_list = []
for element in comment_messages:
    comment_list.append([element[0], element[1], newline_pattern.sub(r"", element[2]).strip()])

fn = open('total_comments.csv', 'w')
fn.write(str(num_processed))
fn.close()

# sample 100 messages
random.seed(12345)
samp = []
samp = random.sample(comment_messages, 100)

fs = open('comment_messages_samples.csv','a', encoding='utf-8')
fs.write('pagename,post_id,comment_id,comment\n')
for line in samp:
    fs.write('wsj' + ',' + line[0] + ',' + line[1] + ',' + '\"' + line[2] + '\"' + '\n')
fs.close()









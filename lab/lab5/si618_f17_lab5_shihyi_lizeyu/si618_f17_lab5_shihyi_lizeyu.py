import requests
import random
import re


# Step 1le
token = '1913334812319444|fa868ec55b05d72fd042f52f78c90389'
fanpage_id = '8304333127'
name = 'wsj'
#url = 'https://www.facebook.com/wsj'

#facebook_id = requests.get('https://graph.facebook.com/?id={}'.format(url)).json()['og_object']['id']
information_list = []
comments_id = []
res = requests.get('https://graph.facebook.com/v2.10/{}/posts?fields=created_time&until=2017-09-27&since=2017-09-20&limit=36&access_token={}'.format(fanpage_id, token))
for id in res.json()['data']:
    #information_list.append([id['id'], id['created_time']])
    d = id['id']
#d = '8304333127_10156693469928128'
#d = information_list[0][0]
#for i in range(len(information_list)):
    #d = information_list[i][0]
    #tcount = requests.get('https://graph.facebook.com/v2.10/{}/comments?limit=0&summary=True&access_token={}'.format(d, token))
    #ttcount = tcount.json()['summary'].get('total_count')
    cres = requests.get('https://graph.facebook.com/v2.10/{}/comments?limit=25&order=reverse_chronological&filter=stream&access_token={}'.format(d, token))
    while 'paging' in cres.json():
        for id in cres.json()['data']:
            comments_id.append([d, id['id'], id['message']])
        #cress = cres.json()['data']
        #for c in range(len(cress)):
        #    comment_id.append([cress[c]['id']])
        if 'next' in cres.json()['paging']:
            cres = requests.get(cres.json()['paging']['next'])
        else:
            break
#
# for i in range(len(comment_id)):
#     d = comment_id[i]
#     #c = requests.get('https://graph.facebook.com/v2.10/{}/comments?limit=0&summary=True&access_token={}'.format(d, token))
#     #cc = c.json()['summary'].get('total_count')
#     fc = requests.get('https://graph.facebook.com/v2.10/{}/comments?limit=300&filter=stream&access_token={}'.format(d, token))
#     for mes in fc.json()['data']:
#         comments_list.append([fc['from']['id'], fc['message']])
#


# Remove the emoji
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
comments_list = []
total = []
for i in range(len(comments_id)):
    text = comments_id[i][2]
    comments_list.append(emoji_pattern.sub(r'', text))
    total.append(['wsj', comments_id[i][0], comments_id[i][1], emoji_pattern.sub(r'', text)])

comments = []
for x in range(len(comments_list)):
    if comments_list[x].strip() != "":
        comments.append(comments_list[x])
final = []
for x in range(len(total)):
    if total[x][3].strip() != "":
        final.append(total[x])


# Save as text file
with open('si618_f17_lab5_total_number_of_comments_shihyi_lizeyu.txt', 'w') as file:
    file.write("%d" % len(comments))
# with open('si618_f17_lab5_total_number_of_comments_shihyi_lizeyu.txt','w' ,encoding='utf-16') as file:
#     for line in comments:
#         file.write("%s\n" % (line))

# download_dir = "si618_f17_lab5_total_comments_shihyi_lizeyu.csv"  # where you want the file to be downloaded to
# csv = open(download_dir, "w")
#     # "w" indicates that you're writing strings to the file
# columnTitleRow = "comment\n"
# csv.write(columnTitleRow)
# for line in comments:
#     comment = line
#     row = "\"" + comment + "\"" + "\n"
#     csv.write(row)


# Step 2
subcomments = []
subcomments = random.sample(total, 100)

# Step 3
# with open('si618_f17_lab5_random_sample_100_comments_shihyi_lizeyu.csv','w', encoding='utf-8') as file:
#     file.write('%s;%s;%s;%s\n' % ('pagename', 'post_id', 'comment_id', 'comment'))
#     for line in final:
#         file.write('%s,%s,%s,"%s"' % (line[0], line[1], line[2], line[3].replace('\n', "")))
#         file.write('\n')

download_dir = "si618_f17_lab5_random_sample_100_comments_shihyi_lizeyu.csv" #where you want the file to be downloaded to

csv = open(download_dir, "w")
#"w" indicates that you're writing strings to the file

columnTitleRow = "pagename,post_id,comment_id,comment\n"
csv.write(columnTitleRow)

for line in subcomments:
    pagename = line[0]
    post_id = line[1]
    comment_id = line[2]
    comment = line[3]
    row = pagename+ "," + post_id + "," + comment_id + "," + "\"" + comment + "\"" + "\n"
    csv.write(row)

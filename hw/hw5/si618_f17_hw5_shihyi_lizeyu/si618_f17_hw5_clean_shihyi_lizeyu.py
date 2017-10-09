import sqlite3
import pandas as pd
from collections import Counter

df = pd.read_csv('si618_f17_hw5_batch_result_shihyi_lizeyu.csv', delimiter=',')
df1 = df[['Input.pagename', 'Input.post_id','Input.comment_id', "Answer.question"]]
df1.columns = ['pagename', 'post_id', 'comment_id', 'answer']

sqlite_file = "./data_db.sqlite"
# open connection
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

c.execute('CREATE TABLE fb_table (pagename TEXT, post_id TEXT, comment_id TEXT, answer INTEGER)')

for index, row in df1.iterrows():
    pagename = row[0]
    post_id = row[1]
    comment_id = row[2]
    answer = row[3]
    
    if len(answer) > 1:
        ans_list = answer.split("|")
        for ans in ans_list:
            ans_int = int(ans)
            c.execute("""INSERT INTO fb_table 
            (pagename, post_id, comment_id, answer) 
            VALUES (?,?,?,?)""", (pagename, post_id, comment_id, ans_int))
    else:
        ans_int = int(answer)
        c.execute("""INSERT INTO fb_table 
        (pagename, post_id, comment_id, answer) 
        VALUES (?,?,?,?)""", (pagename, post_id, comment_id, ans_int))
        

c.execute("SELECT COUNT(*) FROM fb_table")
rows = c.fetchall()
for row in rows:
    print(row)
    
c.execute("SELECT * FROM fb_table")
rows = c.fetchall()
for row in rows:
    print(row)

c.execute("""SELECT pagename, post_id, comment_id, 
        COUNT(CASE WHEN answer = 1 THEN 1 END), 
        COUNT(CASE WHEN answer = 2 THEN 1 END), 
        COUNT(CASE WHEN answer = 3 THEN 1 END), 
        COUNT(CASE WHEN answer = 4 THEN 1 END), 
        COUNT(CASE WHEN answer = 5 THEN 1 END),
        COUNT(CASE WHEN answer = 6 THEN 1 END)
        FROM fb_table GROUP BY post_id, comment_id""")
rows = c.fetchall()

f = open('si618_f17_hw5_cleaned_data_shihyi_lizeyu.csv', 'w')
f.write('pagename,post_id,comment_id,answer_1,answer_2,answer_3,answer_4,answer_5,answer_6' + '\n')
for row in rows:
    f.write(row[0] + ',' + row[1] + ',' + row[2] + ',' + 
    str(row[3]) + ',' + str(row[4]) + ',' + str(row[5]) + ',' + str(row[6]) + ',' + str(row[7]) + ',' + str(row[8]) 
    + '\n')
f.close()

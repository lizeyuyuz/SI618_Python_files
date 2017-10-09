import sqlite3
import pandas as pd
from collections import Counter

df = pd.read_csv('si618_f17_hw5_batch_result_shihyi_lizeyu.csv', delimiter=',', encoding='latin-1')
df1 = df[['HITId', 'Input.pagename', 'Input.post_id', 'Input.comment_id', "Answer.question"]]
df1.columns = ['hit_id', 'pagename', 'post_id', 'comment_id', 'answer']

sqlite_file = "./data_db.sqlite"
# open connection
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

c.execute('CREATE TABLE fb_table (hit_id TEXT, pagename TEXT, post_id TEXT, comment_id TEXT, answer INTEGER)')

for index, row in df1.iterrows():
    hit_id = row[0]
    pagename = row[1]
    post_id = row[2]
    comment_id = row[3]
    answer = row[4]

    if len(answer) > 1:
        ans_list = answer.split("|")
        for ans in ans_list:
            ans_int = int(ans)
            c.execute("""INSERT INTO fb_table 
            (hit_id, pagename, post_id, comment_id, answer) 
            VALUES (?, ?,?,?,?)""", (hit_id, pagename, post_id, comment_id, ans_int))
    else:
        ans_int = int(answer)
        c.execute("""INSERT INTO fb_table 
        (hit_id, pagename, post_id, comment_id, answer) 
        VALUES (?,?,?,?,?)""", (hit_id, pagename, post_id, comment_id, ans_int))

c.execute("SELECT COUNT(*) FROM fb_table")

c.execute("""SELECT pagename, post_id, comment_id, 
        COUNT(CASE WHEN answer = 1 THEN 1 END), 
        COUNT(CASE WHEN answer = 2 THEN 1 END), 
        COUNT(CASE WHEN answer = 3 THEN 1 END), 
        COUNT(CASE WHEN answer = 4 THEN 1 END), 
        COUNT(CASE WHEN answer = 5 THEN 1 END),
        COUNT(CASE WHEN answer = 6 THEN 1 END)
        FROM fb_table GROUP BY post_id, comment_id""")
rows = c.fetchall()

dat = pd.DataFrame(rows)
dat.columns = ['pagename', 'post_id', 'comment_id', 'answer_1', 'answer_2', 'answer_3', 'answer_4', 'answer_5', 'answer_6']

# 1
# worker agreement all three
result = []
ans_cols = dat.ix[:,3:9]
nn = 0
for column in ans_cols:
    nn += sum(ans_cols[column] == 3)
# all agreement rate
result.append(nn/100)
# check
sum(dat['answer_1'] == 3) + sum(dat['answer_2'] == 3) + sum(dat['answer_3'] == 3) + sum(dat['answer_4'] == 3) + sum(dat['answer_5'] == 3) + sum(dat['answer_6'] == 3)

# 2
# worker agreement at least 2 attacks
ans_cols = dat.ix[:,3:5]
nn = 0
for column in ans_cols:
    nn += sum(ans_cols[column] >= 2)
# at least 2 agreement rate
result.append(nn/100)
# check
sum(ans_cols['answer_1'] >= 2) + sum(ans_cols['answer_2'] >= 2)

# 2
# worker agreement at least 1 attack
ans_cols = dat.ix[:,3:5]
nn = 0
for column in ans_cols:
    nn += sum(ans_cols[column] >= 1)
# at least 2 agreement rate
result.append(nn/100)

# check
sum(ans_cols['answer_1'] >= 1) + sum(ans_cols['answer_2'] >= 1)

with open('si618_f17_hw5_analysis_output_shihyi_lizeyu.txt', 'w') as file:
    for row in result:
        file.write("%.2f\n" % row)





from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.jsonFile("hdfs:///var/si618f17/NFLPlaybyPlay2015.json")
# df.printSchema()
df.registerTempTable("nfl")

########################
# 1
# total yards progressed by each posteam for each game
q1 = sqlContext.sql('''SELECT DISTINCT GameID, posteam, DefensiveTeam,
        SUM(YardsGained) AS totyards
        FROM nfl
        WHERE posteam IS NOT NULL
        GROUP BY posteam, DefensiveTeam, GameID
        ORDER BY GameID, posteam, DefensiveTeam
        ''')
q1.registerTempTable("tab")

# find delta yards for each posteam-defensiveteam pair for each game, duplicated pairs allowed
q2 = sqlContext.sql('''SELECT a.GameID, a.posteam AS posteam1, b.posteam as
        posteam2, SUM(a.totyards - b.totyards) AS delta
        FROM tab AS a 
        INNER JOIN tab AS b 
        ON a.GameID = b.GameID
        WHERE a.posteam <> b.posteam
        GROUP BY a.posteam, b.posteam, a.GameID
        ORDER BY a.posteam, b.posteam, a.GameID 
        ''')
q3.registerTempTable("tabb")

# find mean across all games
q3 = sqlContext.sql('''SELECT posteam1, MEAN(delta) as meandelta
        FROM tabb
        GROUP BY posteam1
        ORDER BY meandelta DESC
        ''')
# convert to the right format
dff = q3.map(lambda t: t['posteam1'] + "\t" + str(t['meandelta']))
# check print output
for team in dff.collect():
    print team
# save rdd as textfile
dff.saveAsTextFile('si618_f17_lab4_output_1')

########################
#2
q1 = sqlContext.sql('''SELECT posteam, PlayType
        FROM nfl
        WHERE (PlayType = "Run" OR PlayType = "Pass") AND posteam IS NOT NULL
        ORDER BY posteam
        ''')
q1.registerTempTable("tab")
# count runs
q2 = sqlContext.sql('''SELECT posteam, COUNT(PlayType) AS countrun
        FROM tab
        WHERE PlayType = "Run"
        GROUP BY posteam
        ''')
q2.registerTempTable("tab1")
# count passes
q3 = sqlContext.sql('''SELECT posteam, COUNT(PlayType) AS countpass
        FROM tab
        WHERE PlayType = "Pass"
        GROUP BY posteam
        ''')
q3.registerTempTable("tab2")
# find the ratio
q4 = sqlContext.sql('''SELECT a.posteam, SUM(a.countrun)/SUM(b.countpass) as ratio
        FROM tab1 AS a
        LEFT JOIN tab2 AS b 
        ON a.posteam = b.posteam
        GROUP BY a.posteam
        ORDER BY ratio 
        ''')
q4.collect()

# convert to the right format
dff = q4.map(lambda t: t['posteam'] + "\t" + str(t['ratio']))
# check print output
for team in dff.collect():
    print team
# save rdd as textfile
dff.saveAsTextFile('si618_f17_lab4_output_2')

########################
#3
q1 = sqlContext.sql('''SELECT DISTINCT PenalizedPlayer, PenalizedTeam, COUNT(*) as count
        FROM nfl
        WHERE PenalizedPlayer IS NOT NULL
        GROUP BY PenalizedTeam, PenalizedPlayer
        ORDER BY count DESC, PenalizedTeam
        LIMIT 10
        ''')
# convert to the right format
dff = q1.map(lambda t: t['PenalizedPlayer'] + "\t" + t['PenalizedTeam']+ "\t" + 
        str(t['count']))
# check print output
for team in dff.collect():
    print team
# save rdd as textfile
dff.saveAsTextFile('si618_f17_lab4_output_3')

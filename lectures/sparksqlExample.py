
#create SQLContext
from pyspark import SparkContext
sc = SparkContext(appName="lecture4")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#read the file
#data from https://www.kaggle.com/maxhorowitz/nflplaybyplay2015
nfl_df = sqlContext.read.json("hdfs:///var/si618f17/NFLPlaybyPlay2015.json")

#let's see the schema
nfl_df.printSchema()

#lets say we want to find the most accurate passers in the field
#how can we find this? first need to filter the play types to only passing attempts
#lets first just look
nfl_df.select("PlayType").show()

#lets look at distinct values
nfl_df.select("PlayType").distinct().show()

#we care about Pass.
#things are getting complicated enough that we want to switch to programmatic sql queries. lets first register our table
nfl_df.registerTempTable("nfl")

#let's first simply count the attempts
q1 = sqlContext.sql('select Passer, count(*) as attempts from nfl where PlayType = "Pass" group by Passer order by attempts')

#let's first simply see how accurate they are
q2 = sqlContext.sql('select Passer, mean(cast(PassOutcome = "Complete" as int)) as accuracy from nfl where PlayType = "Pass" group by Passer order by accuracy')

#to those that know football, now this will look weird, who are those 1.0 accuracy people? it is the problem of small samples
q3 = sqlContext.sql('select Passer, mean(cast(PassOutcome = "Complete" as int)) as accuracy, count(*) as attempts from nfl where PlayType = "Pass" group by Passer order by accuracy')
q3.registerTempTable('passerAccuracy')
q4 = sqlContext.sql('select Passer, accuracy, attempts from passerAccuracy where attempts>200 order by accuracy')
q4.collect()
#you can also write to file
q4.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('accurate passers')

#use .rdd to convert DF to rdd
q4rdd = q4.rdd
#use toDF to convert rdd to df
q4again = q4rdd.toDF(['Passer', 'accuracy', 'attempts']) #you actually do not need the schema here since the rdd object converted from DF already has the column names


#completing a pass is not the ultimate evaluation. how many of those result in a TD?
q5 = sqlContext.sql('select Passer, mean(Touchdown) as accuracy, count(*) as attempts from nfl where PlayType = "Pass" group by Passer order by accuracy')
q5.registerTempTable('passerAccuracyTD')
q6 = sqlContext.sql('select Passer, accuracy, attempts from passerAccuracyTD where attempts>200 order by accuracy')
q6.collect()






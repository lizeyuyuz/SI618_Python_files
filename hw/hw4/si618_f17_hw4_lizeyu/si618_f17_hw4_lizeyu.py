# spark-submit --master yarn-client --queue si618f17 si618_f17_hw4_lizeyu.py
from pyspark import SQLContext, SparkConf, SparkContext

conf = SparkConf().setAppName("YelpAPI")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

bdf = sqlContext.read.json("hdfs:///var/si618f17/yelp_academic_dataset_business.json")
rdf = sqlContext.read.json("hdfs:///var/si618f17/yelp_academic_dataset_review.json")

bdf.registerTempTable("bus")
rdf.registerTempTable("rev")

# left join business table and review table on business_id
q1 = sqlContext.sql('''SELECT r.user_id, r.business_id, b.city, r.stars 
        FROM rev AS r
        LEFT JOIN bus AS b 
        ON r.business_id = b.business_id
        GROUP BY r.user_id, r.business_id, b.city, r.stars
        ORDER BY r.user_id, r.business_id, b.city, r.stars
        ''')
q1.registerTempTable("tab")

# count number of distinct cities reviewed by each distinct user_id, return only
# a long list of number of cities 30,27,26,25,25,24,24,24, ...2,2,2,2,2,1,1,1,
# 1,1,1,1,1,....,1,1
q2 = sqlContext.sql('''SELECT COUNT(DISTINCT(city)) AS citycount
        FROM tab
        GROUP BY user_id
        ORDER BY citycount 
        ''')
q2.registerTempTable("tabb")

# collect dataframe results to a list 
res = q2.collect()
# create a list containing many 1s, 2s, 3s, ... 30s 
l = []
for i in range(len(res)):
    l.append(res[i].citycount)
# convert the list of numbers 1 - 30 to an RDD
rdd = sc.parallelize(l)
# specify range of buckets for histogram()
x = list(range(1,max(l)+2))
# use histogram() to find frequency of each bucket
ress = rdd.histogram(x)
# convert the tuple ([buckets], [histogram frequency]) to tuples(num_cities, yelp_users )
resss = zip(ress[0], ress[1])

# below code will result in non-report of values 28 and 29
#q3 = sqlContext.sql('''SELECT citycount, COUNT(DISTINCT(user_id)) AS usercount
#        FROM tabb
#        WHERE citycount BETWEEN 1 AND 30
#        GROUP BY citycount
#        ORDER BY citycount 
#        ''')

# save to csv
with open('si618_f17_hw4_output_allreview_lizeyu.csv','wb') as file:
    file.write("%s,%s\n"%("cities", "yelp users"))
    for line in resss:
        file.write("%d,%d"%(line[0], line[1]))
        file.write('\n')


################
# good reviews
g1 = sqlContext.sql('''SELECT user_id, COUNT(DISTINCT(city)) as citycount
        FROM tab
        WHERE stars > 3
        GROUP BY user_id
        ORDER BY citycount
        ''')
g1.registerTempTable("gtab")

g2 = sqlContext.sql('''SELECT citycount, COUNT(DISTINCT(user_id)) as usercount
        FROM gtab
        GROUP BY citycount
        ORDER BY citycount 
        ''')

gres = g2.collect()
with open('si618_f17_hw4_output_goodreview_lizeyu.csv','wb') as file:
    file.write("%s,%s\n"%("cities", "yelp users"))
    for line in gres:
        file.write("%d,%d"%(line[0], line[1]))
        file.write('\n')

################
# bad reviews
b1 = sqlContext.sql('''SELECT user_id, COUNT(DISTINCT(city)) as citycount
        FROM tab
        WHERE stars < 3
        GROUP BY user_id
        ORDER BY citycount
        ''')
b1.registerTempTable("btab")

b2 = sqlContext.sql('''SELECT citycount, COUNT(DISTINCT(user_id)) as usercount
        FROM btab
        GROUP BY citycount
        ORDER BY citycount 
        ''')

bres = b2.collect()
with open('si618_f17_hw4_output_badreview_lizeyu.csv','wb') as file:
    file.write("%s,%s\n"%("cities", "yelp users"))
    for line in bres:
        file.write("%d,%d"%(line[0], line[1]))
        file.write('\n')

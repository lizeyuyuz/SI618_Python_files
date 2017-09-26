# Calculate the average stars for each business category
# Written by Dr. Yuhang Wang for SI601
'''
To run on Fladoop cluster

    spark-submit --master yarn-client --queue si618f17 --num-executors 2 --executor-memory 1g --executor-cores 2 spark_avg_stars_per_category.py
'''
import json

input_file = sc.textFile("hdfs:///var/si618f17/yelp_academic_dataset_business.json")

def cat_star(data):
  cat_star_list = []
  stars = data.get('stars', None)
  categories = data.get('categories', None)
  if categories:
    for c in categories:
      if stars != None:
        cat_star_list.append((c, stars))
  return cat_star_list


cat_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_star) \
                      .mapValues(lambda x: (x, 1)) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                      .map(lambda x: (x[0], x[1][0]/x[1][1]))

cat_stars.collect()



import json
input_file = sc.textFile("hdfs:///var/si618f17/yelp_academic_dataset_business.json")
def city_yelp(data):
    c_list = []
    
    city = data.get('city', None)
    neighborhoods = data.get('neighborhoods', None)
    review_count = data.get('review_count', None)
    stars = data.get('stars', None)
    
    if neighborhoods:
        for n in neighborhoods:
            c_list.append(((city, n), [1, review_count, stars]))
    else:
        c_list.append(((city, 'Unknown'), [1, review_count, stars]))
        
    return c_list

cat_stars = input_file.map(lambda line: json.loads(line))
cat_stars = cat_stars.flatMap(city_yelp)

# define a function that counts the number of 4-star or higher reviews
def stars(x):
    if x[1][2] >= 4.0:
        return ((x[0][0],x[0][1]), [x[1][0], x[1][1], 1])
    else:
        return ((x[0][0],x[0][1]), [x[1][0], x[1][1], 0])

cat_stars = cat_stars.map(stars)
cat_stars = cat_stars.map(lambda x: (x[0][0] + '; ' + x[0][1],  [x[1][0], x[1][1], x[1][2]]))
cat_stars = cat_stars.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
cat_stars = cat_stars.map(lambda t : (t[0].split("; "), [t[1][0], t[1][1], t[1][2]]))
cat_stars = cat_stars.sortBy(lambda x: (x[0][0], -x[1][0], -x[1][1], -x[1][2], x[0][1]), ascending = True)
cat_stars = cat_stars.map(lambda t : t[0][0] + '\t' + t[0][1] + '\t' + str(t[1][0]) + '\t' + str(t[1][1]) + '\t' + str(t[1][2]))
cat_stars.saveAsTextFile('hw3_output2')



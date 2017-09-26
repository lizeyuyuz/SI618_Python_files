import sqlite3
from sys import argv

# open connection
sqlite_file = "data_db.sqlite"
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

# arguments
genre = argv[1]
k = argv[2]

# query
c.execute("SELECT movie_actor.actor, COUNT(DISTINCT(movie_genre.imdb_id)) FROM movie_actor LEFT JOIN movie_genre ON movie_genre.imdb_id = movie_actor.imdb_id WHERE movie_genre.genre = '%s' GROUP BY movie_actor.actor ORDER BY COUNT(DISTINCT(movie_genre.imdb_id)) DESC LIMIT %s" % (genre, str(k)))

# print results
print('Top ' + str(k) + ' actors who played in most ' + genre + ' movies:')
print("Actor, " + genre + " Movies Played in")
rows = c.fetchall()
for row in rows:
    print(", ".join((row[0], str(row[1]))))

conn.close() 

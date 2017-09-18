import json #json.loads()
import sqlite3 #sqlite connection
import sys #output to text file

# write to a text file
#orig_stdout = sys.stdout
#t = open('part1_output.txt','w')
#sys.stdout = t

# read text file
f = open("./movie_actors_data.txt").read().splitlines()

# source: https://www.safaribooksonline.com/library/view/python-cookbook-3rd/9781449357337/ch06s02.html
# json.dumps converts a Python data structure into JSON-encoded string
# json.load converts JSON-encoded string back into a Python data structure
r = []
for line in f:
    r.append(json.loads(line)) # decode json format
    
# sanity check
# type(r[0])
# dict

# http://www.prelc.si/koleznik/tutorial-for-parsing-json-and-creating-sqlite3-database-in-python/
# create a class object for each movie
class MovieInfo():
    def __init__(self, movie):
        self.movie_id = movie["imdb_id"]
        self.genres = movie["genres"] 
        self.title = movie["title"]
        self.year = movie["year"]
        self.rating = movie["rating"]
        self.actors = movie["actors"]

# ignore movies without any genre
data = []
for index, movie in enumerate(r): 
    # https://stackoverflow.com/questions/9524209/count-indexes-using-for-in-python
    if movie.get("genres") == None:
        print(1)
        del r[index]
    else:
        data.append(MovieInfo(movie))

# http://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html
# sqlite file name
sqlite_file = "./data_db.sqlite"
# open connection
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

# table 1
c.execute('CREATE TABLE movie_genre (imdb_id TEXT, genre TEXT)')
for movie in data:
    if len(movie.genres) > 1:
        # print(movie.movie_id)
        # print(movie.genres)
        for genre in movie.genres:
            #print(genre)
            c.execute("INSERT INTO movie_genre (imdb_id, genre) VALUES (?,?)", (movie.movie_id, genre))
    else:
        # print(movie.movie_id)
        # print(movie.genres)
        c.execute("INSERT INTO movie_genre (imdb_id, genre) VALUES (?,?)", (movie.movie_id, movie.genres[0]))

# check table output
#c.execute("SELECT * FROM movie_genre")
#rows = c.fetchall()
#for row in rows:
#    print(row)

# table2
c.execute('CREATE TABLE movies (imdb_id TEXT, title TEXT, year INTEGER, rating REAL)')
for movie in data:
    c.execute("INSERT INTO movies (imdb_id, title, year, rating) VALUES (?,?,?,?)", 
    (movie.movie_id, movie.title, movie.year, movie.rating))

# table 3
c.execute('CREATE TABLE movie_actor (imdb_id TEXT, actor TEXT)')
for movie in data:
    if len(movie.actors) > 1:
        for actor in movie.actors:
            c.execute("INSERT INTO movie_actor (imdb_id, actor) VALUES (?,?)", (movie.movie_id, actor))
    else:
        c.execute("INSERT INTO movie_actor (imdb_id, actor) VALUES (?,?)", (movie.movie_id, movie.actors[0]))


# query 5
c.execute("""
            SELECT genre AS genre, COUNT(DISTINCT(imdb_id)) 
            AS count FROM movie_genre 
            GROUP BY genre 
            ORDER BY count Desc 
            LIMIT 10
            """)
rows = c.fetchall()
print("Top 10 genres:")
print("Genre, Movies")
for row in rows:
    print(",".join((row[0], str(row[1]))))
print(("\n").rstrip())
#t.write('\n')

# query 6
c.execute("SELECT year, COUNT(DISTINCT(imdb_id)) FROM movies GROUP BY year ORDER BY year")
rows = c.fetchall()
print("Movies broken down by year:")
print("Year, Movies")
for row in rows:
    print(", ".join((str(row[0]), str(row[1]))))
print(("\n").rstrip())
#t.write('\n')

# query 7
c.execute("""
            SELECT DISTINCT(movies.title), movies.year, movies.rating 
            FROM movies 
            LEFT JOIN movie_genre 
            ON movies.imdb_id = movie_genre.imdb_id 
            WHERE movie_genre.genre = 'Sci-Fi' 
            ORDER BY rating DESC, year DESC
            """)
rows = c.fetchall()
print("Sci-Fi movies:")
print("Title, Year, Rating")
for row in rows:
    print(", ".join((row[0], str(row[1]), str(row[2]))))
print(("\n").rstrip())
#t.write('\n')

# query 8
c.execute("""
            SELECT movie_actor.actor, COUNT(DISTINCT(movies.imdb_id)) 
            FROM movie_actor 
            LEFT JOIN movies 
            ON movies.imdb_id = movie_actor.imdb_id 
            WHERE movies.year >= 2000 
            GROUP BY movie_actor.actor 
            ORDER BY COUNT(DISTINCT(movies.imdb_id)) DESC 
            LIMIT 10
            """)
rows = c.fetchall()
print("In and after year 2000, top 10 actors who played in most movies:")
print("Actor, Movies")
for row in rows:
    print(", ".join((str(row[0]), str(row[1]))))
print(("\n").rstrip())
#t.write('\n')

# query 9
c.execute("""
            SELECT DISTINCT a.actor, b.actor, COUNT(DISTINCT(b.imdb_id)) 
            FROM movie_actor a 
            INNER JOIN movie_actor b 
            ON a.imdb_id = b.imdb_id 
            WHERE a.actor < b.actor 
            GROUP BY a.actor, b.actor 
            HAVING COUNT(DISTINCT(b.imdb_id)) >= 3 
            ORDER BY COUNT(DISTINCT(b.imdb_id)) DESC
            """)
rows = c.fetchall()
print("Pairs of actors who co-stared in 3 or more movies:")
print("Actor A, Actor B, Co-stared Movies")
for row in rows:
    print(", ".join((row[0], row[1], str(row[2]))))
print(("\n").rstrip())
#t.write('\n')

# Commit changes to the database
conn.commit()
# Close connection to the database
conn.close()

# close text file writing
#sys.stdout = orig_stdout
#t.close()


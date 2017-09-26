import json
import sqlite3

# parse JSON
def parse_file()
    """ parse the JSON strings
    return: a list of decoded json strings
    """
    
    f = open("./movie_actors_data.txt").read().splitlines()

    dat = []
    for line in f:
        dat.append(json.loads(line)) # decode json format
    
    # sanity check
    # type(r[0])
    # dict
    
    return dat 

# class object 
class MovieInfo():
    def __init__(self, movie):
        self.movie_id = movie["imdb_id"]
        self.genres = movie["genres"] 
        self.title = movie["title"]
        self.year = movie["year"]
        self.rating = movie["rating"]
        self.actors = movie["actors"]
            

def clean_up(parsed_list)
    """ ignore movies without any genre
    param parsed_list: a list of dictionaries
    return: a list of movies with genres
    """
    
    dat = []
    for index, movie in enumerate(parsed_list): 
        if movie.get("genres") == None:
            print(1)
            del parsed_list[index]
        else:
            dat.append(MovieInfo(movie))
    
    return dat


def connection(db_file):
    """ create a database connection to the SQLite database
    parameter db_file: database file
    return: Connection object
    """
    conn = sqlite3.connect(db_file)
    
    return conn
    
def close(conn):
    """ Commit changes and close connection to the database """
    # conn.commit()
    conn.close()        

# table1
def movie_genre_table_query():
    """
    """
    
    c.execute('CREATE TABLE movie_genre (imdb_id TEXT, genre TEXT)')
    
    for movie in data:
    if len(movie.genres) > 1:
        # print(movie.movie_id)
        # print(movie.genres)
        for genre in movie.genres:
            print(genre)
            c.execute("INSERT INTO movie_genre (imdb_id, genre) VALUES (?,?)", (movie.movie_id, genre))
    else:
        # print(movie.movie_id)
        # print(movie.genres)
        c.execute("INSERT INTO movie_genre (imdb_id, genre) VALUES (?,?)", (movie.movie_id, movie.genres[0]))

    # check
    # cur.execute("SELECT * FROM movie_genre")
    # rows = cur.fetchall()
    # for row in rows:
    #    print(row)
    
    # query
    c.execute("SELECT genre AS genre, COUNT(DISTINCT(imdb_id)) AS count FROM movie_genre GROUP BY genre ORDER BY count Desc LIMIT 10")
    rows = c.fetchall()
    print("Top 10 genres:")
    print("Genre, Movies")
    for row in rows:
        print(", ".join((row[0], str(row[1]))))
    
    return 

def movie_table_query:
    return

# table3
def movie_actor_table_query():
    """
    """
    
    c.execute('CREATE TABLE movie_actor (imdb_id TEXT, actor TEXT)')

    for movie in data:
        if len(movie.actors) > 1:
            for actor in movie.actors:
                c.execute("INSERT INTO movie_actor (imdb_id, actor) VALUES (?,?)", (movie.movie_id, actor))
        else:
            c.execute("INSERT INTO movie_actor (imdb_id, actor) VALUES (?,?)", (movie.movie_id, movie.actors[0]))
    
    # query
    #c.execute("SELECT DISTINCT a.actor AS 'Aactors', b.actor AS 'Bactors', b.imdb_id AS 'count' FROM movie_actor a INNER JOIN movie_actor b ON a.imdb_id = b.imdb_id WHERE a.actor <> b.actor ORDER BY a.actor")
    c.execute("SELECT DISTINCT a.actor, b.actor, COUNT(DISTINCT(b.imdb_id)) FROM movie_actor a INNER JOIN movie_actor b ON a.imdb_id = b.imdb_id WHERE a.actor < b.actor GROUP BY a.actor, b.actor HAVING COUNT(DISTINCT(b.imdb_id)) >= 3 ORDER BY COUNT(DISTINCT(b.imdb_id)) DESC")
               
    
    rows = c.fetchall()
    print("Pairs of actors who co-stared in 3 or more movies:")
    print("Actor A, Actor B, Co-stared Movies")
    for row in rows:
        print(", ".join((row[0], str(row[1]), str(row[2]))))


    return 

    

def main():
    db_file = "data_db.sqlite"
    
    # create a database connection
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    with conn:
        
        # create movie_genre table
        c.execute('CREATE TABLE movie_genre (imdb_id TEXT, genre TEXT)')

        print("1. Query task by priority:")
        select_task_by_priority(conn,1)
 
        print("2. Query all tasks")
        select_all_tasks(conn)
    
    
    # Close connection to the database
    conn.close()        

if __name__ == '__main__':
    main()
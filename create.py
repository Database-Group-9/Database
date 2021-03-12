import psycopg2
import csv
import re

port = 5433

def createTable(tableName, primaryKey, columns):

    cur.execute("""
    CREATE TABLE %s(
        %s PRIMARY KEY,
        %s   
    )
    """ % (tableName, primaryKey, columns)
    )

def createRelationshipTable(tableName, foreignKey1, foreignKey2):

    cur.execute("""
    CREATE TABLE %s(
        %s,
        %s,
        PRIMARY KEY(%s, %s)  
    )
    """ % (tableName, foreignKey1, foreignKey2, foreignKey1.split(' ')[0], foreignKey2.split(' ')[0]))

conn = psycopg2.connect("host=localhost port=%s dbname=database_cw user=postgres password=myPassword" % (port))
cur = conn.cursor()
# cur.execute("""CREATE TABLE movies(
# movieId integer PRIMARY KEY,
# title text,
# year text
# )
# """)

def seperateNameAndYear(title):
    stripped_title = title.rstrip()
    match = re.match(r'.*([1-3][0-9]{3})', stripped_title[-5:-1])
    if match:
        return (stripped_title[:-7], stripped_title[-5:-1])
    else:
        return (stripped_title, None)


genre = set()
def add_genre_table():
    with open('movies.csv', 'r', encoding="utf8") as movies:
        movieReader = csv.reader(movies)
        next(movieReader) # Skip the header row.
        for movieId, title, genres in movieReader:
            genre_list = genres.split('|')
            for item in genre_list:
                if item == "(no genres listed)":
                    continue
                elif item not in genre:
                    genre.add(item)
    
    for i, item in enumerate(sorted(genre), 1):
        cur.execute(
            "INSERT INTO genres VALUES (%s, %s)",
            (i, item)
        )

        
def add_movie_genre_relationship_table(movieId, genres, sorted_genre):
    for category in genres.split('|'):
        if category in genre:
            cur.execute(
            "INSERT INTO movie_genre VALUES (%s, %s)",
            (movieId, sorted_genre.index(category) + 1))     

def add_movies_table():
    sorted_genre = sorted(genre)
    with open('movies.csv', 'r', encoding="utf8") as movies, open('links.csv', 'r') as links:
        movieReader = csv.reader(movies)
        linkReader = csv.reader(links)
        next(movieReader) # Skip the header row.
        next(linkReader) # Skip the header row.
        for row in zip(movieReader, linkReader):
            title, year = seperateNameAndYear(row[0][1])
            cur.execute("SELECT AVG(RATING) FROM ratings WHERE movieId = " + str(row[0][0]))
            avg = cur.fetchall()
            # avg = avg if avg else [0]
            # print(type(avg[0][0]))
            if type(avg[0][0]) is float:
                cur.execute(
                "INSERT INTO movies VALUES (%s, %s, %s, %s, %s, %s)",
                (row[0][0], title, year ,row[1][1], row[1][2], avg[0][0])
                )
            else:
                cur.execute(
                "INSERT INTO movies VALUES (%s, %s, %s, %s, %s, %s)",
                (row[0][0], title, year ,row[1][1], row[1][2], 0)
                )
            add_movie_genre_relationship_table(row[0][0], row[0][2], sorted_genre)


# def add_ratings_table():
#     with open('ratings.csv', 'r', encoding="utf8") as f:
#         reader = csv.reader(f)
#         next(reader) # Skip the header row.
#         for i, row in enumerate(reader, 1):
#             cur.execute(
#             "INSERT INTO ratings VALUES (%s, %s, %s, %s, %s)",
#             (i, row[0], row[1], row[2], row[3])
#         )

# def add_tags_table():
#     with open('tags.csv', 'r', encoding="utf8") as f:
#         reader = csv.reader(f)
#         next(reader) # Skip the header row.
#         for i, row in enumerate(reader, 1):
#             cur.execute(
#             "INSERT INTO ratings VALUES (%s, %s, %s, %s, %s)",
#             (i, row[0], row[1], row[2], row[3])
#         )

def add_tags_ratings_table(fileName):
    with open(fileName + '.csv', 'r', encoding="utf8") as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for i, row in enumerate(reader, 1):
            cur.execute(
            "INSERT INTO " + fileName + " VALUES (%s, %s, %s, %s, %s)"
            ,(i, row[0], row[1], row[2] , row[3])
        )

def create_and_add_users_table():
    cur.execute(
        """
        CREATE TABLE users(
            userId integer PRIMARY KEY
        )
        """
    )
    cur.execute(
        "INSERT INTO users SELECT DISTINCT userId FROM ratings ORDER BY userId"
    )
    cur.execute(
        "INSERT INTO users SELECT DISTINCT userId FROM tags ORDER BY userId ON CONFLICT DO NOTHING"
    )

def drop_all_tables():
    cur.execute(
        """
        DROP TABLE genres;
        DROP TABLE ratings;
        DROP TABLE movies;
        DROP TABLE movie_genre;
        DROP TABLE tags;
        DROP TABLE users;
        """
    )

def find_likes_and_dislikes():
    like = {}
    

def main():
    # DROP ALL TABLES
    print("Dropping tables...")
    drop_all_tables()

    # add genres table
    print("Creating genres table...")
    createTable('genres', 'genreId integer', 'genre text')
    add_genre_table()

    # add ratings table
    print("Creating ratings table...")
    createTable('ratings', 'ratingId integer', 'userId integer, movieId integer, rating float, timestamp integer')
    add_tags_ratings_table("ratings")

    # add movies and movie_genre table
    print("Creating movie_genre table...")
    createRelationshipTable('movie_genre', 'movieId integer', 'genreId integer')
    print("Creating movie table...")
    createTable('movies', 'movieId integer', 'title text, year integer, imdbId text, tmdbId text, avgRating text')
    add_movies_table()
    
    # add tags table
    print("Creating tags table...")
    createTable('tags', 'tagId integer', 'userId integer, movieId integer, tag text, timestamp integer')
    add_tags_ratings_table("tags")
    
    # add user table
    print("Creating users table...")
    create_and_add_users_table()


    # createTable('tags', 'tagId integer', 'userId integer, movieId integer, tag text, timestamp integer')
    # add_tags_table()

    

    """personality table userid, Openness,Agreeableness,Emotional_stability (bruhh), Conscientiousness,extraversion,
     assigned metric, assigned condition, MovieId [1..12],Predicted_rating [1..12], is_personalized, enjoy_watching 
    """

    conn.commit() #commit everything


    
    
main()
